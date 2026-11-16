"""Source-verified cross-engine performance benchmark on a local Colab scratch disk.

The Part 2 quality panel and every preceding stage are read-only. Only the
processing stage may be written, and its publication is immutable.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .acquisition import DEFAULT_END, DEFAULT_START, quarter_ends, sha256
from .processing_engines import (
    ENGINE_NAMES, WORKLOADS, PARQUET_COLS, ProcessingError,
    compare_results, normalise, plan_text, run_engine, spark_session,
)

SEED = 11230945
DEFAULT_REPETITIONS = 5
DEFAULT_WARMUPS = 1
SAMPLE_NAMES = ("recent_85", "full_170")
PLOTS = ("figures/median_runtime.png", "figures/scaling.png", "figures/median_process_rss.png")
EVIDENCE = (
    "input_snapshot.json", "environment.json", "trials.csv", "trial_results.jsonl", "performance_summary.csv",
    "equivalence.csv", "scaling_summary.csv", "plan_inspection.csv", "plan_discussion.md",
    *PLOTS,
)
LOG = logging.getLogger("fdic_scalable_processing")

class BenchmarkError(RuntimeError):
    """Source lineage, numerical parity, measurement or artifact integrity failure."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _atomic_json(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".partial")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(dict(data), f, sort_keys=True, indent=2, allow_nan=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BenchmarkError(f"Missing or invalid JSON: {path}") from exc
    if not isinstance(obj, dict):
        raise BenchmarkError(f"Expected JSON object: {path}")
    return obj


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        w = csv.DictWriter(handle, fieldnames=columns, extrasaction="raise")
        w.writeheader()
        w.writerows(rows)


def _csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise BenchmarkError(f"Missing CSV header: {path}")
            return list(reader)
    except OSError as exc:
        raise BenchmarkError(f"Unable to open CSV: {path}") from exc


def _safe_stage_partitions(m: dict[str, Any], quality_stage: Path) -> tuple[list[dict[str, Any]], int]:
    """Validate the exact Part 2 manifest schema and selected 170 Parquet paths."""
    from .panel import panel_schema, EXCLUDED_PREDICTOR_FIELDS
    quarters = quarter_ends(DEFAULT_START, DEFAULT_END)
    if m.get("expected_quarters") != len(quarters) or m.get("scope") != {
        "start": quarters[0], "end": quarters[-1],
    }:
        raise BenchmarkError("Part 2 reporting calendar differs from required scope")
    if m.get("panel_fields") != panel_schema().names or m.get("lag_is_verified_publication") is not False:
        raise BenchmarkError("Part 2 panel schema or availability policy changed")
    if set(m["panel_fields"]) & EXCLUDED_PREDICTOR_FIELDS:
        raise BenchmarkError("Retrospective event information appears in processing input")
    p = m.get("partitions", [])
    if not isinstance(p, list) or len(p) != len(quarters):
        raise BenchmarkError("Expected 170 quarter-partitioned input files")
    if [x.get("quarter") for x in p] != quarters:
        raise BenchmarkError("Part 2 quarter ordering differs from required calendar")
    records: list[dict[str, Any]] = []
    total = 0
    for x in p:
        q = x["quarter"]
        year = q[:4]
        number = (int(q[4:6]) - 1) // 3 + 1
        rel = Path("panel") / f"year={year}" / f"quarter={number}" / "part-00000.parquet"
        if x.get("file") != str(rel) or not isinstance(x.get("rows"), int) or x["rows"] < 0:
            raise BenchmarkError(f"Invalid panel entry for quarter {q}")
        if not isinstance(x.get("sha256"), str) or len(x["sha256"]) != 64:
            raise BenchmarkError(f"No SHA-256 for {q}")
        if not isinstance(x.get("size_bytes"), int) or x["size_bytes"] <= 0:
            raise BenchmarkError(f"Missing source file size for {q}")
        total += x["rows"]
        records.append({"quarter": q, "file": str(rel), "rows": x["rows"],
                        "sha256": x["sha256"], "size_bytes": x["size_bytes"]})
    if total != m.get("panel_rows") or total + m.get("quarantined_rows", -1) != m.get("raw_rows"):
        raise BenchmarkError("Source/panel/quarantine accounting differs from Part 2")
    if total <= 0:
        raise BenchmarkError("Empty panel")
    return records, total


def input_contract(drive_root: Path, *, check_data_hashes: bool = True) -> dict[str, Any]:
    """Resolve all upstream stages and validate Part 2 manifest and source identity."""
    from .project_flow import get_active, resolve_stage
    active = get_active(drive_root)
    acq = resolve_stage(drive_root, "acquisition")
    storage = resolve_stage(drive_root, "storage")
    quality = resolve_stage(drive_root, "quality_panel")
    stage = Path(quality["path"])
    mpath = stage / "part2_manifest.json"
    manifest = _read_json(mpath)
    if sha256(mpath) != quality["validation"].get("part2_manifest_sha256"):
        raise BenchmarkError("Part 2 manifest digest differs from published validation")
    if manifest.get("source_manifest_sha256") != acq["validation"].get("source_manifest_sha256"):
        raise BenchmarkError("Part 2 input does not match acquisition source")
    if manifest.get("storage_verification_sha256") != storage["verification_sha256"]:
        raise BenchmarkError("Part 2 did not consume the selected storage stage")
    if manifest.get("code_commit") != quality["validation"].get("code_commit"):
        raise BenchmarkError("Part 2 source revision differs from its manifest")
    records, total = _safe_stage_partitions(manifest, stage)
    if check_data_hashes:
        for item in records:
            source_file = stage / item["file"]
            if (not source_file.is_file() or source_file.stat().st_size != item["size_bytes"]
                    or sha256(source_file) != item["sha256"]):
                raise BenchmarkError(f"Part 2 panel partition changed: {item['quarter']}")
    return {
        "experiment_id": active["experiment_id"],
        "part1_commit": active["config"]["git_commit"],
        "part2_commit": manifest["code_commit"],
        "acquisition_verification_sha256": acq["verification_sha256"],
        "storage_verification_sha256": storage["verification_sha256"],
        "quality_verification_sha256": quality["verification_sha256"],
        "part2_manifest_sha256": sha256(mpath),
        "panel_rows": total,
        "panel_fields": len(manifest["panel_fields"]),
        "quality_dir": str(stage),
        "partitions": records,
        "samples": {
            "recent_85": {"quarters": 85, "rows": sum(x["rows"] for x in records[-85:]),
                          "first": records[-85]["quarter"], "last": records[-1]["quarter"]},
            "full_170": {"quarters": 170, "rows": total,
                         "first": records[0]["quarter"], "last": records[-1]["quarter"]},
        },
    }


def prepare_samples(snapshot: dict[str, Any], scratch: Path) -> dict[str, Any]:
    """Copy once from Drive to local disk; validate hashes; hard-link the 85-quarter subset."""
    source = Path(snapshot["quality_dir"])
    full = scratch / "full_170"
    recent = scratch / "recent_85"
    selected = {x["quarter"] for x in snapshot["partitions"][-85:]}
    for index, entry in enumerate(snapshot["partitions"], 1):
        rel = Path(entry["file"])
        src = source / rel
        dest = full / rel
        if not dest.exists() or sha256(dest) != entry["sha256"]:
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_name(dest.name + ".partial")
            try:
                shutil.copy2(src, tmp)
                if sha256(tmp) != entry["sha256"]:
                    raise BenchmarkError(f"Local transfer checksum differs: {entry['quarter']}")
                os.replace(tmp, dest)
            finally:
                tmp.unlink(missing_ok=True)
        if entry["quarter"] in selected:
            dest_recent = recent / rel
            dest_recent.parent.mkdir(parents=True, exist_ok=True)
            if not dest_recent.exists() or sha256(dest_recent) != entry["sha256"]:
                dest_recent.unlink(missing_ok=True)
                try:
                    os.link(dest, dest_recent)
                except OSError:
                    shutil.copy2(dest, dest_recent)
            if sha256(dest_recent) != entry["sha256"]:
                raise BenchmarkError("Recent sample is not identical to the verified source")
        if index % 25 == 0 or index == len(snapshot["partitions"]):
            LOG.info("Staged %s/%s verified Parquet partitions", index, len(snapshot["partitions"]))
    return {sample: str(scratch / sample / "panel") for sample in SAMPLE_NAMES}


def _tree_rss_mib() -> float:
    import psutil
    try:
        proc = psutil.Process(os.getpid())
        entries = [proc, *proc.children(recursive=True)]
    except psutil.Error:
        return 0.0
    total = 0
    for entry in entries:
        try:
            total += entry.memory_info().rss
        except psutil.Error:
            pass
    return total / (1024 ** 2)


def measure_once(engine: str, workload: str, panel: Path) -> dict[str, Any]:
    """Exclude engine startup, include data I/O, transformations and result materialization."""
    if engine not in ENGINE_NAMES or workload not in WORKLOADS:
        raise BenchmarkError("Unknown engine or workload")
    spark = spark_session() if engine == "pyspark" else None
    stop = threading.Event()
    samples = [_tree_rss_mib()]
    base = samples[0]
    def watch() -> None:
        while not stop.wait(0.05):
            samples.append(_tree_rss_mib())
    monitor = threading.Thread(target=watch, daemon=True)
    monitor.start()
    try:
        start = time.perf_counter()
        result = run_engine(engine, panel, workload, spark=spark)
        elapsed = time.perf_counter() - start
    finally:
        samples.append(_tree_rss_mib())
        stop.set()
        monitor.join(timeout=2)
        if spark is not None:
            spark.stop()
    return {
        "engine": engine, "workload": workload, "elapsed_seconds": elapsed,
        "peak_process_tree_rss_mib": max(samples), "start_process_tree_rss_mib": base,
        "output_rows": len(result), "result": result,
    }


def _child_run(script: Path, panel: Path, engine: str, workload: str,
               timeout_seconds: int = 900) -> dict[str, Any]:
    command = [sys.executable, str(script), "worker", "--panel", str(panel),
               "--engine", engine, "--workload", workload]
    try:
        process = subprocess.run(command, capture_output=True, text=True,
                                 timeout=timeout_seconds, check=False)
    except subprocess.TimeoutExpired as exc:
        raise BenchmarkError(f"Worker timed out: {engine}/{workload}") from exc
    if process.returncode != 0:
        raise BenchmarkError(f"Worker failed ({engine}/{workload}): {process.stderr[-2500:]}")
    lines = [l for l in process.stdout.splitlines() if l.strip()]
    if not lines:
        raise BenchmarkError(f"Worker returned no output: {engine}/{workload}")
    try:
        row = json.loads(lines[-1])
    except ValueError as exc:
        raise BenchmarkError(f"Worker returned invalid JSON: {lines[-1][:200]}") from exc
    if row.get("engine") != engine or row.get("workload") != workload:
        raise BenchmarkError("Worker identity mismatch")
    return row


def _collect_plans(panels: Mapping[str, str], output: Path) -> list[dict[str, Any]]:
    """Capture true optimized Polars and Spark logical/physical plans without running a trial."""
    import polars as pl
    directory = output / "plans"
    directory.mkdir(parents=True, exist_ok=True)
    spark = spark_session()
    result = []
    try:
        for workload in WORKLOADS:
            for engine in ("polars", "pyspark"):
                content = plan_text(engine, Path(panels["full_170"]), workload, spark=spark)
                if not isinstance(content, str) or not content.strip():
                    raise BenchmarkError(f"Missing execution plan for {engine}/{workload}")
                name = f"{engine}_{workload}.txt"
                (directory / name).write_text(content + "\n", encoding="utf-8")
                low = content.lower()
                result.append({
                    "engine": engine, "workload": workload, "plan_file": "plans/" + name,
                    "plan_sha256": sha256(directory / name), "plan_characters": len(content),
                    "contains_scan": int("scan" in low),
                    "contains_filter": int("filter" in low),
                    "contains_aggregate": int("agg" in low or "aggregate" in low),
                    "contains_sort": int("sort" in low),
                    "contains_window": int("window" in low or "over" in low),
                    "contains_exchange_or_shuffle": int("exchange" in low or "shuffle" in low),
                })
    finally:
        spark.stop()
    _write_csv(output / "plan_inspection.csv", result,
               ("engine", "workload", "plan_file", "plan_sha256", "plan_characters", "contains_scan",
                "contains_filter", "contains_aggregate", "contains_sort", "contains_window",
                "contains_exchange_or_shuffle"))
    lines = [
        "# Processing engine plan observations", "",
        "Plans were obtained from Polars LazyFrame.explain(optimized=True) and Spark queryExecution().",
        "The inspection indicators below are string matches, not proof of a particular physical optimization.",
        "Projection and predicate pushdown should be assessed in each saved plan itself.",
        "Both engines operated within one Colab VM; Spark was configured as local[2].", "",
        "| Workload | Engine | Contains scan | Contains aggregation | Contains window | Contains exchange/shuffle |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for r in result:
        lines.append(f"| {r['workload']} | {r['engine']} | {r['contains_scan']} | {r['contains_aggregate']} | {r['contains_window']} | {r['contains_exchange_or_shuffle']} |")
    (output / "plan_discussion.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return result


def _summaries(trials: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Recompute medians and relative performance exclusively from measured trials."""
    measured = [r for r in trials if not r["warmup"]]
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in measured:
        groups[(r["sample"], r["workload"], r["engine"])].append(r)
    summary = []
    for sample in SAMPLE_NAMES:
        for workload in WORKLOADS:
            pd_trials = groups[(sample, workload, "pandas")]
            if not pd_trials:
                raise BenchmarkError(f"Missing pandas baseline for {sample}/{workload}")
            pd_median = statistics.median(float(x["elapsed_seconds"]) for x in pd_trials)
            for engine in ENGINE_NAMES:
                subset = groups[(sample, workload, engine)]
                if not subset:
                    raise BenchmarkError(f"Missing benchmark trials for {sample}/{workload}/{engine}")
                runtime = statistics.median(float(x["elapsed_seconds"]) for x in subset)
                rss = statistics.median(float(x["peak_process_tree_rss_mib"]) for x in subset)
                if runtime <= 0:
                    raise BenchmarkError("Measured runtime must be positive")
                summary.append({
                    "sample": sample, "workload": workload, "engine": engine,
                    "runs": len(subset), "median_seconds": runtime,
                    "median_peak_process_tree_rss_mib": rss,
                    "pandas_over_engine_speedup": pd_median / runtime,
                    "output_rows": subset[0]["output_rows"],
                })
    sm = {(r["sample"], r["workload"], r["engine"]): r for r in summary}
    scaling = []
    for engine in ENGINE_NAMES:
        for workload in WORKLOADS:
            a = sm[("recent_85", workload, engine)]["median_seconds"]
            b = sm[("full_170", workload, engine)]["median_seconds"]
            scaling.append({
                "engine": engine, "workload": workload,
                "recent_85_median_seconds": a, "full_170_median_seconds": b,
                "full_over_recent_runtime_ratio": b / a,
            })
    return summary, scaling


def _draw(output: Path, summary: list[dict[str, Any]], scaling: list[dict[str, Any]]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    figdir = output / "figures"
    figdir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    categories = [(s, w) for s in SAMPLE_NAMES for w in WORKLOADS]
    x = list(range(len(categories)))
    for offset, engine in zip((-0.25, 0, 0.25), ENGINE_NAMES):
        values = [next(r["median_seconds"] for r in summary if
                       (r["sample"], r["workload"], r["engine"]) == (s, w, engine))
                  for s, w in categories]
        ax.bar([i + offset for i in x], values, width=0.22, label=engine)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}\n{w}" for s, w in categories], fontsize=8, rotation=25, ha="right")
    ax.set_ylabel("Median elapsed time (s)")
    ax.set_title("Equivalent workloads on local Colab disk")
    ax.legend()
    fig.tight_layout()
    fig.savefig(figdir / "median_runtime.png", dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 4.5))
    cats = list(WORKLOADS)
    for i, engine in enumerate(ENGINE_NAMES):
        vals = [next(r["full_over_recent_runtime_ratio"] for r in scaling
                     if r["workload"] == w and r["engine"] == engine) for w in cats]
        ax.bar([j + (i-1) * .24 for j in range(len(cats))], vals, width=.22, label=engine)
    ax.set_xticks(range(len(cats)))
    ax.set_xticklabels(cats, rotation=12)
    ax.set_ylabel("Full / recent elapsed-time ratio")
    ax.set_title("Empirical scaling across distinct reporting-period samples")
    ax.legend()
    fig.tight_layout()
    fig.savefig(figdir / "scaling.png", dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5))
    for offset, engine in zip((-.25, 0, .25), ENGINE_NAMES):
        values = [next(r["median_peak_process_tree_rss_mib"] for r in summary if
                       (r["sample"], r["workload"], r["engine"]) == (s, w, engine))
                  for s, w in categories]
        ax.bar([i + offset for i in x], values, width=.22, label=engine)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}\n{w}" for s, w in categories], fontsize=8, rotation=25, ha="right")
    ax.set_ylabel("Sampled process-tree peak RSS (MiB)")
    ax.set_title("Python and child processes, including Spark JVM")
    ax.legend()
    fig.tight_layout()
    fig.savefig(figdir / "median_process_rss.png", dpi=160)
    plt.close(fig)


def _environment(repetitions: int, warmups: int) -> dict[str, Any]:
    import importlib.metadata
    packages = ("pandas", "polars", "pyspark", "numpy", "pyarrow", "psutil", "matplotlib", "pytest")
    return {
        "created_utc": _now(), "python_version": sys.version, "platform": platform.platform(),
        "cpu_count_logical": os.cpu_count(),
        "versions": {name: importlib.metadata.version(name) for name in packages},
        "java_version": subprocess.run(["java", "-version"], capture_output=True, text=True).stderr.splitlines()[:2],
        "engine_settings": {"pandas": "eager parquet reader", "polars": "LazyFrame optimized explain; default collect",
                            "pyspark": "local[2], shuffle partitions=8, adaptive=true"},
        "timing": "wall-clock execution of file read, transformation and materialized result; engine startup excluded",
        "memory": "50 ms sampled Python worker and recursive child process RSS; Spark JVM included; approximate",
        "input_location": "Colab local /content scratch, copied and SHA-256 checked from Google Drive",
        "seed": SEED, "repetitions": repetitions, "warmups": warmups,
        "sampling": "full 170 quarters and the most recent 85 of those quarters; no row replication",
        "trial_order": "seeded shuffled engine/workload/sample order within each repetition",
        "cache_note": "OS file cache can persist between process trials; fresh worker excludes engine-object reuse",
    }


def build_evidence(output: Path, scratch: Path, script: Path,
                   snapshot: dict[str, Any], *, repetitions: int = DEFAULT_REPETITIONS,
                   warmups: int = DEFAULT_WARMUPS) -> dict[str, Any]:
    """Execute and audit all trial groups before writing the final stage evidence."""
    if repetitions < 3 or warmups < 1:
        raise BenchmarkError("At least three measured runs and one warmup are required")
    if output.exists():
        raise BenchmarkError("Refusing to overwrite an existing processing stage")
    panels = prepare_samples(snapshot, scratch)
    output.mkdir(parents=True, exist_ok=False)
    _atomic_json(output / "input_snapshot.json", snapshot)
    _atomic_json(output / "environment.json", _environment(repetitions, warmups))
    # Capture plans after trials so a driver-side Spark JVM does not compete with measured workers.
    tasks = [(sample, workload, engine) for sample in SAMPLE_NAMES
             for workload in WORKLOADS for engine in ENGINE_NAMES]
    rng = random.Random(SEED)
    trials = []
    trial_results: list[dict[str, Any]] = []
    comparisons = []
    reference_results: dict[str, list[dict[str, Any]]] = {}
    for iteration in range(-warmups, repetitions):
        round_tasks = tasks.copy()
        rng.shuffle(round_tasks)
        by_task: dict[tuple[str, str, str], dict[str, Any]] = {}
        for j, (sample, workload, engine) in enumerate(round_tasks, 1):
            LOG.info("Trial %s/%s: %s, %s, %s (%s/%s)",
                     iteration + warmups + 1, repetitions + warmups, sample,
                     workload, engine, j, len(round_tasks))
            res = _child_run(script, Path(panels[sample]), engine, workload)
            if res["elapsed_seconds"] <= 0 or res["peak_process_tree_rss_mib"] <= 0:
                raise BenchmarkError("Invalid worker elapsed time or sampled memory")
            expected_limit = snapshot["samples"][sample]["quarters"]
            if workload in ("quarter_aggregation", "adjacent_asset_growth") and res["output_rows"] > expected_limit:
                raise BenchmarkError("Quarterly result unexpectedly exceeds sample quarter count")
            by_task[(sample, workload, engine)] = res
            trial_results.append({"iteration": iteration, "sample": sample,
                                  "workload": workload, "engine": engine, "result": res["result"]})
            trials.append({
                "iteration": iteration, "warmup": iteration < 0, "sample": sample,
                "workload": workload, "engine": engine,
                "elapsed_seconds": res["elapsed_seconds"],
                "peak_process_tree_rss_mib": res["peak_process_tree_rss_mib"],
                "start_process_tree_rss_mib": res["start_process_tree_rss_mib"],
                "output_rows": res["output_rows"],
            })
        for sample in SAMPLE_NAMES:
            for workload in WORKLOADS:
                baseline = by_task[(sample, workload, "pandas")]["result"]
                if iteration == 0:
                    reference_results[f"results/{sample}_{workload}.json"] = baseline
                for engine in ("polars", "pyspark"):
                    checked = compare_results(baseline, by_task[(sample, workload, engine)]["result"], workload)
                    comparisons.append({"iteration": iteration, "warmup": iteration < 0,
                                        "sample": sample, "workload": workload,
                                        "comparison": f"pandas_vs_{engine}", **checked})
        LOG.info("Numerical parity verified for round %s", iteration)
    summary, scaling = _summaries(trials)
    # Save the full, small grouped outputs so parity can be recalculated read-only.
    raw_results = output / "trial_results.jsonl"
    temp_results = raw_results.with_suffix(".partial")
    try:
        with temp_results.open("w", encoding="utf-8") as f:
            for record in trial_results:
                f.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_results, raw_results)
    finally:
        temp_results.unlink(missing_ok=True)
    _write_csv(output / "trials.csv", trials,
               ("iteration", "warmup", "sample", "workload", "engine", "elapsed_seconds",
                "peak_process_tree_rss_mib", "start_process_tree_rss_mib", "output_rows"))
    _write_csv(output / "equivalence.csv", comparisons,
               ("iteration", "warmup", "sample", "workload", "comparison",
                "rows_compared", "max_absolute_difference"))
    _write_csv(output / "performance_summary.csv", summary,
               ("sample", "workload", "engine", "runs", "median_seconds",
                "median_peak_process_tree_rss_mib", "pandas_over_engine_speedup", "output_rows"))
    _write_csv(output / "scaling_summary.csv", scaling,
               ("engine", "workload", "recent_85_median_seconds", "full_170_median_seconds",
                "full_over_recent_runtime_ratio"))
    _draw(output, summary, scaling)
    plan_rows = _collect_plans(panels, output)
    for filename, rows in reference_results.items():
        _atomic_json(output / filename, {"rows": rows})
    manifest = {
        "schema_version": 1, "created_utc": _now(), "code_commit": snapshot["part3_commit"],
        "experiment_id": snapshot["experiment_id"],
        "part2_manifest_sha256": snapshot["part2_manifest_sha256"],
        "quality_verification_sha256": snapshot["quality_verification_sha256"],
        "storage_verification_sha256": snapshot["storage_verification_sha256"],
        "source_rows": snapshot["panel_rows"], "partitions": len(snapshot["partitions"]),
        "sample_specs": snapshot["samples"],
        "workloads": list(WORKLOADS), "engines": list(ENGINE_NAMES),
        "warmups": warmups, "repetitions": repetitions,
        "measured_trials": repetitions * len(tasks),
        "warmup_trials": warmups * len(tasks),
        "comparisons": (repetitions + warmups) * len(SAMPLE_NAMES) * len(WORKLOADS) * 2,
        "plan_count": len(plan_rows),
        "evidence_sha256": {p: sha256(output / p) for p in EVIDENCE},
        "plan_sha256": {r["plan_file"]: r["plan_sha256"] for r in plan_rows},
        "reference_result_sha256": {name: sha256(output / name) for name in sorted(reference_results)},
    }
    _atomic_json(output / "part3_manifest.json", manifest)
    verdict = verify_evidence(output, snapshot)
    _atomic_json(output / "part3_validation.json", verdict)
    LOG.info("Completed %s measured trials and %s parity comparisons",
             manifest["measured_trials"], manifest["comparisons"])
    return verdict


def verify_evidence(output: Path, snapshot: dict[str, Any], *,
                    check_published: bool = False) -> dict[str, Any]:
    """Read-only independent recalculation of trial counts, medians, hashes and lineage."""
    m = _read_json(output / "part3_manifest.json")
    if (m.get("experiment_id") != snapshot["experiment_id"] or
        m.get("code_commit") != snapshot["part3_commit"] or
        m.get("source_rows") != snapshot["panel_rows"] or
        m.get("partitions") != 170 or
        m.get("part2_manifest_sha256") != snapshot["part2_manifest_sha256"] or
        m.get("quality_verification_sha256") != snapshot["quality_verification_sha256"] or
        m.get("storage_verification_sha256") != snapshot["storage_verification_sha256"] or
        m.get("sample_specs") != snapshot["samples"] or
        m.get("workloads") != list(WORKLOADS) or m.get("engines") != list(ENGINE_NAMES)):
        raise BenchmarkError("Processing stage does not match verified source or selected protocol")
    saved_input = _read_json(output / "input_snapshot.json")
    if saved_input != snapshot:
        raise BenchmarkError("Saved input snapshot differs from verified stage")
    if set(m.get("evidence_sha256", {})) != set(EVIDENCE):
        raise BenchmarkError("Incomplete evidence inventory")
    for file, hexdigest in m["evidence_sha256"].items():
        if not (output / file).is_file() or sha256(output / file) != hexdigest:
            raise BenchmarkError(f"Evidence file changed: {file}")
    expected_results = {f"results/{sample}_{workload}.json"
                        for sample in SAMPLE_NAMES for workload in WORKLOADS}
    if set(m.get("reference_result_sha256", {})) != expected_results:
        raise BenchmarkError("Incomplete reference output table inventory")
    for filename, code in m["reference_result_sha256"].items():
        if not (output / filename).is_file() or sha256(output / filename) != code:
            raise BenchmarkError(f"Reference output table changed: {filename}")
        if not isinstance(_read_json(output / filename).get("rows"), list):
            raise BenchmarkError(f"Malformed reference output table: {filename}")
    if len(m.get("plan_sha256", {})) != len(WORKLOADS) * 2:
        raise BenchmarkError("Missing optimized engine plans")
    for path, code in m["plan_sha256"].items():
        if not path.startswith("plans/") or sha256(output / path) != code:
            raise BenchmarkError(f"Execution plan missing or modified: {path}")
    trials = _csv(output / "trials.csv")
    parsed = []
    for r in trials:
        try:
            row = {**r,
                   "iteration": int(r["iteration"]),
                   "warmup": r["warmup"] == "True",
                   "elapsed_seconds": float(r["elapsed_seconds"]),
                   "peak_process_tree_rss_mib": float(r["peak_process_tree_rss_mib"]),
                   "start_process_tree_rss_mib": float(r["start_process_tree_rss_mib"]),
                   "output_rows": int(r["output_rows"])}
        except (ValueError, TypeError, KeyError) as exc:
            raise BenchmarkError("Malformed trial record") from exc
        if (r["warmup"] not in ("True", "False") or not math.isfinite(row["elapsed_seconds"]) or
            row["elapsed_seconds"] <= 0 or row["peak_process_tree_rss_mib"] <= 0):
            raise BenchmarkError("Invalid trial values")
        parsed.append(row)
    if len(parsed) != m.get("measured_trials", -1) + m.get("warmup_trials", -1):
        raise BenchmarkError("Wrong total trial count")
    tasks = {(sample, workload, engine) for sample in SAMPLE_NAMES for workload in WORKLOADS for engine in ENGINE_NAMES}
    for i in range(-m["warmups"], m["repetitions"]):
        subset = [r for r in parsed if r["iteration"] == i]
        if (len(subset) != len(tasks) or
            {(r["sample"], r["workload"], r["engine"]) for r in subset} != tasks or
            any(r["warmup"] != (i < 0) for r in subset)):
            raise BenchmarkError(f"Incomplete or repeated benchmark round {i}")
    recalc, scales = _summaries(parsed)
    def check_table(file: str, recomputed: list[dict[str, Any]], key_cols: tuple[str, ...],
                    numeric_cols: tuple[str, ...]) -> None:
        stored = _csv(output / file)
        if len(stored) != len(recomputed):
            raise BenchmarkError(f"Unexpected summary row count: {file}")
        indexed = {tuple(r[k] for k in key_cols): r for r in stored}
        if len(indexed) != len(recomputed):
            raise BenchmarkError(f"Duplicate summary key: {file}")
        for expected in recomputed:
            key = tuple(expected[k] for k in key_cols)
            actual = indexed.get(key)
            if actual is None:
                raise BenchmarkError(f"Missing summary row {key}")
            for col in numeric_cols:
                if not math.isclose(float(actual[col]), float(expected[col]), rel_tol=1e-12, abs_tol=1e-8):
                    raise BenchmarkError(f"Summary mismatch {file}/{key}/{col}")
    check_table("performance_summary.csv", recalc, ("sample", "workload", "engine"),
                ("runs", "median_seconds", "median_peak_process_tree_rss_mib",
                 "pandas_over_engine_speedup", "output_rows"))
    check_table("scaling_summary.csv", scales, ("engine", "workload"),
                ("recent_85_median_seconds", "full_170_median_seconds", "full_over_recent_runtime_ratio"))
    comparisons = _csv(output / "equivalence.csv")
    # Recalculate all numerical comparisons from the saved complete result tables.
    result_path = output / "trial_results.jsonl"
    try:
        with result_path.open(encoding="utf-8") as f:
            complete_results = [json.loads(line) for line in f if line.strip()]
    except (OSError, ValueError) as exc:
        raise BenchmarkError("Saved trial outputs are unreadable") from exc
    if len(complete_results) != len(parsed):
        raise BenchmarkError("Trial result count differs from timing records")
    by_output: dict[tuple[int, str, str, str], list[dict[str, Any]]] = {}
    for r in complete_results:
        key = (r["iteration"], r["sample"], r["workload"], r["engine"])
        if key in by_output or r["sample"] not in SAMPLE_NAMES or r["workload"] not in WORKLOADS or r["engine"] not in ENGINE_NAMES:
            raise BenchmarkError("Duplicate or unexpected saved trial output")
        by_output[key] = r["result"]
    comparison_map = {(int(r["iteration"]), r["sample"], r["workload"], r["comparison"]):r
                      for r in comparisons}
    if len(comparison_map) != len(comparisons):
        raise BenchmarkError("Duplicate saved cross-engine comparison")
    for row in parsed:
        output_key = (row["iteration"], row["sample"], row["workload"], row["engine"])
        if output_key not in by_output or len(by_output[output_key]) != row["output_rows"]:
            raise BenchmarkError("Saved result rows differ from measured trial")
    for i in range(-m["warmups"], m["repetitions"]):
        for sample in SAMPLE_NAMES:
            for workload in WORKLOADS:
                baseline = by_output[(i, sample, workload, "pandas")]
                if i == 0:
                    expected_ref = _read_json(output / f"results/{sample}_{workload}.json")["rows"]
                    compare_results(baseline, expected_ref, workload)
                for engine in ("polars", "pyspark"):
                    measured_parity = compare_results(baseline, by_output[(i,sample,workload,engine)], workload)
                    key = (i,sample,workload,f"pandas_vs_{engine}")
                    saved_parity = comparison_map.get(key)
                    if (saved_parity is None or int(saved_parity["rows_compared"]) != measured_parity["rows_compared"]
                        or not math.isclose(float(saved_parity["max_absolute_difference"]),
                                            measured_parity["max_absolute_difference"],
                                            rel_tol=1e-10, abs_tol=1e-7)):
                        raise BenchmarkError("Saved cross-engine parity differs from computed result tables")
    if len(comparisons) != m.get("comparisons", -1):
        raise BenchmarkError("Incomplete cross-engine parity evidence")
    comparison_keys = {(int(r["iteration"]), r["sample"], r["workload"], r["comparison"]) for r in comparisons}
    if len(comparison_keys) != len(comparisons):
        raise BenchmarkError("Repeated equivalence comparison")
    for row in comparisons:
        if (int(row["rows_compared"]) < 0 or
            float(row["max_absolute_difference"]) < 0 or
            not math.isfinite(float(row["max_absolute_difference"]))):
            raise BenchmarkError("Malformed cross-engine equivalence record")
    verdict = {
        "success": True, "experiment_id": snapshot["experiment_id"],
        "code_commit": snapshot["part3_commit"],
        "part3_manifest_sha256": sha256(output / "part3_manifest.json"),
        "part2_manifest_sha256": snapshot["part2_manifest_sha256"],
        "quality_verification_sha256": snapshot["quality_verification_sha256"],
        "storage_verification_sha256": snapshot["storage_verification_sha256"],
        "panel_rows": snapshot["panel_rows"], "source_quarters": 170,
        "measured_trials": m["measured_trials"], "warmup_trials": m["warmup_trials"],
        "numerical_comparisons": len(comparisons), "plan_files": len(m["plan_sha256"]),
        "evidence_artifacts_verified": len(m["evidence_sha256"]) + len(m["plan_sha256"]) + len(m["reference_result_sha256"]),
        "source_data_as_predictors_only": True, "cluster_tested": False,
    }
    if check_published and _read_json(output / "part3_validation.json") != verdict:
        raise BenchmarkError("Published validation differs from independent read-only verification")
    return verdict


def run_project_stage(drive_root: Path, code_commit: str, scratch: Path, script: Path,
                      *, repetitions: int = DEFAULT_REPETITIONS,
                      warmups: int = DEFAULT_WARMUPS,
                      replace_incomplete: bool = False) -> dict[str, Any]:
    from .project_flow import HandoffError, resolve_stage, stage_dir, publish_stage
    if len(code_commit) != 40 or any(ch not in "0123456789abcdef" for ch in code_commit):
        raise BenchmarkError("A complete 40-character Part 3 source revision is required")
    snapshot = input_contract(drive_root)
    snapshot["part3_commit"] = code_commit
    output = stage_dir(drive_root, "processing")
    try:
        published = resolve_stage(drive_root, "processing")
    except HandoffError as exc:
        if "has not passed verification" not in str(exc):
            raise
        published = None
    if published is not None:
        actual = _read_json(output / "part3_manifest.json")
        snapshot["part3_commit"] = actual["code_commit"]
        verdict = verify_evidence(output, snapshot, check_published=True)
        if published["verification_sha256"] != sha256(output / "part3_validation.json"):
            raise BenchmarkError("Published processing-stage validation was modified")
        return verdict
    if output.exists():
        if not replace_incomplete:
            raise BenchmarkError("Unpublished processing output exists; explicitly allow replacement")
        if not output.is_dir() or output.is_symlink():
            raise BenchmarkError("Refusing to remove unsafe output path")
        shutil.rmtree(output)
    result = build_evidence(output, scratch, script, snapshot,
                            repetitions=repetitions, warmups=warmups)
    published = publish_stage(drive_root, "processing", "part3_validation.json")
    if published["verification_sha256"] != sha256(output / "part3_validation.json"):
        raise BenchmarkError("Published processing validation digest mismatch")
    return result


def verify_project_stage(drive_root: Path) -> dict[str, Any]:
    from .project_flow import resolve_stage
    snapshot = input_contract(drive_root)
    stage = resolve_stage(drive_root, "processing")
    saved = _read_json(Path(stage["path"]) / "part3_manifest.json")
    snapshot["part3_commit"] = saved["code_commit"]
    verdict = verify_evidence(Path(stage["path"]), snapshot, check_published=True)
    if stage["verification_sha256"] != sha256(Path(stage["path"]) / "part3_validation.json"):
        raise BenchmarkError("Stage registry and validation SHA-256 differ")
    return verdict


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FDIC Part 3 reproducible processing benchmark")
    sub = parser.add_subparsers(dest="action", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--panel", type=Path, required=True)
    w.add_argument("--engine", choices=ENGINE_NAMES, required=True)
    w.add_argument("--workload", choices=WORKLOADS, required=True)
    build = sub.add_parser("build")
    build.add_argument("--drive-root", type=Path, required=True)
    build.add_argument("--code-commit", required=True)
    build.add_argument("--scratch", type=Path, required=True)
    build.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    build.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    build.add_argument("--replace-incomplete", action="store_true")
    verify = sub.add_parser("verify")
    verify.add_argument("--drive-root", type=Path, required=True)
    for item in (build, verify):
        item.add_argument("--log-file", type=Path)
    args = parser.parse_args(argv)
    if args.action == "worker":
        print(json.dumps(measure_once(args.engine, args.workload, args.panel), allow_nan=False))
        return 0
    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        args.log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)
    if args.action == "build":
        result = run_project_stage(args.drive_root, args.code_commit, args.scratch,
                                   Path(__file__).resolve().parents[2] / "run_scalable_processing.py",
                                   repetitions=args.repetitions, warmups=args.warmups,
                                   replace_incomplete=args.replace_incomplete)
    else:
        result = verify_project_stage(args.drive_root)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

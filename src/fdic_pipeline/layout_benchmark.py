"""Controlled flat-versus-quarter-partitioned Parquet scan experiment.

All scans use the same locally staged Financials snapshot. The measured read
includes dataset discovery and materialization, but excludes process startup.
Partition selection is counted from selected file fragments, not inferred from
wall-clock speedups.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import resource
import statistics
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

from .acquisition import quarter_ends, financial_path, sha256, DEFAULT_START, DEFAULT_END
from .storage_benchmark import read_json, write_json, BenchmarkError

LAYOUTS = ("flat", "partitioned")
WORKLOADS = ("full_projection", "one_quarter", "one_year")
COLUMNS = ("CERT", "REPDTE", "ASSET", "DEP", "ROA", "EQ")
FILTER_YEAR = 2010
FILTER_QUARTER = 4
SEED = 11230945


def workload_dates(workload: str) -> tuple[date | None, date | None]:
    if workload == "full_projection":
        return None, None
    if workload == "one_quarter":
        return date(FILTER_YEAR, 12, 31), date(FILTER_YEAR, 12, 31)
    if workload == "one_year":
        return date(FILTER_YEAR, 1, 1), date(FILTER_YEAR, 12, 31)
    raise BenchmarkError(f"Unknown layout workload: {workload}")


def _predicate(layout: str, workload: str, ds: Any, pa: Any) -> Any:
    start, end = workload_dates(workload)
    if start is None:
        return None
    if layout == "partitioned":
        base = ds.field("year") == FILTER_YEAR
        return base if workload == "one_year" else base & (ds.field("quarter") == FILTER_QUARTER)
    key = ds.field("REPDTE")
    return (key >= pa.scalar(start, type=pa.date32())) & (key <= pa.scalar(end, type=pa.date32()))


def _rss() -> float:
    with Path("/proc/self/status").open() as handle:
        for line in handle:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    return 0.0


def worker(work: Path, layout: str, workload: str) -> dict[str, Any]:
    if layout not in LAYOUTS or workload not in WORKLOADS:
        raise BenchmarkError("Unsupported layout benchmark operation")
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    path = (work / "format_pair" / "financials.snappy.parquet" if layout == "flat"
            else work / "source_snapshot" / "financials")
    before = _rss()
    t0 = time.perf_counter()
    dataset = ds.dataset(str(path), format="parquet",
                         partitioning="hive" if layout == "partitioned" else None)
    predicate = _predicate(layout, workload, ds, pa)
    total_fragments = sum(1 for _ in dataset.get_fragments())
    selected_fragments = sum(1 for _ in dataset.get_fragments(filter=predicate))
    planned = time.perf_counter() - t0
    table = dataset.to_table(columns=list(COLUMNS), filter=predicate, use_threads=True)
    elapsed = time.perf_counter() - t0
    count = table.num_rows
    cert_sum = int(pc.sum(table["CERT"]).as_py()) if count else 0
    asset_sum = float(pc.sum(table["ASSET"]).as_py() or 0.0)
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    return {
        "layout": layout, "workload": workload, "elapsed_s": elapsed,
        "planning_s": planned, "rows": count, "cert_sum": cert_sum,
        "asset_sum": asset_sum, "total_fragments": total_fragments,
        "selected_fragments": selected_fragments,
        "peak_rss_mib": peak_mib, "rss_before_mib": before,
    }


def check_comparable(a: dict[str, Any], b: dict[str, Any]) -> None:
    if a["workload"] != b["workload"]:
        raise BenchmarkError("Workload mismatch")
    if (a["rows"], a["cert_sum"]) != (b["rows"], b["cert_sum"]):
        raise BenchmarkError(f"Flat/partitioned row or CERT aggregate differs for {a['workload']}")
    if not math.isclose(float(a["asset_sum"]), float(b["asset_sum"]),
                        rel_tol=1e-12, abs_tol=1e-5):
        raise BenchmarkError(f"Flat/partitioned ASSET aggregate differs for {a['workload']}")
    if a["total_fragments"] != 1 or b["total_fragments"] != 170:
        raise BenchmarkError("Unexpected physical layout: expected 1 flat and 170 quarter fragments")
    expected = {"full_projection": 170, "one_quarter": 1, "one_year": 4}[a["workload"]]
    if b["selected_fragments"] != expected or a["selected_fragments"] != 1:
        raise BenchmarkError("Expected partition/file pruning was not observed")


def _invoke(script: Path, work: Path, layout: str, workload: str) -> dict[str, Any]:
    done = subprocess.run([sys.executable, str(script), "worker", "--work-dir", str(work),
                           "--layout", layout, "--workload", workload],
                          capture_output=True, text=True, check=False)
    if done.returncode:
        raise BenchmarkError(f"Worker failed: {layout}/{workload}: {done.stderr[-1800:]}")
    try:
        return json.loads(done.stdout)
    except ValueError as exc:
        raise BenchmarkError(f"Invalid layout worker response: {done.stdout[-500:]}") from exc


def _write_csv(path: Path, data: list[dict[str, Any]]) -> None:
    if not data:
        raise BenchmarkError("No measurements to export")
    with path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)


def _source_contract(source: Path, work: Path) -> dict[str, Any]:
    manifest = read_json(source / "manifest.json")
    report = read_json(source / "verification_report.json")
    prep = read_json(work / "preparation.json")
    quarters = quarter_ends(DEFAULT_START, DEFAULT_END)
    if (report.get("success") is not True or prep.get("round_trip_equality") is not True
        or prep.get("num_quarters") != 170 or prep.get("financial_rows") != report.get("financial_rows")
        or prep.get("acquisition_manifest_sha256") != sha256(source / "manifest.json")
        or prep.get("acquisition_report_sha256") != sha256(source / "verification_report.json")
        or set(manifest.get("financials", {})) != set(quarters)):
        raise BenchmarkError("Unverified or altered source snapshot")
    flat = work / "format_pair" / "financials.snappy.parquet"
    if not flat.is_file() or sha256(flat) != prep["parquet_sha256"]:
        raise BenchmarkError("Flat Parquet changed after preparation")
    for q in quarters:
        file = financial_path(work / "source_snapshot", q)
        if not file.is_file() or sha256(file) != manifest["financials"][q]["sha256"]:
            raise BenchmarkError(f"Partitioned Parquet changed: {q}")
    return {"source_manifest_sha256": sha256(source / "manifest.json"),
            "source_verification_sha256": sha256(source / "verification_report.json"),
            "preparation_sha256": sha256(work / "preparation.json"),
            "rows": report["financial_rows"], "partitions": 170}


def _plot(result_dir: Path, summary: list[dict[str, Any]]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(9.2, 4.6))
    x = range(len(WORKLOADS))
    width = 0.35
    for index, layout in enumerate(LAYOUTS):
        vals = [next(r["median_seconds"] for r in summary
                     if r["layout"] == layout and r["workload"] == workload)
                for workload in WORKLOADS]
        bars = ax.bar([i + (index - .5) * width for i in x], vals,
                      width, label="Flat Parquet" if layout == "flat" else "Quarter-partitioned Parquet")
        ax.bar_label(bars, fmt="%.3f", padding=3, fontsize=9)
    ax.set_xticks(list(x), ["All quarters", "2010 Q4", "2010 full year"])
    ax.set_ylabel("Median scan and discovery time (s)")
    ax.set_title("Physical layout benchmark: equivalent six-column projections")
    ax.set_ylim(bottom=0, top=max(row["median_seconds"] for row in summary) * 1.28)
    fig.legend(loc="upper center", ncol=2, bbox_to_anchor=(0.5, 1.01), frameon=False)
    fig.subplots_adjust(top=0.75, left=0.10, bottom=0.15, right=0.98)
    fig.savefig(result_dir / "layout_scan_times.png", dpi=170)
    plt.close(fig)


def benchmark(source: Path, work: Path, results: Path, script: Path,
              repetitions: int = 5, warmups: int = 1, seed: int = SEED) -> dict[str, Any]:
    if repetitions < 3 or warmups < 1:
        raise BenchmarkError("Require at least three measured repetitions and one warmup")
    if (results / "layout_validation.json").exists():
        raise BenchmarkError("Completed run is immutable; choose another experiment")
    contract = _source_contract(source, work)
    results.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    trials: list[dict[str, Any]] = []
    for trial in range(-warmups, repetitions):
        tasks = [(w, f) for w in WORKLOADS for f in LAYOUTS]
        rng.shuffle(tasks)
        observed: dict[tuple[str, str], dict[str, Any]] = {}
        for workload, layout in tasks:
            row = _invoke(script, work, layout, workload)
            row.update({"trial": trial, "warmup": trial < 0})
            trials.append(row)
            observed[(workload, layout)] = row
        for workload in WORKLOADS:
            check_comparable(observed[(workload, "flat")], observed[(workload, "partitioned")])
    summary: list[dict[str, Any]] = []
    for workload in WORKLOADS:
        for layout in LAYOUTS:
            runs = [r for r in trials if r["workload"] == workload and r["layout"] == layout and not r["warmup"]]
            summary.append({
                "workload": workload, "layout": layout, "runs": len(runs),
                "median_seconds": statistics.median(r["elapsed_s"] for r in runs),
                "median_planning_seconds": statistics.median(r["planning_s"] for r in runs),
                "median_peak_rss_mib": statistics.median(r["peak_rss_mib"] for r in runs),
                "rows_output": runs[0]["rows"], "total_fragments": runs[0]["total_fragments"],
                "selected_fragments": runs[0]["selected_fragments"],
            })
    _write_csv(results / "layout_trials.csv", trials)
    _write_csv(results / "layout_summary.csv", summary)
    _plot(results, summary)
    environment = {
        **contract, "repetitions": repetitions, "warmups": warmups, "seed": seed,
        "units": "wall-clock seconds; peak process RSS in MiB; local Colab disk",
        "workloads": list(WORKLOADS), "fields": list(COLUMNS),
        "process_startup_timed": False,
        "discovery_included_in_total_time": True,
        "pruning_evidence": "get_fragments(filter=...) selected physical fragments",
    }
    write_json(results / "layout_environment.json", environment)
    evidence = {name: sha256(results / name) for name in
                ("layout_trials.csv", "layout_summary.csv", "layout_scan_times.png", "layout_environment.json")}
    validation = {"success": True, "source": contract, "measured_trials": repetitions * len(LAYOUTS) * len(WORKLOADS),
                  "warmup_trials": warmups * len(LAYOUTS) * len(WORKLOADS),
                  "equivalence_passed": True, "partition_pruning_verified": True,
                  "artifacts": evidence}
    write_json(results / "layout_validation.json", validation)
    return validation


def verify(source: Path, work: Path, results: Path) -> dict[str, Any]:
    contract = _source_contract(source, work)
    validation = read_json(results / "layout_validation.json")
    if (validation.get("success") is not True or validation.get("source") != contract
        or validation.get("equivalence_passed") is not True
        or validation.get("partition_pruning_verified") is not True):
        raise BenchmarkError("Invalid layout benchmark source or validation")
    for name, expected in validation["artifacts"].items():
        if sha256(results / name) != expected:
            raise BenchmarkError(f"Altered layout evidence: {name}")
    with (results / "layout_summary.csv").open(newline="", encoding="utf-8") as handle:
        summary = list(csv.DictReader(handle))
    if (len(summary) != 6 or set((r["workload"], r["layout"]) for r in summary)
            != {(w, f) for w in WORKLOADS for f in LAYOUTS}):
        raise BenchmarkError("Layout summary is incomplete")
    for row in summary:
        if int(row["runs"]) < 3 or float(row["median_seconds"]) < 0:
            raise BenchmarkError("Invalid measured layout summary")
        expected_fragments = (1 if row["layout"] == "flat" else
                              {"full_projection": 170, "one_quarter": 1, "one_year": 4}[row["workload"]])
        if int(row["selected_fragments"]) != expected_fragments:
            raise BenchmarkError("Partition pruning evidence is inconsistent")
    return validation


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--work-dir", type=Path, required=True)
    w.add_argument("--layout", choices=LAYOUTS, required=True)
    w.add_argument("--workload", choices=WORKLOADS, required=True)
    r = sub.add_parser("run")
    r.add_argument("--source-dir", type=Path, required=True)
    r.add_argument("--work-dir", type=Path, required=True)
    r.add_argument("--results-dir", type=Path, required=True)
    r.add_argument("--repetitions", type=int, default=5)
    r.add_argument("--warmups", type=int, default=1)
    r.add_argument("--seed", type=int, default=SEED)
    v = sub.add_parser("verify")
    v.add_argument("--source-dir", type=Path, required=True)
    v.add_argument("--work-dir", type=Path, required=True)
    v.add_argument("--results-dir", type=Path, required=True)
    args = p.parse_args(argv)
    if args.cmd == "worker":
        print(json.dumps(worker(args.work_dir, args.layout, args.workload)))
        return 0
    if args.cmd == "run":
        value = benchmark(args.source_dir, args.work_dir, args.results_dir,
                          Path(__file__).resolve().parents[2] / "run_layout_benchmark.py",
                          args.repetitions, args.warmups, args.seed)
    else:
        value = verify(args.source_dir, args.work_dir, args.results_dir)
    print(json.dumps(value, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

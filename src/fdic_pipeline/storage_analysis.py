"""Part 1 / Storage: audit measured trials and derive report evidence.

The input is one explicit benchmark run selected by the preceding notebook cell.
Only the designated analysis directory is written; acquisition and trial data
remain unchanged.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# 1. Audit inputs and numerical validation
WORKLOADS = ("full_scan", "projection", "date_filter")
FORMATS = ("csv", "parquet")
FIGURES = ("storage_sizes.png", "read_time_medians.png", "peak_rss_medians.png")
INPUTS = ("preparation.json", "benchmark_verification.json", "environment.json",
          "trials.csv", "summary.csv", "comparisons.csv", "storage_sizes.csv", *FIGURES)
MIB = 1024 ** 2


class AnalysisError(ValueError):
    """Benchmark evidence is incomplete or internally inconsistent."""


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AnalysisError(message)


def load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            value = json.load(fh)
    except (OSError, ValueError) as exc:
        raise AnalysisError(f"Cannot read JSON: {path}") from exc
    expect(isinstance(value, dict), f"Expected JSON object: {path.name}")
    return value


def load_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            expect(reader.fieldnames is not None, f"Missing header: {path.name}")
            rows = list(reader)
    except OSError as exc:
        raise AnalysisError(f"Cannot read CSV: {path}") from exc
    expect(bool(rows), f"Empty CSV: {path.name}")
    expect(all(None not in r for r in rows), f"Malformed CSV record: {path.name}")
    return rows


def number(value: Any, label: str) -> float:
    try:
        n = float(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise AnalysisError(f"Invalid number for {label}: {value!r}") from exc
    expect(math.isfinite(n), f"Non-finite number for {label}")
    return n


def integer(value: Any, label: str) -> int:
    n = number(value, label)
    expect(n.is_integer(), f"Not an integer for {label}")
    return int(n)


def positive(value: Any, label: str) -> float:
    n = number(value, label)
    expect(n > 0, f"Expected positive value for {label}")
    return n


def close(a: Any, b: Any, label: str, rel_tol: float = 1e-9,
          abs_tol: float = 1e-8) -> None:
    x, y = number(a, label), number(b, label)
    expect(math.isclose(x, y, rel_tol=rel_tol, abs_tol=abs_tol),
           f"Inconsistent {label}: {x!r} versus {y!r}")


def flag(value: str, label: str) -> bool:
    normalized = str(value).strip().lower()
    expect(normalized in {"true", "false"}, f"Invalid boolean {label}: {value!r}")
    return normalized == "true"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name("." + path.name + ".partial")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False, sort_keys=True)
        fh.write("\n")
    tmp.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    expect(bool(rows), "Cannot write empty summary table")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


# 2. Cross-table trial verification

def validate_run(run_dir: Path) -> dict[str, Any]:
    """Recompute read summaries from raw trials and cross-check saved aggregates."""
    for name in INPUTS:
        expect((run_dir / name).is_file(), f"Missing benchmark evidence: {name}")
    prep = load_json(run_dir / "preparation.json")
    verified = load_json(run_dir / "benchmark_verification.json")
    env = load_json(run_dir / "environment.json")
    trials = load_csv(run_dir / "trials.csv")
    summary = load_csv(run_dir / "summary.csv")
    comparisons = load_csv(run_dir / "comparisons.csv")
    sizes = load_csv(run_dir / "storage_sizes.csv")

    expect(verified.get("success") is True, "Benchmark verification did not pass")
    expect(prep.get("round_trip_equality") is True
           and verified.get("round_trip_equality") is True,
           "CSV/Parquet round-trip equality has not been verified")
    actual_rows = integer(prep.get("financial_rows"), "financial_rows")
    expect(actual_rows > 0, "Financials row count must be positive")
    expect(integer(prep.get("num_quarters"), "num_quarters") == 170,
           "Unexpected quarter count")
    expect(integer(prep.get("field_count"), "field_count") == 33,
           "Unexpected Financials field count")
    expect(prep.get("source_scope") == {"start": "19840331", "end": "20260630"},
           "Unexpected data scope")
    expect(integer(verified.get("expected_rows"), "expected_rows") == actual_rows,
           "Benchmark verification row count differs from acquisition")
    if "production_partition_count" in prep:
        expect(integer(prep["production_partition_count"], "partition_count") == 170,
               "Unexpected source partition count")
    repetitions = integer(env.get("repetitions"), "repetitions")
    warmups = integer(env.get("warmups"), "warmups")
    expect(repetitions >= 3 and warmups >= 0, "Insufficient repetitions or invalid warmups")
    seed = integer(env.get("seed"), "random_seed")
    expect(integer(verified.get("trial_count"), "trial_count") ==
           len(WORKLOADS) * len(FORMATS) * (repetitions + warmups),
           "Unexpected declared trial count")
    expect(len(trials) == len(WORKLOADS) * len(FORMATS) * (repetitions + warmups),
           "Trial count does not match execution settings")

    by_key: dict[tuple[str, str], dict[int, dict[str, str]]] = defaultdict(dict)
    for row in trials:
        w, fmt = row.get("workload"), row.get("format")
        expect(w in WORKLOADS and fmt in FORMATS, "Unexpected workload or format in trials")
        t = integer(row.get("trial"), "trial")
        expect(-warmups <= t < repetitions, "Trial index outside run settings")
        expect(t not in by_key[(w, fmt)], f"Repeated trial index {w}/{fmt}/{t}")
        expect(flag(row.get("warmup", ""), "warmup") == (t < 0),
               f"Incorrect warmup flag for {w}/{fmt}/{t}")
        positive(row.get("elapsed_s"), "elapsed_s")
        positive(row.get("peak_rss_mib"), "peak_rss_mib")
        expect(number(row.get("baseline_rss_mib"), "baseline_rss_mib") >= 0,
               "Negative baseline RSS")
        expect(number(row.get("peak_over_baseline_mib"), "peak_over_baseline_mib") >= 0,
               "Negative incremental RSS")
        expect(integer(row.get("rows"), "rows") > 0, "Nonpositive output row count")
        integer(row.get("cert_sum"), "cert_sum")
        number(row.get("asset_sum"), "asset_sum")
        by_key[(w, fmt)][t] = row
    expected_indices = set(range(-warmups, repetitions))
    # A full scan and a pure projection retain every source observation.
    for workload in ("full_scan", "projection"):
        for fmt in FORMATS:
            for idx in expected_indices:
                expect(integer(by_key[(workload, fmt)][idx]["rows"],
                               "rows") == actual_rows,
                       f"Unexpected output row count in {workload}/{fmt}/{idx}")
    for workload in WORKLOADS:
        for fmt in FORMATS:
            expect(set(by_key[(workload, fmt)]) == expected_indices,
                   f"Missing trial for {workload}/{fmt}")
        for index in expected_indices:
            csv_row = by_key[(workload, "csv")][index]
            pq_row = by_key[(workload, "parquet")][index]
            expect(csv_row["rows"] == pq_row["rows"] and
                   csv_row["cert_sum"] == pq_row["cert_sum"],
                   f"Unequal workload outputs for {workload}/{index}")
            close(csv_row["asset_sum"], pq_row["asset_sum"],
                  f"asset sum {workload}/{index}", rel_tol=1e-12, abs_tol=1e-5)

    summary_map = {}
    for row in summary:
        key = (row.get("workload"), row.get("format"))
        expect(key in by_key and key not in summary_map,
               f"Invalid summary key: {key}")
        runs = [by_key[key][i] for i in range(repetitions)]
        dur = [float(r["elapsed_s"]) for r in runs]
        peaks = [float(r["peak_rss_mib"]) for r in runs]
        delta = [float(r["peak_over_baseline_mib"]) for r in runs]
        expect(integer(row.get("runs"), "runs") == repetitions,
               f"Run count mismatch: {key}")
        for col, actual in (("median_seconds", statistics.median(dur)),
                            ("min_seconds", min(dur)), ("max_seconds", max(dur)),
                            ("median_peak_rss_mib", statistics.median(peaks)),
                            ("median_peak_over_baseline_mib", statistics.median(delta))):
            close(row.get(col), actual, f"{key}/{col}")
        expect(integer(row.get("rows_output"), "rows_output") == int(runs[0]["rows"]),
               f"Output row count mismatch in summary: {key}")
        summary_map[key] = row
    expect(set(summary_map) == set(by_key), "Incomplete summary rows")

    comparison_map = {}
    for row in comparisons:
        w = row.get("workload")
        expect(w in WORKLOADS and w not in comparison_map,
               f"Invalid comparisons workload: {w}")
        csvrow = summary_map[(w, "csv")]
        pqrow = summary_map[(w, "parquet")]
        for col, actual in (("csv_median_seconds", csvrow["median_seconds"]),
                            ("parquet_median_seconds", pqrow["median_seconds"]),
                            ("csv_median_peak_rss_mib", csvrow["median_peak_rss_mib"]),
                            ("parquet_median_peak_rss_mib", pqrow["median_peak_rss_mib"])):
            close(row.get(col), actual, f"{w}/{col}")
        speedup = float(csvrow["median_seconds"]) / float(pqrow["median_seconds"])
        close(row.get("speedup_csv_over_parquet"), speedup, f"{w}/speedup")
        expect(integer(row.get("output_rows"), "output_rows") == int(csvrow["rows_output"]),
               f"Row count mismatch for {w}")
        comparison_map[w] = row
    expect(set(comparison_map) == set(WORKLOADS), "Incomplete comparisons")
    expect(len(verified.get("comparisons", [])) == len(comparisons),
           "Benchmark verification comparison rows differ")
    for r in verified["comparisons"]:
        w = r.get("workload")
        expect(w in comparison_map, f"Unexpected verification workload {w}")
        close(r.get("speedup_csv_over_parquet"),
              comparison_map[w]["speedup_csv_over_parquet"], f"verification speedup {w}")

    byte_fields = {"single-file": ("csv", "csv_bytes"),
                   "single-file-snappy": ("parquet", "flat_parquet_bytes"),
                   "original-partitioned-snappy": ("parquet", "production_partitioned_parquet_bytes")}
    expect(len(sizes) == len(byte_fields), "Storage size table does not have 3 representations")
    seen = set()
    for row in sizes:
        layout = row.get("layout")
        expect(layout in byte_fields and layout not in seen,
               f"Unknown or duplicate storage layout: {layout}")
        fmt, field = byte_fields[layout]
        expect(row.get("format") == fmt, f"Wrong format for {layout}")
        b = positive(prep.get(field), field)
        close(row.get("bytes"), b, f"{layout}/bytes")
        close(row.get("mib"), b / MIB, f"{layout}/mib")
        seen.add(layout)
    expect(seen == set(byte_fields), "Missing storage layout")
    close(verified.get("csv_over_parquet_size_ratio"),
          float(prep["csv_bytes"]) / float(prep["flat_parquet_bytes"]),
          "verification size ratio")
    expect(str(env.get("read_engine", "")).startswith("PyArrow"),
           "Unexpected read engine; review benchmark protocol")
    hashes = {name: sha256(run_dir / name) for name in INPUTS}
    return {"preparation": prep, "verification": verified, "environment": env,
            "trials": trials, "summary": summary, "comparisons": comparison_map,
            "hashes": hashes, "repetitions": repetitions, "warmups": warmups,
            "seed": seed}


def speed_phrase(workload: str, c: float, p: float) -> str:
    label = {"full_scan": "Full scan", "projection": "Six-column projection",
             "date_filter": "Date-filtered query"}[workload]
    ratio = c / p
    if ratio >= 1.0:
        return f"{label}: Parquet was {ratio:.2f} times as fast ({p:.3f} s versus {c:.3f} s for CSV)."
    return f"{label}: Parquet was {1 / ratio:.2f} times slower ({p:.3f} s versus {c:.3f} s for CSV)."


# 3. Audited evidence tables, without report prose

def build_evidence_tables(run_dir: Path, source: dict[str, Any], output: Path) -> dict[str, Any]:
    """Derive precise storage and performance tables from verified trial records."""
    prep = source["preparation"]
    comparison = source["comparisons"]
    c = float(prep["csv_bytes"])
    p = float(prep["flat_parquet_bytes"])
    part = float(prep["production_partitioned_parquet_bytes"])
    fmt_table = [
        {"representation": "Uncompressed CSV (flat)", "size_mib": round(c / MIB, 3),
         "relative_to_csv_pct": 100.0},
        {"representation": "Snappy Parquet (flat)", "size_mib": round(p / MIB, 3),
         "relative_to_csv_pct": round(100 * p / c, 3)},
        {"representation": "Snappy Parquet (170 quarter partitions)",
         "size_mib": round(part / MIB, 3),
         "relative_to_csv_pct": round(100 * part / c, 3)},
    ]
    perf_table = []
    for workload in WORKLOADS:
        row = comparison[workload]
        perf_table.append({
            "workload": workload, "output_rows": int(row["output_rows"]),
            "csv_median_s": round(float(row["csv_median_seconds"]), 6),
            "parquet_median_s": round(float(row["parquet_median_seconds"]), 6),
            "csv_over_parquet_speedup": round(float(row["speedup_csv_over_parquet"]), 6),
            "csv_median_peak_rss_mib": round(float(row["csv_median_peak_rss_mib"]), 3),
            "parquet_median_peak_rss_mib": round(float(row["parquet_median_peak_rss_mib"]), 3),
        })
    stats = {
        "source_rows": prep["financial_rows"], "quarters": prep["num_quarters"],
        "csv_mib": c / MIB, "parquet_mib": p / MIB,
        "partitioned_parquet_mib": part / MIB,
        "flat_parquet_reduction_pct": 100 * (c - p) / c,
        "csv_over_flat_parquet_size_ratio": c / p,
        "partitioned_vs_flat_pct": 100 * (part - p) / p,
        "workloads": perf_table,
        "interpretation": {
            "storage_format_comparison": "Uncompressed flat CSV versus flat Snappy Parquet",
            "partitioned_layout_timed_separately": True,
            "rss_units": "MiB of process peak RSS; not Arrow allocation alone",
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "report_storage_table.csv", fmt_table)
    write_csv(output / "report_performance_table.csv", perf_table)
    write_json(output / "storage_summary.json", stats)
    return stats


# 4. Evidence package and provenance

def analyze(run_dir: Path, output: Path) -> dict[str, Any]:
    run_dir = run_dir.expanduser().resolve()
    output = output.expanduser().resolve()
    expect(run_dir.is_dir(), f"Benchmark run directory does not exist: {run_dir}")
    expect(output != run_dir and run_dir not in output.parents,
           "Analysis output must be outside the source benchmark directory")
    expect(output != run_dir.parent and output not in run_dir.parents,
           "Analysis output must not overwrite a benchmark parent")
    audited = validate_run(run_dir)
    output.mkdir(parents=True, exist_ok=True)
    fig_dir = output / "figures"
    fig_dir.mkdir(exist_ok=True)
    for name in FIGURES:
        shutil.copy2(run_dir / name, fig_dir / name)
        expect(sha256(fig_dir / name) == audited["hashes"][name],
               f"Figure copy checksum differs: {name}")
    statistics_data = build_evidence_tables(run_dir, audited, output)
    write_json(output / "source_file_hashes.json", audited["hashes"])
    provenance = {
        "success": True,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "benchmark_run": run_dir.name,
        "benchmark_path": str(run_dir),
        "data_scope": audited["preparation"]["source_scope"],
        "source_rows": audited["preparation"]["financial_rows"],
        "quarter_count": audited["preparation"]["num_quarters"],
        "run_count_per_workload_format": audited["repetitions"],
        "warmups_per_workload_format": audited["warmups"],
        "total_trials_verified": len(audited["trials"]),
        "seed": audited["seed"],
        "acquisition_manifest_sha256": audited["preparation"].get("acquisition_manifest_sha256"),
        "acquisition_report_sha256": audited["preparation"].get("acquisition_report_sha256"),
        "git_commit_sha": audited["environment"].get("git_commit_sha"),
        "input_files_hashed": len(audited["hashes"]),
        "csv_parquet_round_trip_equality": True,
        "validation_checks": [
            "file presence", "dataset scope", "source record count", "equivalent data",
            "trial count and pairing", "trial aggregate agreement", "summaries recomputed",
            "speedups recomputed", "size accounting", "source figure checksum"
        ],
        "statistics": statistics_data,
        "outputs": ["storage_summary.json", "report_storage_table.csv",
                    "report_performance_table.csv", "source_file_hashes.json",
                    "analysis_validation.json", "figures/"],
    }
    write_json(output / "analysis_validation.json", provenance)
    return provenance


# 5. Command-line interface

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = analyze(args.benchmark_dir, args.output_dir)
    except (AnalysisError, OSError) as exc:
        parser.exit(1, f"Storage analysis validation failed: {exc}\n")
    print(json.dumps({"success": True, "benchmark_run": report["benchmark_run"],
                      "rows": report["source_rows"], "trials": report["total_trials_verified"],
                      "output": str(args.output_dir)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

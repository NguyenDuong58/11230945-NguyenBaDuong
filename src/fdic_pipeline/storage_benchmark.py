"""Part 1 / Storage: controlled CSV–Parquet comparison of FDIC Financials.

Inputs are verified quarter-partitioned Parquet files. A local, logically identical
CSV/Parquet pair is generated before read times are measured in fresh processes.
The acquisition directory is treated as read-only throughout this module.
"""

from __future__ import annotations

import argparse
import csv as stdcsv
import gc
import hashlib
import json
import logging
import math
import os
import platform
import random
import resource
import shutil
import statistics
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .acquisition import (
    DEFAULT_END, DEFAULT_START, arrow_schema, financial_path, quarter_ends, sha256,
)

LOG = logging.getLogger("storage_benchmark")
# 1. Comparison configuration and workload definitions
PROJECT_COLUMNS = ("CERT", "REPDTE", "ASSET", "DEP", "EQ", "ROA")
WORKLOADS = ("full_scan", "projection", "date_filter")
FORMATS = ("csv", "parquet")
FILTER_DATE = date(2020, 1, 1)
MIB = 1024 * 1024


class BenchmarkError(RuntimeError):
    """Invalid experiment input, dataset, or result."""


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, content: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(content, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise BenchmarkError(f"Missing JSON file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise BenchmarkError(f"JSON object expected: {path}")
    return data


# 2. Acquisition contract and local verified snapshot

def check_source_report(report: dict[str, Any], quarters: list[str],
                        expected_rows: int) -> None:
    if (report.get("success") is not True
            or report.get("expected_quarters") != len(quarters)
            or report.get("verified_quarters") != len(quarters)
            or report.get("missing_quarters") != []
            or report.get("invalid_artifacts") != []):
        raise BenchmarkError("Acquisition verification_report.json did not pass for this period")
    if report.get("scope") != {"start": quarters[0], "end": quarters[-1]}:
        raise BenchmarkError("Verification report scope is not the requested period")
    if report.get("financial_rows") != expected_rows:
        raise BenchmarkError(
            f"Financial row total differs: {report.get('financial_rows')} != {expected_rows}"
        )
    tables = report.get("tables", {})
    if not all(key in tables for key in ("failures", "institutions")):
        raise BenchmarkError("Reference tables are absent from the acquisition report")


def check_source_manifest(manifest: dict[str, Any], quarters: list[str],
                          start: date, end: date, expected_rows: int) -> None:
    spec = manifest.get("spec", {})
    if spec.get("start") != start.isoformat() or spec.get("end") != end.isoformat():
        raise BenchmarkError("Manifest scope differs from the benchmark scope")
    quarter_entries = manifest.get("financials", {})
    if not isinstance(quarter_entries, dict):
        raise BenchmarkError("Manifest financials section missing")
    total = 0
    for quarter in quarters:
        record = quarter_entries.get(quarter)
        if not isinstance(record, dict) or record.get("complete") is not True:
            raise BenchmarkError(f"Unverified quarter in manifest: {quarter}")
        if (not isinstance(record.get("rows"), int)
                or record.get("rows") <= 0
                or record.get("rows") != record.get("api_total")
                or not isinstance(record.get("sha256"), str)
                or len(record.get("sha256")) != 64):
            raise BenchmarkError(f"Incomplete manifest entry for {quarter}")
        total += record["rows"]
    if total != expected_rows:
        raise BenchmarkError(f"Manifest total differs: {total} != {expected_rows}")


def copy_verified_quarters(source: Path, local: Path, manifest: dict[str, Any],
                           quarters: list[str]) -> int:
    """Copy and hash-check exactly the requested Parquet files, never modify Drive input."""
    import pyarrow.parquet as pq

    total_bytes = 0
    for index, quarter in enumerate(quarters, start=1):
        src = financial_path(source, quarter)
        dst = financial_path(local, quarter)
        record = manifest["financials"][quarter]
        if not src.is_file():
            raise BenchmarkError(f"Missing acquisition partition: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.is_file() or sha256(dst) != record["sha256"]:
            shutil.copy2(src, dst)
        if sha256(dst) != record["sha256"]:
            raise BenchmarkError(f"Checksum mismatch after local copy: {quarter}")
        parquet_file = pq.ParquetFile(dst)
        if parquet_file.schema_arrow != arrow_schema("financials"):
            raise BenchmarkError(f"Unexpected schema in {quarter}")
        if parquet_file.metadata.num_rows != record["rows"]:
            raise BenchmarkError(f"Row count mismatch in {quarter}")
        total_bytes += dst.stat().st_size
        if index == 1 or index % 20 == 0 or index == len(quarters):
            LOG.info("Copied and verified %d/%d partitions", index, len(quarters))
    return total_bytes


# 3. Equivalent CSV/Parquet construction

def csv_options(schema: Any, columns: list[str] | None = None) -> Any:
    import pyarrow.csv as pacsv

    return pacsv.ConvertOptions(
        column_types=schema,
        strings_can_be_null=True,
        quoted_strings_can_be_null=False,
        null_values=[""],
        include_columns=columns or [],
    )


def read_csv_table(path: Path, schema: Any,
                   columns: list[str] | None = None) -> Any:
    import pyarrow.csv as pacsv

    return pacsv.read_csv(path, convert_options=csv_options(schema, columns))


def write_equivalent_files(local: Path, quarters: list[str], pair_dir: Path) -> tuple[Path, Path]:
    """Stream all source quarters into a flat CSV and flat Snappy Parquet pair."""
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    pair_dir.mkdir(parents=True, exist_ok=True)
    csv_path = pair_dir / "financials.csv"
    pq_path = pair_dir / "financials.snappy.parquet"
    csv_tmp = pair_dir / ".financials.csv.partial"
    pq_tmp = pair_dir / ".financials.snappy.parquet.partial"
    for path in (csv_tmp, pq_tmp):
        path.unlink(missing_ok=True)
    schema = arrow_schema("financials")
    n_rows = 0
    try:
        with (pacsv.CSVWriter(str(csv_tmp), schema,
                              write_options=pacsv.WriteOptions(include_header=True)) as csv_writer,
              pq.ParquetWriter(str(pq_tmp), schema, compression="snappy", use_dictionary=True) as pq_writer):
            for idx, quarter in enumerate(quarters, start=1):
                source_file = pq.ParquetFile(financial_path(local, quarter))
                for batch in source_file.iter_batches(batch_size=65_536):
                    data = pa.Table.from_batches([batch]).cast(schema, safe=True)
                    csv_writer.write_table(data)
                    pq_writer.write_table(data)
                    n_rows += data.num_rows
                if idx == 1 or idx % 20 == 0 or idx == len(quarters):
                    LOG.info("Converted %d/%d quarters (%d rows)", idx, len(quarters), n_rows)
        os.replace(csv_tmp, csv_path)
        os.replace(pq_tmp, pq_path)
    finally:
        csv_tmp.unlink(missing_ok=True)
        pq_tmp.unlink(missing_ok=True)
    return csv_path, pq_path


def check_pair_equal(csv_path: Path, parquet_path: Path,
                     expected_rows: int) -> None:
    """Compare all values, nulls and Arrow types across the two output formats."""
    import pyarrow.parquet as pq

    schema = arrow_schema("financials")
    csv_table = read_csv_table(csv_path, schema)
    parquet_table = pq.read_table(parquet_path)
    try:
        if (csv_table.num_rows != expected_rows
                or parquet_table.num_rows != expected_rows):
            raise BenchmarkError("CSV and Parquet do not contain the expected number of rows")
        if csv_table.schema != schema or parquet_table.schema != schema:
            raise BenchmarkError("CSV and Parquet schema differs from the acquisition schema")
        if not csv_table.equals(parquet_table, check_metadata=False):
            raise BenchmarkError("CSV/Parquet round-trip check failed: values or nulls differ")
    finally:
        del csv_table, parquet_table
        gc.collect()
    LOG.info("Lossless CSV/Parquet comparison passed for %d rows", expected_rows)


def prepare(source: Path, work: Path, start: date = DEFAULT_START,
            end: date = DEFAULT_END, expected_rows: int | None = None) -> dict[str, Any]:
    """Reproduce an immutable local snapshot and equivalent comparison files."""
    quarters = quarter_ends(start, end)
    report_path = source / "verification_report.json"
    manifest_path = source / "manifest.json"
    report = read_json(report_path)
    manifest = read_json(manifest_path)
    # The row count is observed from the current FDIC acquisition, never from a
    # previous benchmark or a constant copied from another execution.
    if expected_rows is None:
        expected_rows = report.get("financial_rows")
    if not isinstance(expected_rows, int) or isinstance(expected_rows, bool) or expected_rows <= 0:
        raise BenchmarkError("Expected Financials row count must be a positive integer")
    check_source_report(report, quarters, expected_rows)
    check_source_manifest(manifest, quarters, start, end, expected_rows)
    local = work / "source_snapshot"
    production_size = copy_verified_quarters(source, local, manifest, quarters)
    pair_dir = work / "format_pair"
    metadata_path = work / "preparation.json"
    manifest_hash = sha256(manifest_path)
    report_hash = sha256(report_path)
    csv_path = pair_dir / "financials.csv"
    pq_path = pair_dir / "financials.snappy.parquet"
    if metadata_path.exists():
        prior = read_json(metadata_path)
        if (prior.get("acquisition_manifest_sha256") == manifest_hash
                and prior.get("acquisition_report_sha256") == report_hash
                and prior.get("financial_rows") == expected_rows
                and csv_path.is_file() and pq_path.is_file()
                and prior.get("csv_sha256") == sha256(csv_path)
                and prior.get("parquet_sha256") == sha256(pq_path)):
            LOG.info("Verified format pair already exists, reusing it")
            return prior
    csv_path, pq_path = write_equivalent_files(local, quarters, pair_dir)
    check_pair_equal(csv_path, pq_path, expected_rows)
    result = {
        "prepared_at_utc": now_utc(),
        "source_directory": str(source),
        "source_scope": {"start": quarters[0], "end": quarters[-1]},
        "financial_rows": expected_rows,
        "num_quarters": len(quarters),
        "field_count": len(arrow_schema("financials")),
        "production_partitioned_parquet_bytes": production_size,
        "production_partition_count": len(quarters),
        "csv_bytes": csv_path.stat().st_size,
        "flat_parquet_bytes": pq_path.stat().st_size,
        "csv_sha256": sha256(csv_path),
        "parquet_sha256": sha256(pq_path),
        "acquisition_manifest_sha256": manifest_hash,
        "acquisition_report_sha256": report_hash,
        "reference_tables": {
            "failures_rows": report["tables"]["failures"]["rows"],
            "failures_missing_cert_rows": report["tables"]["failures"].get("missing_cert_rows"),
            "institutions_rows": report["tables"]["institutions"]["rows"],
        },
        "distinct_financial_certs": report.get("distinct_financial_certs"),
        "round_trip_equality": True,
        "format_pair_directory": str(pair_dir),
    }
    write_json(metadata_path, result)
    LOG.info("PREPARED | %d rows | CSV %.1f MiB | Parquet %.1f MiB",
             expected_rows, result["csv_bytes"] / MIB, result["flat_parquet_bytes"] / MIB)
    return result


def rss_mib() -> float:
    """Linux /proc-based current resident memory, before each read."""
    statm = Path("/proc/self/statm")
    pages = int(statm.read_text().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE") / MIB


# 4. Isolated read workloads and result agreement

def worker_read(pair_dir: Path, fmt: str, workload: str) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    if fmt not in FORMATS or workload not in WORKLOADS:
        raise BenchmarkError("Unknown format or workload")
    csv_file = pair_dir / "financials.csv"
    pq_file = pair_dir / "financials.snappy.parquet"
    schema = arrow_schema("financials")
    columns = None if workload == "full_scan" else list(PROJECT_COLUMNS)
    base_rss = rss_mib()
    started = time.perf_counter()
    if fmt == "csv":
        result = read_csv_table(csv_file, schema, columns)
        if workload == "date_filter":
            result = result.filter(pc.greater_equal(
                result.column("REPDTE"), pa.scalar(FILTER_DATE, type=pa.date32())))
    elif workload == "date_filter":
        result = pq.read_table(pq_file, columns=columns,
                               filters=[("REPDTE", ">=", FILTER_DATE)])
    else:
        result = pq.read_table(pq_file, columns=columns)
    # Force a deterministic, actual materialized read in each isolated process.
    n = result.num_rows
    cert_sum = pc.sum(result.column("CERT")).as_py()
    asset_sum = pc.sum(result.column("ASSET")).as_py()
    elapsed = time.perf_counter() - started
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    output = {
        "format": fmt, "workload": workload,
        "elapsed_s": elapsed,
        "baseline_rss_mib": base_rss,
        "peak_rss_mib": peak,
        "peak_over_baseline_mib": max(0.0, peak - base_rss),
        "rows": n,
        "cert_sum": cert_sum,
        "asset_sum": asset_sum,
    }
    del result
    return output


def assert_same_result(a: dict[str, Any], b: dict[str, Any]) -> None:
    if (a["rows"] != b["rows"] or a["cert_sum"] != b["cert_sum"]):
        raise BenchmarkError(f"Workload outputs differ: {a['workload']}")
    if not math.isclose(a["asset_sum"], b["asset_sum"], rel_tol=1e-12, abs_tol=1e-5):
        raise BenchmarkError(f"Workload ASSET aggregates differ: {a['workload']}")


def invoke_worker(script_path: Path, pair_dir: Path, fmt: str,
                  workload: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(script_path), "worker", "--pair-dir", str(pair_dir),
         "--format", fmt, "--workload", workload],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise BenchmarkError(
            f"Benchmark worker failed ({fmt}/{workload}, exit {completed.returncode}): "
            f"{completed.stderr[-2000:]}"
        )
    try:
        return json.loads(completed.stdout)
    except ValueError as exc:
        raise BenchmarkError(f"Invalid worker output: {completed.stdout[-1000:]}") from exc


# 5. Trial summaries and charts

def summarize(trials: list[dict[str, Any]], prep: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary = []
    comparisons = []
    for workload in WORKLOADS:
        fmt_rows = {}
        for fmt in FORMATS:
            runs = [r for r in trials if r["format"] == fmt and
                    r["workload"] == workload and not r["warmup"]]
            if not runs:
                raise BenchmarkError(f"No measured runs for {workload}/{fmt}")
            durations = [float(r["elapsed_s"]) for r in runs]
            peaks = [float(r["peak_rss_mib"]) for r in runs]
            row = {
                "workload": workload, "format": fmt, "runs": len(runs),
                "median_seconds": statistics.median(durations),
                "min_seconds": min(durations), "max_seconds": max(durations),
                "median_peak_rss_mib": statistics.median(peaks),
                "median_peak_over_baseline_mib": statistics.median(
                    float(r["peak_over_baseline_mib"]) for r in runs),
                "rows_output": runs[0]["rows"],
            }
            summary.append(row)
            fmt_rows[fmt] = row
        csv_row = fmt_rows["csv"]
        pq_row = fmt_rows["parquet"]
        comparisons.append({
            "workload": workload,
            "csv_median_seconds": csv_row["median_seconds"],
            "parquet_median_seconds": pq_row["median_seconds"],
            "speedup_csv_over_parquet": (
                csv_row["median_seconds"] / pq_row["median_seconds"]),
            "csv_median_peak_rss_mib": csv_row["median_peak_rss_mib"],
            "parquet_median_peak_rss_mib": pq_row["median_peak_rss_mib"],
            "output_rows": csv_row["rows_output"],
        })
    return summary, comparisons


def write_csv_records(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise BenchmarkError("Cannot export an empty result table")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = stdcsv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_charts(results: Path, prep: dict[str, Any],
                 comparisons: list[dict[str, Any]]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = ["Raw CSV", "Flat Parquet\nSnappy", "Partitioned\nParquet"]
    sizes = [prep["csv_bytes"] / MIB,
             prep["flat_parquet_bytes"] / MIB,
             prep["production_partitioned_parquet_bytes"] / MIB]
    fig, ax = plt.subplots(figsize=(7, 4.4))
    bars = ax.bar(labels, sizes)
    ax.bar_label(bars, fmt="%.1f", padding=3)
    ax.set_ylabel("Storage (MiB)")
    ax.set_title(f"Financials: storage footprint ({prep['financial_rows']:,} rows)")
    ax.set_ylim(top=max(sizes) * 1.20)
    fig.tight_layout()
    fig.savefig(results / "storage_sizes.png", dpi=160)
    plt.close(fig)

    x = list(range(len(comparisons)))
    width = 0.35
    csv_times = [r["csv_median_seconds"] for r in comparisons]
    pq_times = [r["parquet_median_seconds"] for r in comparisons]
    fig, ax = plt.subplots(figsize=(8, 4.7))
    left = ax.bar([i - width / 2 for i in x], csv_times, width, label="CSV")
    right = ax.bar([i + width / 2 for i in x], pq_times, width, label="Parquet")
    ax.bar_label(left, fmt="%.2f", padding=2, fontsize=8)
    ax.bar_label(right, fmt="%.2f", padding=2, fontsize=8)
    ax.set_xticks(x, [r["workload"].replace("_", " ") for r in comparisons])
    ax.set_ylabel("Median read time (s); lower is better")
    ax.set_title("PyArrow read benchmark, separate processes")
    ax.set_ylim(top=max(csv_times + pq_times) * 1.22)
    fig.legend(loc="upper center", bbox_to_anchor=(0.5, 0.99), ncol=2, frameon=False)
    fig.subplots_adjust(top=0.76, left=0.10, right=0.97, bottom=0.15)
    fig.savefig(results / "read_time_medians.png", dpi=160)
    plt.close(fig)

    csv_rss = [r["csv_median_peak_rss_mib"] for r in comparisons]
    pq_rss = [r["parquet_median_peak_rss_mib"] for r in comparisons]
    fig, ax = plt.subplots(figsize=(8, 4.7))
    left = ax.bar([i - width / 2 for i in x], csv_rss, width, label="CSV")
    right = ax.bar([i + width / 2 for i in x], pq_rss, width, label="Parquet")
    ax.bar_label(left, fmt="%.1f", padding=2, fontsize=8)
    ax.bar_label(right, fmt="%.1f", padding=2, fontsize=8)
    ax.set_xticks(x, [r["workload"].replace("_", " ") for r in comparisons])
    ax.set_ylabel("Median process peak RSS (MiB); lower is better")
    ax.set_title("PyArrow read process high-water memory")
    ax.set_ylim(top=max(csv_rss + pq_rss) * 1.22)
    fig.legend(loc="upper center", bbox_to_anchor=(0.5, 0.99), ncol=2, frameon=False)
    fig.subplots_adjust(top=0.76, left=0.10, right=0.97, bottom=0.15)
    fig.savefig(results / "peak_rss_medians.png", dpi=160)
    plt.close(fig)


def environment_info() -> dict[str, Any]:
    import pyarrow as pa
    import matplotlib

    info = {
        "timestamp_utc": now_utc(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "processor": platform.processor(),
        "logical_cpu_count": os.cpu_count(),
        "pyarrow_version": pa.__version__,
        "matplotlib_version": matplotlib.__version__,
        "pyarrow_threads": "default",
        "memory_measurement": "Linux resource.ru_maxrss in fresh worker processes",
        "read_engine": "PyArrow for both CSV and Parquet",
        "workloads": {"full_scan": "all columns",
                      "projection": list(PROJECT_COLUMNS),
                      "date_filter": f"{list(PROJECT_COLUMNS)}; REPDTE >= {FILTER_DATE}"},
        "repeated_trials": "randomized execution order; one new Python worker per trial",
        "data_read_location": "Colab local /content filesystem (not Google Drive mount)",
    }
    try:
        info["physical_memory_bytes"] = (
            os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    except (ValueError, OSError):
        pass
    return info


def write_report(results: Path, prep: dict[str, Any], summary: list[dict[str, Any]],
                 comparisons: list[dict[str, Any]], repetitions: int, warmups: int) -> None:
    c = prep["csv_bytes"]
    p = prep["flat_parquet_bytes"]
    prod = prep["production_partitioned_parquet_bytes"]
    lines = [
        "# BFT65B — Storage benchmark evidence (Part 1)", "",
        f"Generated (UTC): {now_utc()}", "",
        "## Data and validation", "",
        f"- Source: {prep['source_directory']}",
        f"- Scope: {prep['source_scope']['start']}–{prep['source_scope']['end']} ({prep['num_quarters']} quarters)",
        f"- Rows: {prep['financial_rows']:,}; columns: {prep['field_count']}",
        "- Full CSV/Parquet Arrow table equality: PASS (types, values, nulls, order)",
        "- Original dataset: validated manifest and per-quarter SHA-256 checks",
        "", "## Storage size", "",
        "| Representation | MiB |", "|---|---:|",
        f"| Uncompressed flat CSV | {c/MIB:.2f} |",
        f"| Snappy flat Parquet | {p/MIB:.2f} |",
        f"| Original partitioned Snappy Parquet (170 files) | {prod/MIB:.2f} |",
        "", f"CSV / flat Parquet size ratio: **{c/p:.3f}x**.", "",
        "## Read timings", "",
        f"{repetitions} measured repetitions per workload and format, after {warmups} warmup run(s).",
        "| Workload | CSV median (s) | Parquet median (s) | CSV/Parquet time |",
        "|---|---:|---:|---:|",
    ]
    for r in comparisons:
        lines.append(
            f"| {r['workload']} | {r['csv_median_seconds']:.3f} | "
            f"{r['parquet_median_seconds']:.3f} | "
            f"{r['speedup_csv_over_parquet']:.3f}x |")
    lines += [
        "", "## Peak resident memory", "",
        "| Workload | CSV median peak RSS (MiB) | Parquet median peak RSS (MiB) |",
        "|---|---:|---:|",
    ]
    for r in comparisons:
        lines.append(
            f"| {r['workload']} | {r['csv_median_peak_rss_mib']:.2f} | "
            f"{r['parquet_median_peak_rss_mib']:.2f} |")
    lines += [
        "", "## Methodology and limitations", "",
        "- Both formats store the same observations, column order and PyArrow types.",
        "- A single flat CSV is compared with a single flat Parquet file for read timings.",
        "- The production partitioned Parquet layout is included for storage-size context, not as a timed input.",
        "- Timings use the same PyArrow library and local Colab storage; they are NOT Polars-vs-pandas results.",
        "- Projection selects CERT, REPDTE, ASSET, DEP, EQ, ROA.",
        f"- Date filter retains REPDTE >= {FILTER_DATE.isoformat()}. The Parquet reader can prune row groups; CSV is filtered after parsing.",
        "- Each trial runs in a fresh Python subprocess. Trial order is shuffled with a fixed seed.",
        "- Peak RSS is process high-water memory and includes Python/Arrow runtime overhead; it is not a pure Arrow allocation measure.",
        "- This is a warm-cache benchmark after warmup; OS page cache cannot be fully controlled on managed Colab.",
        "- Results depend on the Colab CPU, RAM, workload and library versions recorded in environment.json.",
        "- Results do not establish that a Spark cluster is needed; scaling decisions require Part 3 experiments.",
        "", "## Output files", "",
        "`preparation.json`, `environment.json`, `storage_sizes.csv`, `trials.csv`, `summary.csv`, "
        "`comparisons.csv`, `storage_sizes.png`, `read_time_medians.png`, `peak_rss_medians.png`, `storage_benchmark.log`.",
    ]
    (results / "storage_report_evidence.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# 6. Experimental execution and recorded evidence

def run_benchmark(work: Path, results: Path, script_path: Path,
                  repetitions: int = 5, warmups: int = 1,
                  seed: int = 11230945) -> dict[str, Any]:
    if repetitions < 3 or warmups < 0:
        raise BenchmarkError("Use at least 3 measured repetitions and nonnegative warmups")
    prep = read_json(work / "preparation.json")
    pair_dir = work / "format_pair"
    for fmt, path, hash_field in [
        ("csv", pair_dir / "financials.csv", "csv_sha256"),
        ("parquet", pair_dir / "financials.snappy.parquet", "parquet_sha256")]:
        if not path.is_file() or sha256(path) != prep[hash_field]:
            raise BenchmarkError(f"Pair changed since preparation: {fmt}")
    if prep.get("round_trip_equality") is not True:
        raise BenchmarkError("Prepare has not verified source format equivalence")
    results.mkdir(parents=True, exist_ok=True)
    trials: list[dict[str, Any]] = []
    rng = random.Random(seed)
    for trial_index in range(-warmups, repetitions):
        tasks = [(w, f) for w in WORKLOADS for f in FORMATS]
        rng.shuffle(tasks)
        observed = {}
        for workload, fmt in tasks:
            row = invoke_worker(script_path, pair_dir, fmt, workload)
            row.update({"trial": trial_index, "warmup": trial_index < 0})
            trials.append(row)
            observed[(workload, fmt)] = row
            LOG.info("%s %s repeat=%d: %.3fs, peak RSS %.1f MiB, rows=%s",
                     workload, fmt, trial_index, row["elapsed_s"],
                     row["peak_rss_mib"], row["rows"])
        for workload in WORKLOADS:
            assert_same_result(observed[(workload, "csv")],
                               observed[(workload, "parquet")])
    summary, comparisons = summarize(trials, prep)
    sizes = [
        {"format": "csv", "layout": "single-file", "bytes": prep["csv_bytes"],
         "mib": prep["csv_bytes"]/MIB},
        {"format": "parquet", "layout": "single-file-snappy", "bytes": prep["flat_parquet_bytes"],
         "mib": prep["flat_parquet_bytes"]/MIB},
        {"format": "parquet", "layout": "original-partitioned-snappy",
         "bytes": prep["production_partitioned_parquet_bytes"],
         "mib": prep["production_partitioned_parquet_bytes"]/MIB},
    ]
    write_csv_records(results / "trials.csv", trials)
    write_csv_records(results / "summary.csv", summary)
    write_csv_records(results / "comparisons.csv", comparisons)
    write_csv_records(results / "storage_sizes.csv", sizes)
    write_json(results / "preparation.json", prep)
    env = environment_info()
    env.update({"repetitions": repetitions, "warmups": warmups, "seed": seed})
    # The Git commit identifies the implementation used for this run, if the
    # notebook executed from a cloned Git repository.
    git_dir = Path(__file__).resolve().parents[2]
    commit = subprocess.run(["git", "-C", str(git_dir), "rev-parse", "HEAD"],
                            text=True, capture_output=True, check=False)
    env["git_commit_sha"] = commit.stdout.strip() if commit.returncode == 0 else None
    write_json(results / "environment.json", env)
    write_charts(results, prep, comparisons)
    write_report(results, prep, summary, comparisons, repetitions, warmups)
    result = {
        "success": True,
        "expected_rows": prep["financial_rows"],
        "round_trip_equality": prep["round_trip_equality"],
        "trial_count": len(trials),
        "csv_over_parquet_size_ratio": prep["csv_bytes"] / prep["flat_parquet_bytes"],
        "comparisons": comparisons,
        "results_directory": str(results),
    }
    write_json(results / "benchmark_verification.json", result)
    return result


def configure_logging(log_path: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s UTC | %(levelname)s | %(message)s",
                        handlers=handlers, force=True)
    logging.Formatter.converter = time.gmtime


# 7. Command-line interface

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    cmd = parser.add_subparsers(dest="command", required=True)
    p_prepare = cmd.add_parser("prepare", help="Copy verified Parquet, create CSV/Parquet pair, validate")
    p_prepare.add_argument("--source-dir", required=True, type=Path)
    p_prepare.add_argument("--work-dir", required=True, type=Path)
    p_prepare.add_argument("--start", default=DEFAULT_START.isoformat())
    p_prepare.add_argument("--end", default=DEFAULT_END.isoformat())
    p_prepare.add_argument("--expected-rows", type=int, default=None,
                           help="Optional guard against an independently known count")
    p_benchmark = cmd.add_parser("benchmark", help="Run measured reads and export evidence")
    p_benchmark.add_argument("--work-dir", required=True, type=Path)
    p_benchmark.add_argument("--results-dir", required=True, type=Path)
    p_benchmark.add_argument("--repetitions", type=int, default=5)
    p_benchmark.add_argument("--warmups", type=int, default=1)
    p_benchmark.add_argument("--seed", type=int, default=11230945)
    p_worker = cmd.add_parser("worker", help=argparse.SUPPRESS)
    p_worker.add_argument("--pair-dir", required=True, type=Path)
    p_worker.add_argument("--format", choices=FORMATS, required=True)
    p_worker.add_argument("--workload", choices=WORKLOADS, required=True)
    args = parser.parse_args(argv)
    if args.command == "worker":
        print(json.dumps(worker_read(args.pair_dir, args.format, args.workload)))
        return 0
    log_path = (args.work_dir / "logs" / "storage_prepare.log"
                if args.command == "prepare"
                else args.results_dir / "storage_benchmark.log")
    configure_logging(log_path)
    try:
        if args.command == "prepare":
            result = prepare(args.source_dir.resolve(), args.work_dir.resolve(),
                             date.fromisoformat(args.start),
                             date.fromisoformat(args.end), args.expected_rows)
        else:
            result = run_benchmark(args.work_dir.resolve(), args.results_dir.resolve(),
                                   Path(__file__).resolve().parents[2] / "run_storage_benchmark.py",
                                   args.repetitions, args.warmups, args.seed)
        LOG.info("SUCCESS: %s", json.dumps(result, ensure_ascii=False))
        return 0
    except (BenchmarkError, OSError, ValueError, ImportError, KeyError) as exc:
        LOG.error("Storage benchmark failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

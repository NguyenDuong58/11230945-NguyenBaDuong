"""Tests for analysis of an existing, immutable benchmark result directory."""
from __future__ import annotations

import copy
import csv
import json
import math
import statistics
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fdic_pipeline import storage_analysis as sa


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def fixture_run(tmp_path):
    run = tmp_path / "results" / "run_20990101T000000Z"
    run.mkdir(parents=True)
    reps, warmups = 5, 1
    prep = {
        "round_trip_equality": True, "financial_rows": 4321,
        "num_quarters": 170, "field_count": 33,
        "source_scope": {"start": "19840331", "end": "20260630"},
        "csv_bytes": 120 * sa.MIB, "flat_parquet_bytes": 80 * sa.MIB,
        "production_partitioned_parquet_bytes": 82 * sa.MIB,
        "production_partition_count": 170,
    }
    env = {"repetitions": reps, "warmups": warmups,
           "read_engine": "PyArrow for both CSV and Parquet", "seed": 11230945}
    trials = []
    summary = []
    comparisons = []
    for w in sa.WORKLOADS:
        data = {}
        for fmt in sa.FORMATS:
            vals = []
            for i in range(-warmups, reps):
                seconds = (1.5 if fmt == "csv" else 0.75) + i * 0.01
                peak = (250.0 if fmt == "csv" else 200.0) + i
                row = {"workload": w, "format": fmt, "trial": i, "warmup": i < 0,
                       "elapsed_s": seconds, "peak_rss_mib": peak,
                       "baseline_rss_mib": 90.0,
                       "peak_over_baseline_mib": peak - 90.0,
                       "rows": 2000 if w == "date_filter" else 4321,
                       "cert_sum": 560_000, "asset_sum": 870_000.5}
                trials.append(row)
                if i >= 0:
                    vals.append(row)
            med = statistics.median(r["elapsed_s"] for r in vals)
            med_peak = statistics.median(r["peak_rss_mib"] for r in vals)
            row = {"workload": w, "format": fmt, "runs": reps,
                   "median_seconds": med, "min_seconds": min(r["elapsed_s"] for r in vals),
                   "max_seconds": max(r["elapsed_s"] for r in vals),
                   "median_peak_rss_mib": med_peak,
                   "median_peak_over_baseline_mib": med_peak - 90.0,
                   "rows_output": vals[0]["rows"]}
            summary.append(row)
            data[fmt] = row
        comparisons.append({
            "workload": w,
            "csv_median_seconds": data["csv"]["median_seconds"],
            "parquet_median_seconds": data["parquet"]["median_seconds"],
            "speedup_csv_over_parquet": data["csv"]["median_seconds"] / data["parquet"]["median_seconds"],
            "csv_median_peak_rss_mib": data["csv"]["median_peak_rss_mib"],
            "parquet_median_peak_rss_mib": data["parquet"]["median_peak_rss_mib"],
            "output_rows": data["csv"]["rows_output"],
        })
    verification = {
        "success": True, "expected_rows": 4321,
        "round_trip_equality": True, "trial_count": len(trials),
        "csv_over_parquet_size_ratio": prep["csv_bytes"] / prep["flat_parquet_bytes"],
        "comparisons": comparisons,
    }
    sizes = [
        {"format": "csv", "layout": "single-file", "bytes": prep["csv_bytes"],
         "mib": prep["csv_bytes"] / sa.MIB},
        {"format": "parquet", "layout": "single-file-snappy", "bytes": prep["flat_parquet_bytes"],
         "mib": prep["flat_parquet_bytes"] / sa.MIB},
        {"format": "parquet", "layout": "original-partitioned-snappy",
         "bytes": prep["production_partitioned_parquet_bytes"],
         "mib": prep["production_partitioned_parquet_bytes"] / sa.MIB},
    ]
    for name, data in [("preparation.json", prep), ("environment.json", env),
                       ("benchmark_verification.json", verification)]:
        (run / name).write_text(json.dumps(data), encoding="utf-8")
    for name, data in [("trials.csv", trials), ("summary.csv", summary),
                       ("comparisons.csv", comparisons), ("storage_sizes.csv", sizes)]:
        write_csv(run / name, data)
    for name in sa.FIGURES:
        (run / name).write_bytes(b"PNG fixture, not decoded " + name.encode())
    return run


def test_validate_fixture_and_derive_stats(tmp_path):
    run = fixture_run(tmp_path)
    result = sa.validate_run(run)
    assert result["repetitions"] == 5
    assert result["warmups"] == 1
    assert len(result["hashes"]) == len(sa.INPUTS)
    out = tmp_path / "analysis" / run.name
    output = sa.analyze(run, out)
    assert output["success"] is True
    assert output["total_trials_verified"] == 36
    assert output["statistics"]["flat_parquet_reduction_pct"] == pytest.approx(33.3333333)
    stats = sa.load_json(out / "storage_summary.json")
    assert stats["source_rows"] == 4321
    assert stats["quarters"] == 170
    assert stats["flat_parquet_reduction_pct"] == pytest.approx(33.3333333)
    assert stats["interpretation"]["partitioned_layout_timed_separately"] is True
    assert (out / "figures" / "storage_sizes.png").read_bytes() == (run / "storage_sizes.png").read_bytes()
    assert len(sa.load_csv(out / "report_performance_table.csv")) == 3
    assert sa.load_json(out / "analysis_validation.json")["success"] is True


def test_dynamic_financial_row_count(tmp_path):
    """A new FDIC extract need not equal any previous execution's row count."""
    run = fixture_run(tmp_path)
    new_count = 4322
    prep_path = run / "preparation.json"
    prep = sa.load_json(prep_path)
    prep["financial_rows"] = new_count
    prep_path.write_text(json.dumps(prep))
    verify_path = run / "benchmark_verification.json"
    verification = sa.load_json(verify_path)
    verification["expected_rows"] = new_count
    verify_path.write_text(json.dumps(verification))
    for filename in ("trials.csv", "summary.csv", "comparisons.csv"):
        rows = sa.load_csv(run / filename)
        for row in rows:
            if row.get("workload") in ("full_scan", "projection"):
                key = "rows" if filename == "trials.csv" else (
                    "rows_output" if filename == "summary.csv" else "output_rows")
                row[key] = str(new_count)
        write_csv(run / filename, rows)
    result = sa.validate_run(run)
    assert result["preparation"]["financial_rows"] == new_count


def test_reject_undocumented_scope(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "preparation.json"
    data = json.loads(path.read_text())
    data["num_quarters"] = 169
    path.write_text(json.dumps(data))
    with pytest.raises(sa.AnalysisError, match="quarter count"):
        sa.validate_run(run)


def test_reject_incorrect_speedup(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "comparisons.csv"
    rows = sa.load_csv(path)
    rows[0]["speedup_csv_over_parquet"] = "99"
    write_csv(path, rows)
    with pytest.raises(sa.AnalysisError, match="speedup"):
        sa.validate_run(run)


def test_reject_missing_trial(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "trials.csv"
    rows = sa.load_csv(path)
    write_csv(path, rows[:-1])
    with pytest.raises(sa.AnalysisError, match="Trial count"):
        sa.validate_run(run)


def test_reject_wrong_warmup(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "trials.csv"
    rows = sa.load_csv(path)
    rows[0]["warmup"] = "False"
    write_csv(path, rows)
    with pytest.raises(sa.AnalysisError, match="warmup flag"):
        sa.validate_run(run)


def test_reject_duplicate_trial(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "trials.csv"
    rows = sa.load_csv(path)
    rows[1]["trial"] = rows[0]["trial"]
    write_csv(path, rows)
    with pytest.raises(sa.AnalysisError, match="Repeated trial"):
        sa.validate_run(run)


def test_reject_data_mismatch_between_formats(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "trials.csv"
    rows = sa.load_csv(path)
    for row in rows:
        if row["format"] == "parquet":
            row["asset_sum"] = "9999"
            break
    write_csv(path, rows)
    with pytest.raises(sa.AnalysisError, match="asset sum"):
        sa.validate_run(run)


def test_reject_wrong_storage_bytes(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "storage_sizes.csv"
    rows = sa.load_csv(path)
    rows[0]["bytes"] = "123"
    write_csv(path, rows)
    with pytest.raises(sa.AnalysisError, match="bytes"):
        sa.validate_run(run)


def test_reject_missing_figure(tmp_path):
    run = fixture_run(tmp_path)
    (run / "peak_rss_medians.png").unlink()
    with pytest.raises(sa.AnalysisError, match="Missing benchmark evidence"):
        sa.validate_run(run)


def test_reject_failed_verification(tmp_path):
    run = fixture_run(tmp_path)
    path = run / "benchmark_verification.json"
    data = json.loads(path.read_text())
    data["success"] = False
    path.write_text(json.dumps(data))
    with pytest.raises(sa.AnalysisError, match="did not pass"):
        sa.validate_run(run)


def test_no_source_mutation(tmp_path):
    run = fixture_run(tmp_path)
    before = {p.name: sa.sha256(p) for p in run.iterdir() if p.is_file()}
    sa.analyze(run, tmp_path / "evidence")
    after = {p.name: sa.sha256(p) for p in run.iterdir() if p.is_file()}
    assert before == after


def test_must_write_outside_source(tmp_path):
    run = fixture_run(tmp_path)
    with pytest.raises(sa.AnalysisError, match="outside"):
        sa.analyze(run, run / "analysis")


def test_fail_closed_for_nonnumeric_and_nan():
    with pytest.raises(sa.AnalysisError):
        sa.number("NaN", "elapsed_s")
    with pytest.raises(sa.AnalysisError):
        sa.number("n/a", "elapsed_s")
    with pytest.raises(sa.AnalysisError):
        sa.flag("not true", "warmup")


def test_slow_parquet_language():
    assert "slower" in sa.speed_phrase("full_scan", 1.0, 2.0)
    assert "as fast" in sa.speed_phrase("full_scan", 2.0, 1.0)

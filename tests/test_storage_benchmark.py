"""Storage-benchmark tests; integration tests require installed PyArrow."""

import csv
import hashlib
import json
from datetime import date
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fdic_pipeline import storage_benchmark as b  # noqa: E402
from fdic_pipeline.acquisition import arrow_schema, financial_path, sha256  # noqa: E402


def fake_report():
    return {
        "success": True, "expected_quarters": 2, "verified_quarters": 2,
        "financial_rows": 5, "missing_quarters": [], "invalid_artifacts": [],
        "scope": {"start": "20230331", "end": "20230630"},
        "tables": {"failures": {"rows": 3}, "institutions": {"rows": 2}},
    }


def fake_manifest():
    return {"spec": {"start": "2023-03-31", "end": "2023-06-30"},
            "financials": {
                "20230331": {"complete": True, "rows": 3, "api_total": 3,
                             "sha256": "0" * 64},
                "20230630": {"complete": True, "rows": 2, "api_total": 2,
                             "sha256": "0" * 64},
            }}


def test_source_report_accepts_valid():
    b.check_source_report(fake_report(), ["20230331", "20230630"], 5)


@pytest.mark.parametrize("field,value", [
    ("success", False), ("verified_quarters", 1), ("financial_rows", 6),
    ("missing_quarters", ["20230331"]), ("invalid_artifacts", ["failures"]),
])
def test_bad_source_report_rejected(field, value):
    report = fake_report()
    report[field] = value
    with pytest.raises(b.BenchmarkError):
        b.check_source_report(report, ["20230331", "20230630"], 5)


def test_source_scope_rejected():
    report = fake_report()
    report["scope"]["start"] = "20200101"
    with pytest.raises(b.BenchmarkError, match="scope"):
        b.check_source_report(report, ["20230331", "20230630"], 5)


def test_manifest_check():
    b.check_source_manifest(fake_manifest(), ["20230331", "20230630"],
                            date(2023, 3, 31), date(2023, 6, 30), 5)
    manifest = fake_manifest()
    manifest["financials"]["20230331"]["rows"] = 1
    with pytest.raises(b.BenchmarkError):
        b.check_source_manifest(manifest, ["20230331", "20230630"],
                                date(2023, 3, 31), date(2023, 6, 30), 5)


def test_worker_result_equality():
    a = {"workload": "projection", "rows": 7, "cert_sum": 100,
         "asset_sum": 999.25}
    b.assert_same_result(a, dict(a))
    with pytest.raises(b.BenchmarkError):
        b.assert_same_result(a, {**a, "rows": 6})
    with pytest.raises(b.BenchmarkError):
        b.assert_same_result(a, {**a, "asset_sum": 999.0})


def test_summarize_three_workloads():
    trial_rows = []
    for workload in b.WORKLOADS:
        for fmt in b.FORMATS:
            for idx, seconds in enumerate([1.0, 2.0, 3.0]):
                trial_rows.append({
                    "workload": workload, "format": fmt, "warmup": False,
                    "elapsed_s": seconds if fmt == "csv" else seconds / 2,
                    "peak_rss_mib": 150.0, "peak_over_baseline_mib": 50.0,
                    "rows": 6,
                })
    summary, comparisons = b.summarize(trial_rows, {})
    assert len(summary) == 6
    assert len(comparisons) == 3
    assert all(x["speedup_csv_over_parquet"] == 2 for x in comparisons)


def test_prepare_requires_good_report(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    b.write_json(source / "verification_report.json", {"success": False})
    b.write_json(source / "manifest.json", fake_manifest())
    with pytest.raises(b.BenchmarkError, match="verification_report"):
        b.prepare(source, tmp_path / "work", date(2023, 3, 31), date(2023, 6, 30), 5)


def test_results_csv_written(tmp_path):
    b.write_csv_records(tmp_path / "summary.csv", [{"workload": "a", "seconds": 1.234}])
    with (tmp_path / "summary.csv").open(newline="") as handle:
        assert list(csv.DictReader(handle))[0]["workload"] == "a"


@pytest.mark.skipif(__import__("importlib").util.find_spec("pyarrow") is None,
                    reason="PyArrow not installed in this execution environment")
def test_real_arrow_csv_parquet_pipeline_end_to_end(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = tmp_path / "source"
    source.mkdir()
    schema = arrow_schema("financials")
    records = {
        "20230331": [
            {"CERT": 1, "REPDTE": date(2023, 3, 31), "STNAME": "New York, USA",
             "ASSET": 102.123456789, "DEP": 40.0, "EQ": 9.0, "ROA": -2.5},
            {"CERT": 2, "REPDTE": date(2023, 3, 31), "STNAME": "",
             "ASSET": None, "DEP": 0.0, "EQ": 6.0, "ROA": 0.0},
            {"CERT": 3, "REPDTE": date(2023, 3, 31), "STNAME": None,
             "ASSET": -4.0, "DEP": None, "EQ": None, "ROA": None},
        ],
        "20230630": [
            {"CERT": 1, "REPDTE": date(2023, 6, 30), "STNAME": "Café \"École\"",
             "ASSET": 104.0, "DEP": 43.1, "EQ": 10.1, "ROA": 2.5},
            {"CERT": 3, "REPDTE": date(2023, 6, 30), "STNAME": "NY",
             "ASSET": 106.0, "DEP": 44.0, "EQ": 11.0, "ROA": 2.2},
        ],
    }
    manifest = fake_manifest()
    for quarter, rowlist in records.items():
        path = financial_path(source, quarter)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rowlist, schema=schema)
        pq.write_table(table, path, compression="snappy")
        manifest["financials"][quarter]["sha256"] = sha256(path)
    b.write_json(source / "manifest.json", manifest)
    b.write_json(source / "verification_report.json", fake_report())
    work = tmp_path / "work"
    first = b.prepare(source, work, date(2023, 3, 31), date(2023, 6, 30), 5)
    assert first["round_trip_equality"]
    assert first["financial_rows"] == 5
    second = b.prepare(source, work, date(2023, 3, 31), date(2023, 6, 30), 5)
    assert second["csv_sha256"] == first["csv_sha256"]
    script = Path(__file__).resolve().parents[1] / "run_storage_benchmark.py"
    results_dir = tmp_path / "results"
    outcome = b.run_benchmark(work, results_dir, script, repetitions=3, warmups=0)
    assert outcome["success"] and outcome["trial_count"] == 18
    assert (results_dir / "read_time_medians.png").is_file()
    assert (results_dir / "peak_rss_medians.png").is_file()
    assert (results_dir / "storage_report_evidence.md").is_file()
    assert len(list(csv.DictReader((results_dir / "comparisons.csv").open()))) == 3

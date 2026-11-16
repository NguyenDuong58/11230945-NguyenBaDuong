"""Parquet integration tests on synthetic FDIC-shaped reports and references."""
from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pytest
pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq

from fdic_pipeline.acquisition import arrow_schema, financial_path, table_path, sha256
from fdic_pipeline.panel import PanelError, build_panel, verify_panel, panel_schema
from fdic_pipeline.project_flow import initialize, stage_dir, publish_stage, resolve_stage

QUARTERS = ("19840331", "19840630", "19840930")


def _write(path: Path, endpoint: str, rows: list[dict]) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=arrow_schema(endpoint)),
                   path, compression="snappy")
    return {"rows": len(rows), "sha256": sha256(path)}


def _financial(cert: int, repdte: date, assets: float, dep: float,
               *, eq: float = 15.0, roa: float = 1.5) -> dict:
    return {"CERT": cert, "REPDTE": repdte, "STNAME": "TX",
            "ASSET": assets, "DEP": dep, "EQ": eq, "LNLSGR": 10.0,
            "NCLNLS": 1.0, "ROA": roa, "ROE": 7.0, "CHBAL": 8.0}


def _synthetic_source(tmp_path: Path):
    src = tmp_path / "acquisition"
    source_rows = [
        [_financial(1, date(1984, 3, 31), 100.0, 80.0),
         _financial(2, date(1984, 3, 31), 0.0, 0.0),
         _financial(3, date(1984, 3, 31), 110.0, 95.0)],
        [_financial(1, date(1984, 6, 30), 90.0, 95.0)],
        [_financial(1, date(1984, 9, 30), 70.0, 65.0, eq=-2.0, roa=150.0),
         _financial(2, date(1984, 9, 30), 70.0, 60.0),
         _financial(3, date(1984, 9, 30), 105.0, 100.0)],
    ]
    financials = {}
    for q, rows in zip(QUARTERS, source_rows):
        financials[q] = _write(financial_path(src, q), "financials", rows)
    failures = [
        {"CERT": 1, "FAILDATE": "1984-08-15", "NAME": "F1"},
        {"CERT": 3, "FAILDATE": "1984-10-01", "NAME": "F3"},
        {"CERT": 2, "FAILDATE": None, "NAME": "Undated"},
        {"CERT": None, "FAILDATE": "1984-05-01", "NAME": "Unlinked"},
    ]
    institutes = [
        {"CERT": 1, "NAME": "F1", "ENDEFYMD": "19841231", "ACTIVE": "0"},
        {"CERT": 2, "NAME": "F2", "ENDEFYMD": "19841231", "ACTIVE": "0"},
        {"CERT": 3, "NAME": "F3", "ENDEFYMD": "19841231", "ACTIVE": "0"},
    ]
    refs = {
        "failures": _write(table_path(src, "failures"), "failures", failures),
        "institutions": _write(table_path(src, "institutions"), "institutions", institutes),
    }
    (src / "manifest.json").write_text(json.dumps({
        "financials": financials, "tables": refs,
    }), encoding="utf-8")
    (src / "verification_report.json").write_text(json.dumps({"success": True}), encoding="utf-8")
    contract = {
        "manifest_sha256": sha256(src / "manifest.json"),
        "verification_report_sha256": sha256(src / "verification_report.json"),
        "financial_rows": 7, "distinct_financial_certs": 3,
        "failures_rows": 4, "failures_missing_cert_rows": 1,
        "institutions_rows": 3,
    }
    return src, contract


def _rows(path: Path):
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_synthetic_panel_reconciliation_quality_and_event_separation(tmp_path):
    source, contract = _synthetic_source(tmp_path)
    out = tmp_path / "quality_panel"
    result = build_panel(source, out, source_summary=contract,
                         storage_verification_sha256="b" * 64,
                         code_commit="a" * 40, expected_quarters=QUARTERS)
    assert result["success"] is True
    assert result["quarters"] == 3
    assert result["source_rows"] == 7
    assert result["panel_rows"] == 6
    assert result["quarantined_rows"] == 1
    assert result["source_certificates"] == 3
    assert result["failure_missing_cert_rows"] == 1
    assert result["reporting_gap_intervals"] == 2
    assert result["terminal_class_counts"]["inconsistent_event_timing"] == 1
    assert result["terminal_class_counts"]["confirmed_failure"] == 1
    assert result["terminal_class_counts"]["ambiguous_failure_evidence"] == 1
    assert "FAILDATE" not in panel_schema().names
    q2 = out / "panel" / "year=1984" / "quarter=2" / "part-00000.parquet"
    rows = pq.ParquetFile(q2).read().to_pylist()
    assert rows[0]["DQ_DEPOSITS_GT_ASSETS"] is True
    assert rows[0]["PRIOR_REPORT_COUNT"] == 1
    q3 = pq.ParquetFile(out / "panel" / "year=1984" / "quarter=3" / "part-00000.parquet").read().to_pylist()
    assert next(r for r in q3 if r["CERT"] == 3)["REPORT_GAP_QUARTERS"] == 1
    assert next(r for r in q3 if r["CERT"] == 2)["PRIOR_REPORT_COUNT"] == 1
    assert len(_rows(out / "audits" / "failure_linkage.csv")) == 4
    assert len(_rows(out / "audits" / "quarantined_rows.csv")) == 1
    assert len(_rows(out / "audits" / "terminal_exits.csv")) == 3
    assert len(_rows(out / "audits" / "field_year_missingness.csv")) > 1
    assert len(list((out / "figures").glob("*.png"))) == 4
    assert verify_panel(source, out, source_summary=contract,
                        storage_verification_sha256="b" * 64,
                        expected_quarters=QUARTERS) == result


def test_readonly_verification_rejects_tampered_audit(tmp_path):
    source, contract = _synthetic_source(tmp_path)
    out = tmp_path / "quality_panel"
    build_panel(source, out, source_summary=contract,
                storage_verification_sha256="b" * 64, code_commit="a" * 40,
                expected_quarters=QUARTERS)
    path = out / "audits" / "failure_counts_by_year.csv"
    path.write_text(path.read_text(encoding="utf-8") + "modified\n", encoding="utf-8")
    with pytest.raises(PanelError, match="Audit evidence integrity"):
        verify_panel(source, out, source_summary=contract,
                     storage_verification_sha256="b" * 64,
                     expected_quarters=QUARTERS)


def test_no_silent_overwriting_of_verified_or_incomplete_stage(tmp_path):
    source, contract = _synthetic_source(tmp_path)
    out = tmp_path / "quality_panel"
    build_panel(source, out, source_summary=contract,
                storage_verification_sha256="b" * 64, code_commit="a" * 40,
                expected_quarters=QUARTERS)
    with pytest.raises(PanelError, match="Refusing to overwrite"):
        build_panel(source, out, source_summary=contract,
                    storage_verification_sha256="b" * 64,
                    code_commit="a" * 40, expected_quarters=QUARTERS)


def test_registry_accepts_quality_panel_and_checks_acquisition_parent(tmp_path):
    initialize(tmp_path, "a" * 40)
    acquisition = stage_dir(tmp_path, "acquisition")
    acquisition.mkdir()
    (acquisition / "acquisition_gate.json").write_text('{"success":true}')
    publish_stage(tmp_path, "acquisition", "acquisition_gate.json")
    storage = stage_dir(tmp_path, "storage")
    storage.mkdir()
    (storage / "part1_validation.json").write_text('{"success":true}')
    publish_stage(tmp_path, "storage", "part1_validation.json")
    panel = stage_dir(tmp_path, "quality_panel")
    panel.mkdir()
    (panel / "part2_validation.json").write_text('{"success":true}')
    result = publish_stage(tmp_path, "quality_panel", "part2_validation.json")
    assert result["parent_verification_sha256"] == resolve_stage(tmp_path, "acquisition")["verification_sha256"]
    assert resolve_stage(tmp_path, "quality_panel")["status"] == "PASSED"
    (acquisition / "acquisition_gate.json").write_text('{"success":false}')
    with pytest.raises(Exception, match="hash mismatch"):
        resolve_stage(tmp_path, "quality_panel")

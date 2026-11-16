"""Part 1 handoff tests; synthetic metadata never represents FDIC measurements."""
import json
from pathlib import Path

import pytest

from fdic_pipeline import workflow
from fdic_pipeline.acquisition import (
    DEFAULT_START, DEFAULT_END, FINANCIAL_FIELDS, FAILURE_FIELDS,
    INSTITUTION_FIELDS, quarter_ends,
)


def make_contract(tmp_path):
    source = tmp_path / "fdic_1984Q1_2026Q2"
    source.mkdir()
    quarters = quarter_ends(DEFAULT_START, DEFAULT_END)
    meta = {
        "spec": {"start": DEFAULT_START.isoformat(), "end": DEFAULT_END.isoformat(),
                 "financials_fields": list(FINANCIAL_FIELDS),
                 "failures_fields": list(FAILURE_FIELDS),
                 "institutions_fields": list(INSTITUTION_FIELDS)},
        "financials": {q: {"rows": 1, "api_total": 1, "complete": True} for q in quarters},
    }
    report = {
        "success": True, "scope": {"start": quarters[0], "end": quarters[-1]},
        "expected_quarters": 170, "verified_quarters": 170,
        "missing_quarters": [], "invalid_artifacts": [],
        "financial_rows": 170, "distinct_financial_certs": 15,
        "tables": {"failures": {"rows": 5, "missing_cert_rows": 2},
                   "institutions": {"rows": 20}},
    }
    (source / "manifest.json").write_text(json.dumps(meta))
    (source / "verification_report.json").write_text(json.dumps(report))
    return source, report, meta


def test_acquisition_contract_uses_measured_rows(tmp_path):
    src, _, _ = make_contract(tmp_path)
    result = workflow.acquisition_contract(src)
    assert result["financial_rows"] == 170
    assert result["failures_missing_cert_rows"] == 2
    assert len(result["manifest_sha256"]) == 64


def test_contract_rejects_missing_quarter(tmp_path):
    src, report, _ = make_contract(tmp_path)
    report["verified_quarters"] = 169
    (src / "verification_report.json").write_text(json.dumps(report))
    with pytest.raises(workflow.WorkflowError, match="Incomplete quarter"):
        workflow.acquisition_contract(src)


def test_contract_rejects_inconsistent_rows(tmp_path):
    src, report, _ = make_contract(tmp_path)
    report["financial_rows"] = 169
    (src / "verification_report.json").write_text(json.dumps(report))
    with pytest.raises(workflow.WorkflowError, match="row counts"):
        workflow.acquisition_contract(src)


def test_contract_rejects_changed_field_selection(tmp_path):
    src, _, meta = make_contract(tmp_path)
    meta["spec"]["financials_fields"] = ["CERT"]
    (src / "manifest.json").write_text(json.dumps(meta))
    with pytest.raises(workflow.WorkflowError, match="field selection"):
        workflow.acquisition_contract(src)


def test_contract_rejects_missing_failure_audit(tmp_path):
    src, report, _ = make_contract(tmp_path)
    del report["tables"]["failures"]["missing_cert_rows"]
    (src / "verification_report.json").write_text(json.dumps(report))
    with pytest.raises(workflow.WorkflowError, match="missing-CERT"):
        workflow.acquisition_contract(src)


def test_mismatched_run_ids_rejected(tmp_path):
    src, _, _ = make_contract(tmp_path)
    bench = tmp_path / "storage_benchmark" / "run_001"
    analysis = tmp_path / "storage_analysis" / "run_002"
    bench.mkdir(parents=True)
    analysis.mkdir(parents=True)
    with pytest.raises(workflow.WorkflowError, match="run ID"):
        workflow.evidence_contract(src, bench, analysis)


def make_evidence(tmp_path):
    """Construct a consistent synthetic handoff (not measured FDIC data)."""
    import hashlib
    from fdic_pipeline.storage_analysis import INPUTS

    src, _, _ = make_contract(tmp_path)
    contract = workflow.acquisition_contract(src)
    bench = tmp_path / "storage_benchmark" / "run_20990101T000000Z"
    analysis = tmp_path / "storage_analysis" / bench.name
    bench.mkdir(parents=True)
    analysis.mkdir(parents=True)
    prep = {
        "financial_rows": contract["financial_rows"], "num_quarters": 170,
        "round_trip_equality": True,
        "acquisition_manifest_sha256": contract["manifest_sha256"],
        "acquisition_report_sha256": contract["verification_report_sha256"],
    }
    verification = {"success": True, "round_trip_equality": True, "trial_count": 36}
    (bench / "preparation.json").write_text(json.dumps(prep))
    (bench / "benchmark_verification.json").write_text(json.dumps(verification))
    (bench / "environment.json").write_text(json.dumps({"git_commit_sha": "a" * 40}))
    for filename in INPUTS:
        if not (bench / filename).exists():
            (bench / filename).write_bytes((filename + " synthetic").encode())
    hashes = {name: workflow.sha256(bench / name) for name in INPUTS}
    (analysis / "source_file_hashes.json").write_text(json.dumps(hashes))
    outcome = {
        "success": True, "benchmark_run": bench.name,
        "source_rows": contract["financial_rows"], "quarter_count": 170,
        "total_trials_verified": 36,
        "acquisition_manifest_sha256": contract["manifest_sha256"],
        "acquisition_report_sha256": contract["verification_report_sha256"],
    }
    (analysis / "analysis_validation.json").write_text(json.dumps(outcome))
    for name in ("report_storage_table.csv", "report_performance_table.csv",
                 "storage_summary.json"):
        (analysis / name).write_text("synthetic evidence")
    (analysis / "figures").mkdir()
    for name in ("storage_sizes.png", "read_time_medians.png", "peak_rss_medians.png"):
        (analysis / "figures" / name).write_bytes((bench / name).read_bytes())
    return src, bench, analysis


def test_end_to_end_evidence_contract(tmp_path):
    src, bench, analysis = make_evidence(tmp_path)
    result = workflow.evidence_contract(src, bench, analysis)
    assert result["success"] is True
    assert result["measured_trials"] == 36


def test_reject_modified_benchmark_after_analysis(tmp_path):
    src, bench, analysis = make_evidence(tmp_path)
    with (bench / "comparisons.csv").open("ab") as f:
        f.write(b"altered")
    with pytest.raises(workflow.WorkflowError, match="differs"):
        workflow.evidence_contract(src, bench, analysis)


def test_reject_modified_acquisition_after_benchmark(tmp_path):
    src, bench, analysis = make_evidence(tmp_path)
    manifest = src / "manifest.json"
    manifest.write_text(manifest.read_text() + "\n")
    with pytest.raises(workflow.WorkflowError, match="changed"):
        workflow.evidence_contract(src, bench, analysis)

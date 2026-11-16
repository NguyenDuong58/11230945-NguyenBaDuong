"""Part 1 publication gate checks source lineage and trial accounting."""
from __future__ import annotations
import json
import pytest
from fdic_pipeline import project_flow as pf
from fdic_pipeline import part1_handoff as h


def test_final_handoff_checks_all_evidence(monkeypatch, tmp_path):
    pf.initialize(tmp_path, "a" * 40)
    source = pf.stage_dir(tmp_path, "acquisition")
    source.mkdir(parents=True, exist_ok=True)
    (source / "acquisition_gate.json").write_text(json.dumps({
        "success": True, "source_manifest_sha256": "f" * 64,
        "source_verification_sha256": "e" * 64,
    }))
    pf.publish_stage(tmp_path, "acquisition", "acquisition_gate.json")
    output = tmp_path / "example"
    for place, name in (("benchmark", "benchmark_verification.json"),
                        ("analysis", "analysis_validation.json"),
                        ("layout", "layout_validation.json")):
        folder = output / place / "run_test"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / name).write_text('{"success":true}')
    original = {
        "manifest_sha256": "f" * 64, "verification_report_sha256": "e" * 64,
        "financial_rows": 1200, "quarters": 170, "failures_rows": 3,
        "failures_missing_cert_rows": 1, "institutions_rows": 40,
    }
    monkeypatch.setattr(h, "acquisition_contract", lambda _: original)
    monkeypatch.setattr(h, "evidence_contract", lambda *args: {
        "success": True, "measured_trials": 36})
    monkeypatch.setattr(h, "validate_storage_run", lambda *args: {
        "verification": {"trial_count": 36}, "repetitions": 5, "warmups": 1})
    monkeypatch.setattr(h, "verify_layout", lambda *args: {
        "measured_trials": 30, "warmup_trials": 6,
        "partition_pruning_verified": True})
    verdict = h.validate_part1(tmp_path, tmp_path / "work",
             output / "benchmark" / "run_test", output / "analysis" / "run_test",
             output / "layout" / "run_test")
    assert verdict["success"] is True
    assert verdict["baseline_all_trials"] == 36
    assert verdict["baseline_measured_trials"] == 30
    assert verdict["baseline_warmup_trials"] == 6
    assert verdict["layout_measured_trials"] == 30
    assert verdict["pruning_verified"] is True


def test_reject_mismatched_run_identifiers(monkeypatch, tmp_path):
    with pytest.raises(pf.HandoffError):
        h.validate_part1(tmp_path, tmp_path, tmp_path / "a", tmp_path / "b", tmp_path / "c")

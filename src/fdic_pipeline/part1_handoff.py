"""Final Part 1 integrity gate and downstream dataset contract."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .acquisition import sha256
from .layout_benchmark import verify as verify_layout
from .project_flow import stage_dir, resolve_stage
from .storage_benchmark import BenchmarkError
from .storage_analysis import validate_run as validate_storage_run
from .workflow import acquisition_contract, evidence_contract


def validate_part1(root: Path, work: Path, benchmark: Path,
                   analysis: Path, layout: Path) -> dict[str, Any]:
    """Only publish source-linked, verified analysis and layout measurements."""
    active_source = resolve_stage(root, "acquisition")
    source = stage_dir(root, "acquisition")
    acquire = acquisition_contract(source)
    source_gate = active_source["validation"]
    if (source_gate.get("source_manifest_sha256") != acquire["manifest_sha256"]
        or source_gate.get("source_verification_sha256") != acquire["verification_report_sha256"]):
        raise BenchmarkError("Acquisition source fingerprint changed after publication")
    if benchmark.name != analysis.name or layout.name != benchmark.name:
        raise BenchmarkError("Storage benchmark, analysis and layout run IDs must be identical")
    baseline = evidence_contract(source, benchmark, analysis)
    # Validate the benchmark's own raw trials as well as analysis-derived figures.
    checked = validate_storage_run(benchmark)
    extension = verify_layout(source, work, layout)
    if checked["verification"].get("trial_count", 0) != baseline["measured_trials"]:
        raise BenchmarkError("Measured trial count differs between independent audits")
    return {
        "success": True,
        "experiment_id": active_source["path"].parent.name,
        "source_manifest_sha256": acquire["manifest_sha256"],
        "source_verification_sha256": acquire["verification_report_sha256"],
        "source_validation_sha256": active_source["verification_sha256"],
        "financial_rows": acquire["financial_rows"],
        "quarters": acquire["quarters"],
        "failures_rows": acquire["failures_rows"],
        "failures_missing_cert_rows": acquire["failures_missing_cert_rows"],
        "institutions_rows": acquire["institutions_rows"],
        "storage_run_id": benchmark.name,
        "benchmark_validation_sha256": sha256(benchmark / "benchmark_verification.json"),
        "analysis_validation_sha256": sha256(analysis / "analysis_validation.json"),
        "layout_validation_sha256": sha256(layout / "layout_validation.json"),
        "baseline_all_trials": baseline["measured_trials"],
        "baseline_measured_trials": checked["repetitions"] * 3 * 2,
        "baseline_warmup_trials": checked["warmups"] * 3 * 2,
        "layout_measured_trials": extension["measured_trials"],
        "layout_warmup_trials": extension["warmup_trials"],
        "pruning_verified": extension["partition_pruning_verified"],
        "output_locations": {
            "benchmark": str(benchmark), "analysis": str(analysis), "layout": str(layout),
        },
    }

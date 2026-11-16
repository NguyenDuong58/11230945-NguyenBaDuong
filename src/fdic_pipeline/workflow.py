"""Part 1 / Integrity contracts between acquisition, benchmark and analysis.

Each stage verifies the evidence it consumes. A benchmark is identified by an
explicit run ID; chronological directory order is never used to select a run.
"""
from __future__ import annotations

import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .acquisition import (
    DEFAULT_END, DEFAULT_START, FINANCIAL_FIELDS,
    FAILURE_FIELDS, INSTITUTION_FIELDS, quarter_ends, sha256,
)
from .storage_analysis import INPUTS, load_json


# 1. Dataset and analysis integrity contracts

class WorkflowError(ValueError):
    """A required artifact or stage dependency is inconsistent."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise WorkflowError(message)


# 2. Verified acquisition input contract

def acquisition_contract(source: Path) -> dict[str, Any]:
    """Check the manifest and final report without downloading any data.

    The expensive file-by-file hash, schema, and key checks are performed by
    acquisition verification and again by benchmark preparation.
    """
    source = Path(source)
    require(source.is_dir(), f"Acquisition directory is absent: {source}")
    report_path = source / "verification_report.json"
    manifest_path = source / "manifest.json"
    require(report_path.is_file(), "Acquisition verification report is absent")
    require(manifest_path.is_file(), "Acquisition manifest is absent")
    report = load_json(report_path)
    manifest = load_json(manifest_path)
    quarters = quarter_ends(DEFAULT_START, DEFAULT_END)
    require(report.get("success") is True, "Acquisition verification did not pass")
    require(report.get("scope") == {"start": quarters[0], "end": quarters[-1]},
            "Acquisition period differs from the examination window")
    require(report.get("expected_quarters") == 170 and
            report.get("verified_quarters") == 170 and
            report.get("missing_quarters") == [] and
            report.get("invalid_artifacts") == [], "Incomplete quarter verification")
    spec = manifest.get("spec", {})
    require(spec.get("start") == DEFAULT_START.isoformat() and
            spec.get("end") == DEFAULT_END.isoformat(), "Manifest scope differs")
    for field, expected in (("financials_fields", FINANCIAL_FIELDS),
                            ("failures_fields", FAILURE_FIELDS),
                            ("institutions_fields", INSTITUTION_FIELDS)):
        require(spec.get(field) == list(expected), f"Unexpected field selection: {field}")
    entries = manifest.get("financials", {})
    require(isinstance(entries, dict) and set(entries) == set(quarters),
            "Manifest does not contain exactly the required quarters")
    require(all(isinstance(entries[q], dict) and entries[q].get("complete") is True and
                isinstance(entries[q].get("rows"), int) and entries[q]["rows"] > 0 and
                entries[q].get("api_total") == entries[q]["rows"]
                for q in quarters), "Incomplete or inconsistent financials entry")
    total = sum(entries[q]["rows"] for q in quarters)
    require(isinstance(report.get("financial_rows"), int) and
            report["financial_rows"] == total, "Acquisition row counts disagree")
    tables = report.get("tables", {})
    require(all(isinstance(tables.get(k), dict) and
                isinstance(tables[k].get("rows"), int) and tables[k]["rows"] > 0
                for k in ("failures", "institutions")),
            "Failure or institution reference table is absent")
    require(isinstance(report.get("distinct_financial_certs"), int) and
            report["distinct_financial_certs"] > 0,
            "Invalid distinct-bank count")
    missing_cert = tables["failures"].get("missing_cert_rows")
    require(isinstance(missing_cert, int) and
            0 <= missing_cert <= tables["failures"]["rows"],
            "Invalid missing-CERT count in Failures")
    return {
        "scope": report["scope"],
        "quarters": len(quarters),
        "financial_rows": total,
        "financial_fields": len(FINANCIAL_FIELDS),
        "distinct_financial_certs": report["distinct_financial_certs"],
        "failures_rows": tables["failures"]["rows"],
        "failures_missing_cert_rows": missing_cert,
        "institutions_rows": tables["institutions"]["rows"],
        "manifest_sha256": sha256(manifest_path),
        "verification_report_sha256": sha256(report_path),
    }


# 3. Acquisition-to-report lineage contract

def evidence_contract(source: Path, benchmark: Path, analysis: Path) -> dict[str, Any]:
    """Prove that one analysis package corresponds to one measured FDIC dataset."""
    a = acquisition_contract(source)
    benchmark = Path(benchmark)
    analysis = Path(analysis)
    require(benchmark.is_dir() and analysis.is_dir(), "Results directories are missing")
    require(benchmark.name == analysis.name and benchmark.name.startswith("run_"),
            "Benchmark and analysis must share the same explicit run ID")
    prep = load_json(benchmark / "preparation.json")
    checked = load_json(benchmark / "benchmark_verification.json")
    verdict = load_json(analysis / "analysis_validation.json")
    source_hashes = load_json(analysis / "source_file_hashes.json")
    require(prep.get("financial_rows") == a["financial_rows"] and
            prep.get("num_quarters") == a["quarters"],
            "Prepared input differs from verified acquisition")
    require(prep.get("acquisition_manifest_sha256") == a["manifest_sha256"] and
            prep.get("acquisition_report_sha256") == a["verification_report_sha256"],
            "Acquisition files changed after benchmark preparation")
    require(prep.get("round_trip_equality") is True and
            checked.get("success") is True and
            checked.get("round_trip_equality") is True,
            "Benchmark verification has not passed")
    require(verdict.get("success") is True and
            verdict.get("benchmark_run") == benchmark.name and
            verdict.get("source_rows") == a["financial_rows"] and
            verdict.get("quarter_count") == a["quarters"] and
            verdict.get("total_trials_verified") == checked.get("trial_count"),
            "Analysis audit does not match the benchmark")
    require(verdict.get("acquisition_manifest_sha256") == a["manifest_sha256"] and
            verdict.get("acquisition_report_sha256") == a["verification_report_sha256"],
            "Analysis lineage does not match the acquisition")
    require(set(source_hashes) == set(INPUTS), "Incomplete analysis source hash list")
    for name in INPUTS:
        p = benchmark / name
        require(p.is_file() and sha256(p) == source_hashes[name],
                f"Benchmark evidence differs from analysis source: {name}")
    for name in ("report_storage_table.csv", "report_performance_table.csv",
                 "storage_summary.json", "analysis_validation.json"):
        p = analysis / name
        require(p.is_file() and p.stat().st_size > 0, f"Missing report evidence: {name}")
    for name in ("storage_sizes.png", "read_time_medians.png", "peak_rss_medians.png"):
        p = analysis / "figures" / name
        require(p.is_file() and sha256(p) == source_hashes[name],
                f"Analysis figure differs from original benchmark: {name}")
    return {
        "success": True,
        "run_id": benchmark.name,
        "acquisition": a,
        "measured_trials": checked["trial_count"],
        "report_files": 8,
        "benchmark_environment": load_json(benchmark / "environment.json"),
    }

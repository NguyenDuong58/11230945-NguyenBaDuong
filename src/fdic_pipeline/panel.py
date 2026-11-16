"""FDIC data-quality audit, clean bank-quarter panel and immutable evidence.

Financial predictors contain source reporting-period values and causal history
only. Historical failures and institution cessation evidence are audited in
separate files and do not appear in the predictor panel.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .acquisition import (
    DEFAULT_END, DEFAULT_START, FINANCIAL_FIELDS, NUMERIC_FIELDS,
    arrow_schema, financial_path, quarter_ends, sha256, table_path,
)
from .data_quality import (
    QUALITY_RULES, TERMINAL_CLASSES, ILLUSTRATIVE_PERIODS,
    assumed_available_date, asset_exclusion_reason, classify_terminal_event,
    count_null_numerics, omitted_quarters, parse_source_date, screening_flags,
)

LOG = logging.getLogger("fdic_quality_panel")
NUMERIC_FIELDS_FIN = tuple(f for f in FINANCIAL_FIELDS if f in NUMERIC_FIELDS["financials"])
AUDITED_SOURCE_FIELDS = tuple(FINANCIAL_FIELDS)
PANEL_FIELDS = (
    "MISSING_FINANCIAL_COUNT", "PRIOR_REPORT_COUNT", "REPORT_GAP_QUARTERS",
    "ASSUMED_AVAILABLE_FROM", "DQ_FLAG_COUNT", *QUALITY_RULES,
)
EXCLUDED_PREDICTOR_FIELDS = frozenset({
    "FAILDATE", "FAILYR", "COST", "ACTIVE", "CHANGEC1", "ENDEFYMD",
    "EVENT_DATE", "TERMINAL_CLASS", "LABEL", "TARGET", "Y",
})
AUDIT_FILES = (
    "field_missingness.csv", "field_year_missingness.csv",
    "quarterly_coverage.csv", "quality_rules.csv",
    "quarantined_rows.csv", "reporting_gaps.csv", "anomaly_examples.csv",
    "failure_linkage.csv", "terminal_exits.csv", "failure_counts_by_year.csv",
    "failure_counts_by_period.csv", "availability_scenarios.csv",
    "quality_findings.md",
)
FIGURES = (
    "figures/field_missingness.png", "figures/annual_coverage.png",
    "figures/terminal_classes.png", "figures/rule_counts.png",
)
MAX_ANOMALY_EXAMPLES_PER_RULE = 12
AVAILABILITY_SCENARIOS = (30, 90, 180)


class PanelError(RuntimeError):
    """Invalid source, panel invariant or stage-to-stage provenance."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_read(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PanelError(f"Missing or invalid JSON file: {path}") from exc
    if not isinstance(data, dict):
        raise PanelError(f"Expected a JSON object: {path}")
    return data


def _json_write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(dict(value), stream, indent=2, sort_keys=True, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _csv_write(path: Path, fields: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def panel_schema() -> Any:
    """Stable Arrow schema for downstream pandas, Polars and Spark scans."""
    import pyarrow as pa
    extras = [
        pa.field("MISSING_FINANCIAL_COUNT", pa.int16(), nullable=False),
        pa.field("PRIOR_REPORT_COUNT", pa.int32(), nullable=False),
        pa.field("REPORT_GAP_QUARTERS", pa.int16(), nullable=True),
        pa.field("ASSUMED_AVAILABLE_FROM", pa.date32(), nullable=False),
        pa.field("DQ_FLAG_COUNT", pa.int16(), nullable=False),
    ]
    extras.extend(pa.field(name, pa.bool_(), nullable=False) for name in QUALITY_RULES)
    schema = pa.schema(list(arrow_schema("financials")) + extras)
    if EXCLUDED_PREDICTOR_FIELDS.intersection(schema.names):
        raise PanelError("Retrospective outcome field appears in panel schema")
    return schema


def _validate_input_file(path: Path, endpoint: str, entry: Mapping[str, Any],
                         quarter: str | None = None) -> Any:
    """Verify hash, schema, row count and calendar before returning Arrow data."""
    import pyarrow.parquet as pq
    if not path.is_file() or sha256(path) != entry.get("sha256"):
        raise PanelError(f"Source checksum differs from acquisition manifest: {path}")
    parquet = pq.ParquetFile(path)
    if parquet.schema_arrow != arrow_schema(endpoint):
        raise PanelError(f"Unexpected {endpoint} source schema: {path}")
    if parquet.metadata.num_rows != entry.get("rows"):
        raise PanelError(f"Source record-count mismatch: {path}")
    table = parquet.read()
    if table.num_rows != parquet.metadata.num_rows:
        raise PanelError(f"Incomplete source read: {path}")
    if quarter is not None:
        expected = datetime.strptime(quarter, "%Y%m%d").date()
        if set(table.column("REPDTE").to_pylist()) != {expected}:
            raise PanelError(f"Source file contains unexpected reporting dates: {path}")
    return table


def _load_events(source: Path, manifest: Mapping[str, Any]) -> tuple[
        dict[int, list[date]], set[int], dict[int, date | None],
        set[int], dict[int, dict[str, Any]], list[dict[str, Any]],
        list[dict[str, Any]], int]:
    """Preserve all failure rows, including missing CERT and malformed dates."""
    failures = _validate_input_file(table_path(source, "failures"), "failures",
                                    manifest["tables"]["failures"]).to_pylist()
    institutions = _validate_input_file(table_path(source, "institutions"), "institutions",
                                        manifest["tables"]["institutions"]).to_pylist()
    failure_dates: dict[int, list[date]] = defaultdict(list)
    uncertain_failure_certs: set[int] = set()
    end_dates: dict[int, date | None] = {}
    invalid_end_dates: set[int] = set()
    institution_info: dict[int, dict[str, Any]] = {}
    linkage: list[dict[str, Any]] = []
    normalized_events: list[dict[str, Any]] = []
    for index, record in enumerate(failures):
        cert = record["CERT"]
        date_raw = record.get("FAILDATE")
        parsed = None
        date_status = "valid"
        try:
            parsed = parse_source_date(date_raw)
            if parsed is None:
                date_status = "missing_date"
        except ValueError:
            date_status = "unparseable_date"
        if cert is not None:
            cert = int(cert)
            if date_status != "valid":
                uncertain_failure_certs.add(cert)
            else:
                failure_dates[cert].append(parsed)
        status = "missing_certificate" if cert is None else date_status
        linkage.append({
            "SOURCE_ROW": index, "CERT": cert, "FAILDATE_RAW": date_raw,
            "FAILDATE_PARSED": parsed.isoformat() if parsed else "",
            "CERT_STATUS": status, "DATE_STATUS": date_status,
            "FAILYR_RAW": record.get("FAILYR"),
        })
        normalized_events.append({"CERT": cert, "date": parsed, "row": index,
                                  "date_status": date_status})
    for record in institutions:
        cert = int(record["CERT"])
        if cert in institution_info:
            raise PanelError(f"Duplicate Institution CERT: {cert}")
        institution_info[cert] = {
            "ACTIVE": record.get("ACTIVE"),
            "CHANGEC1": record.get("CHANGEC1"),
            "ENDEFYMD": record.get("ENDEFYMD"),
        }
        try:
            end_dates[cert] = parse_source_date(record.get("ENDEFYMD"))
        except ValueError:
            end_dates[cert] = None
            invalid_end_dates.add(cert)
    return (failure_dates, uncertain_failure_certs, end_dates, invalid_end_dates,
            institution_info, linkage, normalized_events, len(institutions))


def _event_counts(events: list[dict[str, Any]], known_certs: set[int]) -> tuple[
        list[dict[str, Any]], list[dict[str, Any]]]:
    """Count actual dated failures separately from positive four-quarter labels."""
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        if row["date"] is not None:
            by_year[row["date"].year].append(row)

    def calculate(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
        certs = {r["CERT"] for r in rows if r["CERT"] is not None}
        linked = certs & known_certs
        return {
            "DATED_FAILURE_SOURCE_ROWS": len(rows),
            "DISTINCT_DATED_CERT": len(certs),
            "DISTINCT_DATED_CERT_IN_FINANCIALS": len(linked),
            "DATED_ROWS_MISSING_CERT": sum(r["CERT"] is None for r in rows),
            "DISTINCT_CERT_DATE_PAIRS": len({(r["CERT"], r["date"]) for r in rows if r["CERT"] is not None}),
        }
    years = [{"YEAR": year, **calculate(rows)} for year, rows in sorted(by_year.items())]
    periods = []
    for label, start, end in ILLUSTRATIVE_PERIODS:
        subset = [r for r in events if r["date"] is not None and start <= r["date"] <= end]
        periods.append({"PERIOD": label, "START": start.isoformat(), "END": end.isoformat(),
                        **calculate(subset)})
    return years, periods


def _figures(out: Path, missing_fields: list[dict[str, Any]],
             coverage: list[dict[str, Any]], flags: Counter[str],
             terminals: Counter[str]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    pictures = out / "figures"
    pictures.mkdir(parents=True, exist_ok=True)
    top = sorted(missing_fields, key=lambda r: (-r["MISSING_PCT"], r["FIELD"]))[:15]
    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.barh([r["FIELD"] for r in reversed(top)], [r["MISSING_PCT"] for r in reversed(top)])
    ax.set_xlabel("Missing observations (%)")
    ax.set_title("Financials: fields with highest missingness")
    fig.tight_layout()
    fig.savefig(pictures / "field_missingness.png", dpi=160)
    plt.close(fig)
    annual = defaultdict(lambda: {"raw": 0, "clean": 0})
    for row in coverage:
        year = int(str(row["QUARTER"])[:4])
        annual[year]["raw"] += int(row["RAW_ROWS"])
        annual[year]["clean"] += int(row["PANEL_ROWS"])
    years = sorted(annual)
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(years, [annual[y]["raw"] for y in years], label="Raw Financials")
    ax.plot(years, [annual[y]["clean"] for y in years], label="Panel", linestyle="--")
    ax.set_xlabel("Reporting year")
    ax.set_ylabel("Bank-quarter observations")
    ax.set_title("Annual reporting coverage")
    ax.legend()
    fig.tight_layout()
    fig.savefig(pictures / "annual_coverage.png", dpi=160)
    plt.close(fig)
    order = sorted(TERMINAL_CLASSES, key=lambda name: -terminals[name])
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(list(reversed(order)), [terminals[name] for name in reversed(order)])
    ax.set_xlabel("Distinct financial institution certificates")
    ax.set_title("Retrospective terminal-evidence classification")
    fig.tight_layout()
    fig.savefig(pictures / "terminal_classes.png", dpi=160)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 4.8))
    order = sorted(QUALITY_RULES, key=lambda name: -flags[name])
    ax.barh(list(reversed(order)), [flags[name] for name in reversed(order)])
    ax.set_xlabel("Flagged bank-quarter observations")
    ax.set_title("Current-quarter screening rules")
    fig.tight_layout()
    fig.savefig(pictures / "rule_counts.png", dpi=160)
    plt.close(fig)


def _findings_md(*, raw_rows: int, panel_rows: int, quarantined: int,
                 gaps: int, missing_fields: list[dict[str, Any]],
                 terminals: Counter[str], source: Mapping[str, Any],
                 lag_days: int, storage_digest: str) -> str:
    top = sorted(missing_fields, key=lambda x: -x["MISSING_PCT"])[:6]
    lines = [
        "# Automated Data Quality Findings — Part 2", "",
        "## Source and accounting", "",
        f"- Financials source observations: {raw_rows:,}.",
        f"- Clean panel observations: {panel_rows:,}.",
        f"- Excluded observations with invalid core assets: {quarantined:,}.",
        f"- Source reconciliation: {panel_rows:,} + {quarantined:,} = {raw_rows:,}.",
        f"- Reporting intervals with one or more missing intermediate quarters: {gaps:,}.",
        f"- Acquisition manifest SHA-256: `{source['manifest_sha256']}`.",
        f"- Storage-stage validation SHA-256: `{storage_digest}`.", "",
        "## Field missingness", "",
        "| Field | Missing (%) |", "|---|---:|",
    ]
    lines.extend(f"| {r['FIELD']} | {r['MISSING_PCT']:.2f} |" for r in top)
    lines.extend(["", "## Terminal evidence, not predictor columns", "",
                  "| Class | Distinct CERT |", "|---|---:|"])
    lines.extend(f"| {name} | {terminals[name]} |" for name in TERMINAL_CLASSES)
    lines.extend(["", "## Information availability and limitations", "",
        f"- `ASSUMED_AVAILABLE_FROM = REPDTE + {lag_days} calendar days` is an unverified release-lag assumption.",
        "- No actual historical publication timestamps are supplied by these source tables.",
        "- No source values were imputed, winsorised or filled from a later quarter.",
        "- Financial anomalies are screening flags, not automatic grounds for deleting genuine stress.",
        "- Failure evidence and institution end/status information are stored only in audit files.",
        "- A disappearance from Financials is not itself proof of failure or merger.",
        "- Source-level failure counts by event date are not predictive label counts.",
        "- The release-lag and terminal-event limitations must be carried into the label policy in Part 4.",
        ""])
    return "\n".join(lines)


def build_panel(source: Path, output: Path, *, source_summary: Mapping[str, Any],
                storage_verification_sha256: str, code_commit: str,
                availability_lag_days: int = 90,
                expected_quarters: Sequence[str] | None = None) -> dict[str, Any]:
    """Construct a complete source-verified audit and quarter-partitioned panel.

    Production calls pass the complete 170-quarter calendar and acquisition
    contract. A shorter explicit calendar is allowed only by synthetic tests.
    This function will not write into an existing output directory.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    source, output = Path(source), Path(output)
    if not source.is_dir():
        raise PanelError(f"Missing acquisition directory: {source}")
    if output.exists():
        raise PanelError(f"Refusing to overwrite an existing quality-panel directory: {output}")
    if not 0 <= availability_lag_days <= 366:
        raise PanelError("Invalid availability lag")
    if len(code_commit) != 40 or any(c not in "0123456789abcdef" for c in code_commit):
        raise PanelError("A complete source Git commit is required")
    if len(storage_verification_sha256) != 64:
        raise PanelError("A storage-stage validation SHA-256 is required")
    quarters = tuple(expected_quarters or quarter_ends(DEFAULT_START, DEFAULT_END))
    if len(set(quarters)) != len(quarters) or list(quarters) != sorted(quarters):
        raise PanelError("Quarter calendar is not ordered or contains duplicates")
    source_manifest_path = source / "manifest.json"
    if sha256(source_manifest_path) != source_summary["manifest_sha256"]:
        raise PanelError("Acquisition source manifest differs from stage contract")
    source_manifest = _json_read(source_manifest_path)
    if set(source_manifest.get("financials", {})) != set(quarters):
        raise PanelError("Acquisition manifest and requested quarter calendar differ")
    (failure_dates, uncertain_failures, end_dates, invalid_end_dates,
     institutions, linkage, events, institution_rows) = _load_events(source, source_manifest)
    if len(linkage) != source_summary["failures_rows"] or institution_rows != source_summary["institutions_rows"]:
        raise PanelError("Reference-table counts differ from acquisition contract")
    missing_certificate = sum(e["CERT"] is None for e in linkage)
    if missing_certificate != source_summary["failures_missing_cert_rows"]:
        raise PanelError("Missing failure certificates differ from source audit")
    output.mkdir(parents=True, exist_ok=False)
    (output / "panel").mkdir()
    (output / "audits").mkdir()
    histories: dict[int, dict[str, Any]] = {}
    seen_keys: set[tuple[int, date]] = set()
    missing_all: Counter[str] = Counter()
    year_missing: Counter[tuple[int, str]] = Counter()
    year_rows: Counter[int] = Counter()
    rule_totals: Counter[str] = Counter()
    rule_certs: dict[str, set[int]] = defaultdict(set)
    anomaly_counters: Counter[str] = Counter()
    anomaly_examples: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    partitions: list[dict[str, Any]] = []
    raw_total = 0
    panel_total = 0
    for seq, q in enumerate(quarters, 1):
        raw = _validate_input_file(financial_path(source, q), "financials",
                                   source_manifest["financials"][q], q)
        records = raw.to_pylist()
        raw_total += len(records)
        year = int(q[:4])
        year_rows[year] += len(records)
        retained: list[dict[str, Any]] = []
        for record in records:
            cert = record["CERT"]
            repdte = record["REPDTE"]
            if cert is None or repdte is None:
                raise PanelError("Source financial key cannot be null")
            cert = int(cert)
            key = (cert, repdte)
            if key in seen_keys:
                raise PanelError(f"Duplicate Financials (CERT, REPDTE) across dataset: {key}")
            seen_keys.add(key)
            for field in AUDITED_SOURCE_FIELDS:
                value = record.get(field)
                is_missing = (value is None or (isinstance(value, str) and not value.strip())
                              or (field in NUMERIC_FIELDS_FIN and not (
                                  isinstance(value, (int, float)) and math.isfinite(float(value)))))
                if is_missing:
                    missing_all[field] += 1
                    year_missing[(year, field)] += 1
            state = histories.get(cert)
            previous_report = state["last"] if state is not None else None
            if state is None:
                gap, prior = None, 0
                histories[cert] = {"first": repdte, "last": repdte, "reports": 1,
                                   "clean_reports": 0, "gap_periods": 0}
            else:
                gap = omitted_quarters(state["last"], repdte)
                prior = state["reports"]
                state["last"] = repdte
                state["reports"] += 1
                if gap > 0:
                    state["gap_periods"] += gap
                    gaps.append({"CERT": cert, "PREVIOUS_REPDTE": previous_report.isoformat(),
                                 "REPDTE": repdte.isoformat(), "MISSING_QUARTERS": gap})
            reason = asset_exclusion_reason(record.get("ASSET"))
            if reason is not None:
                quarantined.append({"CERT": cert, "REPDTE": repdte.isoformat(),
                                    "ASSET": record.get("ASSET"), "REASON": reason})
                continue
            flags = screening_flags(record)
            for name, active in flags.items():
                if not active:
                    continue
                rule_totals[name] += 1
                rule_certs[name].add(cert)
                if anomaly_counters[name] < MAX_ANOMALY_EXAMPLES_PER_RULE:
                    anomaly_examples.append({
                        "RULE": name, "CERT": cert, "REPDTE": repdte.isoformat(),
                        "ASSET": record.get("ASSET"), "DEP": record.get("DEP"),
                        "LNLSGR": record.get("LNLSGR"), "ROA": record.get("ROA"),
                        "ROE": record.get("ROE"),
                    })
                    anomaly_counters[name] += 1
            clean_row = dict(record)
            clean_row.update({
                "MISSING_FINANCIAL_COUNT": count_null_numerics(record, NUMERIC_FIELDS_FIN),
                "PRIOR_REPORT_COUNT": prior,
                "REPORT_GAP_QUARTERS": gap,
                "ASSUMED_AVAILABLE_FROM": assumed_available_date(repdte, availability_lag_days),
                "DQ_FLAG_COUNT": sum(flags.values()),
                **flags,
            })
            retained.append(clean_row)
            histories[cert]["clean_reports"] += 1
        table = pa.Table.from_pylist(retained, schema=panel_schema())
        q_date = datetime.strptime(q, "%Y%m%d").date()
        path = (output / "panel" / f"year={q_date.year}" /
                f"quarter={(q_date.month - 1) // 3 + 1}" / "part-00000.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path, compression="snappy", use_dictionary=True)
        if pq.ParquetFile(path).schema_arrow != panel_schema():
            raise PanelError("Output schema mismatch after Parquet write")
        panel_total += table.num_rows
        partitions.append({
            "quarter": q, "file": str(path.relative_to(output)),
            "rows": table.num_rows, "sha256": sha256(path),
            "size_bytes": path.stat().st_size,
        })
        coverage.append({"QUARTER": q, "RAW_ROWS": len(records),
                         "PANEL_ROWS": table.num_rows,
                         "QUARANTINED_ROWS": len(records) - table.num_rows,
                         "DISTINCT_CERT": len({r["CERT"] for r in records})})
        LOG.info("Quarter %s/%s %s: %s source, %s panel rows", seq, len(quarters),
                 q, len(records), table.num_rows)
        if seq == len(quarters):
            LOG.info("Completed %s quarterly Financials partitions", len(quarters))

    if raw_total != source_summary["financial_rows"]:
        raise PanelError("Raw Financials observation count differs from Part 1")
    if panel_total + len(quarantined) != raw_total:
        raise PanelError("Clean and quarantined row accounting does not reconcile")
    financial_certs = set(histories)
    if len(financial_certs) != source_summary["distinct_financial_certs"]:
        raise PanelError("Distinct Financials CERT count differs from Part 1")
    for event in linkage:
        cert = event["CERT"]
        event["PANEL_CERT_EXISTS"] = cert is not None and cert in financial_certs
        if event["CERT_STATUS"] != "missing_certificate" and cert not in financial_certs:
            event["CERT_STATUS"] = event["DATE_STATUS"] + "_unlinked"
    terminal_rows: list[dict[str, Any]] = []
    terminals: Counter[str] = Counter()
    for cert, state in sorted(histories.items()):
        category, event_date, explanation = classify_terminal_event(
            state["last"], DEFAULT_END, failure_dates.get(cert, []),
            end_dates.get(cert),
            unparseable_or_undated_failure=cert in uncertain_failures,
            unparseable_institution_end=cert in invalid_end_dates,
        )
        terminals[category] += 1
        info = institutions.get(cert, {})
        terminal_rows.append({
            "CERT": cert, "FIRST_REPDTE": state["first"].isoformat(),
            "LAST_REPDTE": state["last"].isoformat(),
            "SOURCE_REPORTS": state["reports"],
            "CLEAN_REPORTS": state["clean_reports"],
            "MISSING_INTERMEDIATE_QUARTERS": state["gap_periods"],
            "TERMINAL_CLASS": category,
            "TERMINAL_EVENT_DATE": event_date.isoformat() if event_date else "",
            "LINKED_FAILURE_DATES": ";".join(sorted({d.isoformat() for d in failure_dates.get(cert, [])})),
            "INSTITUTION_END_DATE": end_dates.get(cert).isoformat() if end_dates.get(cert) else "",
            "INSTITUTION_ACTIVE_RAW": info.get("ACTIVE", ""),
            "INSTITUTION_CHANGE_CODE_RAW": info.get("CHANGEC1", ""),
            "REASON": explanation,
        })
    if sum(terminals.values()) != len(financial_certs):
        raise PanelError("Terminal class coverage differs from Financials bank coverage")
    missing_rows = []
    for field in AUDITED_SOURCE_FIELDS:
        missing_rows.append({
            "FIELD": field, "TOTAL_ROWS": raw_total,
            "MISSING_ROWS": missing_all[field],
            "MISSING_PCT": round(100.0 * missing_all[field] / raw_total, 6),
        })
    field_by_year = [{
        "YEAR": year, "FIELD": field, "TOTAL_ROWS": year_rows[year],
        "MISSING_ROWS": year_missing[(year, field)],
        "MISSING_PCT": round(100.0 * year_missing[(year, field)] / year_rows[year], 6),
    } for year in sorted(year_rows) for field in AUDITED_SOURCE_FIELDS]
    rules = [{"RULE": name, "FLAGGED_ROWS": rule_totals[name],
              "DISTINCT_FLAGGED_CERT": len(rule_certs[name])} for name in QUALITY_RULES]
    years, periods = _event_counts(events, financial_certs)
    available = []
    for lag in AVAILABILITY_SCENARIOS:
        unavailable = sum(int(row["RAW_ROWS"]) for row in coverage
                          if assumed_available_date(datetime.strptime(str(row["QUARTER"]), "%Y%m%d").date(), lag) > DEFAULT_END)
        available.append({
            "ASSUMED_LAG_DAYS": lag, "SOURCE_REPORTS": raw_total,
            "NOT_AVAILABLE_BY_STUDY_END": unavailable,
            "ASSUMPTION_ONLY": True,
        })
    audit = output / "audits"
    _csv_write(audit / "field_missingness.csv",
               ("FIELD", "TOTAL_ROWS", "MISSING_ROWS", "MISSING_PCT"), missing_rows)
    _csv_write(audit / "field_year_missingness.csv",
               ("YEAR", "FIELD", "TOTAL_ROWS", "MISSING_ROWS", "MISSING_PCT"), field_by_year)
    _csv_write(audit / "quarterly_coverage.csv",
               ("QUARTER", "RAW_ROWS", "PANEL_ROWS", "QUARANTINED_ROWS", "DISTINCT_CERT"), coverage)
    _csv_write(audit / "quality_rules.csv",
               ("RULE", "FLAGGED_ROWS", "DISTINCT_FLAGGED_CERT"), rules)
    _csv_write(audit / "quarantined_rows.csv", ("CERT", "REPDTE", "ASSET", "REASON"), quarantined)
    _csv_write(audit / "reporting_gaps.csv",
               ("CERT", "PREVIOUS_REPDTE", "REPDTE", "MISSING_QUARTERS"), gaps)
    _csv_write(audit / "anomaly_examples.csv",
               ("RULE", "CERT", "REPDTE", "ASSET", "DEP", "LNLSGR", "ROA", "ROE"), anomaly_examples)
    _csv_write(audit / "failure_linkage.csv",
               ("SOURCE_ROW", "CERT", "FAILDATE_RAW", "FAILDATE_PARSED", "CERT_STATUS",
                "DATE_STATUS", "FAILYR_RAW", "PANEL_CERT_EXISTS"), linkage)
    _csv_write(audit / "terminal_exits.csv",
               ("CERT", "FIRST_REPDTE", "LAST_REPDTE", "SOURCE_REPORTS",
                "CLEAN_REPORTS", "MISSING_INTERMEDIATE_QUARTERS", "TERMINAL_CLASS",
                "TERMINAL_EVENT_DATE", "LINKED_FAILURE_DATES", "INSTITUTION_END_DATE",
                "INSTITUTION_ACTIVE_RAW", "INSTITUTION_CHANGE_CODE_RAW", "REASON"), terminal_rows)
    event_columns = ("DATED_FAILURE_SOURCE_ROWS", "DISTINCT_DATED_CERT",
                     "DISTINCT_DATED_CERT_IN_FINANCIALS", "DATED_ROWS_MISSING_CERT",
                     "DISTINCT_CERT_DATE_PAIRS")
    _csv_write(audit / "failure_counts_by_year.csv", ("YEAR", *event_columns), years)
    _csv_write(audit / "failure_counts_by_period.csv", ("PERIOD", "START", "END", *event_columns), periods)
    _csv_write(audit / "availability_scenarios.csv",
               ("ASSUMED_LAG_DAYS", "SOURCE_REPORTS", "NOT_AVAILABLE_BY_STUDY_END", "ASSUMPTION_ONLY"),
               available)
    (audit / "quality_findings.md").write_text(_findings_md(
        raw_rows=raw_total, panel_rows=panel_total, quarantined=len(quarantined),
        gaps=len(gaps), missing_fields=missing_rows, terminals=terminals,
        source=source_summary, lag_days=availability_lag_days,
        storage_digest=storage_verification_sha256), encoding="utf-8")
    _figures(output, missing_rows, coverage, rule_totals, terminals)
    all_evidence = {str(Path("audits") / name): sha256(audit / name) for name in AUDIT_FILES}
    all_evidence.update({name: sha256(output / name) for name in FIGURES})
    manifest_out = {
        "schema_version": 1, "built_utc": _now(),
        "code_commit": code_commit, "source_manifest_sha256": source_summary["manifest_sha256"],
        "source_verification_report_sha256": source_summary["verification_report_sha256"],
        "storage_verification_sha256": storage_verification_sha256,
        "scope": {"start": quarters[0], "end": quarters[-1]},
        "expected_quarters": len(quarters),
        "lag_assumption_days": availability_lag_days,
        "lag_is_verified_publication": False,
        "financial_fields": list(FINANCIAL_FIELDS),
        "panel_fields": panel_schema().names,
        "excluded_predictor_columns": sorted(EXCLUDED_PREDICTOR_FIELDS),
        "quality_rule_names": list(QUALITY_RULES),
        "raw_rows": raw_total, "panel_rows": panel_total,
        "quarantined_rows": len(quarantined),
        "source_certificates": len(financial_certs),
        "clean_certificates": sum(s["clean_reports"] > 0 for s in histories.values()),
        "failure_reference_rows": len(linkage),
        "failure_missing_cert_rows": missing_certificate,
        "institution_reference_rows": institution_rows,
        "reporting_gap_intervals": len(gaps),
        "terminal_class_counts": {name: terminals[name] for name in TERMINAL_CLASSES},
        "quality_rule_counts": {name: rule_totals[name] for name in QUALITY_RULES},
        "partitions": partitions,
        "audit_sha256": all_evidence,
    }
    _json_write(output / "part2_manifest.json", manifest_out)
    verdict = verify_panel(source, output, source_summary=source_summary,
                           storage_verification_sha256=storage_verification_sha256,
                           expected_quarters=quarters, compare_existing=False)
    _json_write(output / "part2_validation.json", verdict)
    return verdict


def verify_panel(source: Path, output: Path, *, source_summary: Mapping[str, Any],
                 storage_verification_sha256: str,
                 expected_quarters: Sequence[str] | None = None,
                 compare_existing: bool = True) -> dict[str, Any]:
    """Read-only verification of all output bytes and upstream provenance."""
    import pyarrow.parquet as pq
    source, output = Path(source), Path(output)
    manifest_path = output / "part2_manifest.json"
    m = _json_read(manifest_path)
    quarters = tuple(expected_quarters or quarter_ends(DEFAULT_START, DEFAULT_END))
    if (m.get("source_manifest_sha256") != source_summary["manifest_sha256"] or
        m.get("source_verification_report_sha256") != source_summary["verification_report_sha256"] or
        m.get("storage_verification_sha256") != storage_verification_sha256 or
        m.get("raw_rows") != source_summary["financial_rows"] or
        m.get("source_certificates") != source_summary["distinct_financial_certs"] or
        m.get("failure_reference_rows") != source_summary["failures_rows"] or
        m.get("failure_missing_cert_rows") != source_summary["failures_missing_cert_rows"] or
        m.get("institution_reference_rows") != source_summary["institutions_rows"] or
        m.get("expected_quarters") != len(quarters) or
        m.get("scope") != {"start": quarters[0], "end": quarters[-1]} or
        m.get("lag_is_verified_publication") is not False):
        raise PanelError("Quality-panel manifest differs from input and policy contracts")
    if sha256(source / "manifest.json") != m["source_manifest_sha256"] or sha256(
            source / "verification_report.json") != m["source_verification_report_sha256"]:
        raise PanelError("Original acquisition manifest or verification report changed")
    source_manifest = _json_read(source / "manifest.json")
    if set(source_manifest.get("financials", {})) != set(quarters):
        raise PanelError("Acquisition quarter calendar changed since Part 2 construction")
    for quarter in quarters:
        original = financial_path(source, quarter)
        expected_source = source_manifest["financials"][quarter]
        if not original.is_file() or sha256(original) != expected_source.get("sha256"):
            raise PanelError(f"Source Financials partition changed: {quarter}")
        original_metadata = pq.ParquetFile(original)
        if (original_metadata.schema_arrow != arrow_schema("financials") or
            original_metadata.metadata.num_rows != expected_source.get("rows")):
            raise PanelError(f"Source Financials schema/row count changed: {quarter}")
    for endpoint in ("failures", "institutions"):
        original = table_path(source, endpoint)
        entry = source_manifest["tables"][endpoint]
        if not original.is_file() or sha256(original) != entry.get("sha256"):
            raise PanelError(f"Source reference table changed: {endpoint}")
        metadata = pq.ParquetFile(original)
        if metadata.schema_arrow != arrow_schema(endpoint) or metadata.metadata.num_rows != entry.get("rows"):
            raise PanelError(f"Source reference table schema/rows changed: {endpoint}")
    p = m.get("partitions", [])
    if len(p) != len(quarters) or [entry.get("quarter") for entry in p] != list(quarters):
        raise PanelError("Panel partitions differ from required quarter calendar")
    if m.get("panel_fields") != panel_schema().names or m.get("financial_fields") != list(FINANCIAL_FIELDS):
        raise PanelError("Panel output schema or source fields differ from policy")
    if EXCLUDED_PREDICTOR_FIELDS.intersection(m["panel_fields"]):
        raise PanelError("Retrospective event information appears in panel schema")
    count = 0
    checked_history: dict[int, tuple[date, int]] = {}
    for entry in p:
        rel = Path(str(entry.get("file", "")))
        expected = Path("panel") / f"year={entry['quarter'][:4]}" / f"quarter={(int(entry['quarter'][4:6])-1)//3+1}" / "part-00000.parquet"
        if rel != expected:
            raise PanelError("Unexpected partition layout or path")
        file = output / rel
        if not file.is_file() or sha256(file) != entry.get("sha256"):
            raise PanelError(f"Panel partition integrity failure: {file}")
        if file.stat().st_size != entry.get("size_bytes"):
            raise PanelError(f"Panel partition size changed: {file}")
        parquet = pq.ParquetFile(file)
        if parquet.schema_arrow != panel_schema() or parquet.metadata.num_rows != entry.get("rows"):
            raise PanelError(f"Panel schema or row-count mismatch: {file}")
        # Independent row-level check: the panel must be exactly the retained
        # source values, with only deterministic current/past-quarter extras.
        source_file = financial_path(source, entry["quarter"])
        original_rows = pq.ParquetFile(source_file).read().to_pylist()
        expected_rows: list[dict[str, Any]] = []
        for original in original_rows:
            cert, repdte = int(original["CERT"]), original["REPDTE"]
            previous = checked_history.get(cert)
            prior_count = previous[1] if previous else 0
            gap = omitted_quarters(previous[0], repdte) if previous else None
            checked_history[cert] = (repdte, prior_count + 1)
            if asset_exclusion_reason(original.get("ASSET")) is not None:
                continue
            flags = screening_flags(original)
            expected_rows.append({
                **original,
                "MISSING_FINANCIAL_COUNT": count_null_numerics(original, NUMERIC_FIELDS_FIN),
                "PRIOR_REPORT_COUNT": prior_count,
                "REPORT_GAP_QUARTERS": gap,
                "ASSUMED_AVAILABLE_FROM": assumed_available_date(repdte, m["lag_assumption_days"]),
                "DQ_FLAG_COUNT": sum(flags.values()),
                **flags,
            })
        actual_rows = parquet.read().to_pylist()
        if len(actual_rows) != len(expected_rows):
            raise PanelError(f"Filtered source keys differ from output panel: {entry['quarter']}")
        for position, (actual, expected_row) in enumerate(zip(actual_rows, expected_rows)):
            if actual != expected_row:
                raise PanelError(
                    f"Source values or temporal features differ in quarter {entry['quarter']}, row {position}")
        count += entry["rows"]
    if count != m.get("panel_rows") or count + m.get("quarantined_rows", -1) != m["raw_rows"]:
        raise PanelError("Financials source/panel/quarantine reconciliation failed")
    if sum(m.get("terminal_class_counts", {}).values()) != m["source_certificates"]:
        raise PanelError("Terminal-event class coverage incomplete")
    if set(m.get("terminal_class_counts", {})) != set(TERMINAL_CLASSES):
        raise PanelError("Unexpected terminal-event classes")
    expected_audits = {str(Path("audits") / name) for name in AUDIT_FILES} | set(FIGURES)
    if set(m.get("audit_sha256", {})) != expected_audits:
        raise PanelError("Missing or unexpected audit evidence artifacts")
    for name, fingerprint in m["audit_sha256"].items():
        path = output / name
        if not path.is_file() or sha256(path) != fingerprint:
            raise PanelError(f"Audit evidence integrity failure: {name}")
    if len(_csv_rows(output / "audits" / "quarterly_coverage.csv")) != len(quarters):
        raise PanelError("Quarterly coverage audit is incomplete")
    if len(_csv_rows(output / "audits" / "terminal_exits.csv")) != m["source_certificates"]:
        raise PanelError("Terminal-exit table and bank count differ")
    if len(_csv_rows(output / "audits" / "failure_linkage.csv")) != m["failure_reference_rows"]:
        raise PanelError("Failure source-row audit count differs")
    if len(_csv_rows(output / "audits" / "quarantined_rows.csv")) != m["quarantined_rows"]:
        raise PanelError("Quarantine count differs from saved evidence")
    if len(_csv_rows(output / "audits" / "reporting_gaps.csv")) != m["reporting_gap_intervals"]:
        raise PanelError("Reporting-gap count differs from saved evidence")
    result = {
        "success": True, "part2_manifest_sha256": sha256(manifest_path),
        "source_manifest_sha256": m["source_manifest_sha256"],
        "source_verification_report_sha256": m["source_verification_report_sha256"],
        "storage_verification_sha256": m["storage_verification_sha256"],
        "code_commit": m["code_commit"], "source_rows": m["raw_rows"],
        "panel_rows": m["panel_rows"], "quarantined_rows": m["quarantined_rows"],
        "quarters": len(quarters), "source_certificates": m["source_certificates"],
        "clean_certificates": m["clean_certificates"],
        "failure_reference_rows": m["failure_reference_rows"],
        "failure_missing_cert_rows": m["failure_missing_cert_rows"],
        "terminal_class_counts": m["terminal_class_counts"],
        "quality_rule_counts": m["quality_rule_counts"],
        "reporting_gap_intervals": m["reporting_gap_intervals"],
        "availability_lag_days_assumed": m["lag_assumption_days"],
        "actual_publication_times_verified": False,
        "predictor_event_columns_absent": True,
        "audit_artifacts_verified": len(expected_audits),
    }
    if compare_existing:
        saved = _json_read(output / "part2_validation.json")
        if saved != result:
            raise PanelError("Read-only verification differs from published validation artifact")
    return result


def _check_root(root: Path) -> tuple[Path, Mapping[str, Any], str]:
    """Require both published Part 1 stages, including storage provenance."""
    from .project_flow import resolve_stage
    from .workflow import acquisition_contract
    acquisition = resolve_stage(root, "acquisition")
    storage = resolve_stage(root, "storage")
    source = Path(acquisition["path"])
    checked = acquisition_contract(source)
    if (acquisition["validation"].get("source_manifest_sha256") != checked["manifest_sha256"] or
        acquisition["validation"].get("source_verification_sha256") != checked["verification_report_sha256"]):
        raise PanelError("Acquisition validation and source manifest differ")
    if (storage["validation"].get("source_manifest_sha256") != checked["manifest_sha256"] or
        storage["validation"].get("source_verification_sha256") != checked["verification_report_sha256"]):
        raise PanelError("Storage result does not belong to the selected acquisition")
    return source, checked, storage["verification_sha256"]


def run_project_stage(root: Path, code_commit: str, *, lag_days: int = 90,
                      replace_incomplete: bool = False) -> dict[str, Any]:
    """Build once and publish an immutable, verified Part 2 stage."""
    import shutil
    from .project_flow import HandoffError, publish_stage, resolve_stage, stage_dir
    root = Path(root)
    source, summary, storage_sha = _check_root(root)
    output = stage_dir(root, "quality_panel")
    try:
        published = resolve_stage(root, "quality_panel")
    except HandoffError as exc:
        if "has not passed verification" not in str(exc):
            raise
        published = None
    if published is not None:
        verdict = verify_panel(source, output, source_summary=summary,
                               storage_verification_sha256=storage_sha)
        if published["verification_sha256"] != sha256(output / "part2_validation.json"):
            raise PanelError("Published stage validation SHA-256 changed")
        LOG.info("Previously published Part 2 passed read-only verification")
        return verdict
    if output.exists():
        if not replace_incomplete:
            raise PanelError("Incomplete output exists. Use --replace-incomplete to rebuild the unpublished stage")
        if not output.is_dir() or output.is_symlink():
            raise PanelError("Refusing to remove a non-directory or symlink stage")
        shutil.rmtree(output)
    verdict = build_panel(source, output, source_summary=summary,
                          storage_verification_sha256=storage_sha,
                          code_commit=code_commit, availability_lag_days=lag_days)
    published = publish_stage(root, "quality_panel", "part2_validation.json")
    if published["verification_sha256"] != sha256(output / "part2_validation.json"):
        raise PanelError("Part 2 published stage fingerprint mismatch")
    return verdict


def verify_project_stage(root: Path) -> dict[str, Any]:
    from .project_flow import resolve_stage
    source, summary, storage_sha = _check_root(Path(root))
    stage = resolve_stage(root, "quality_panel")
    return verify_panel(source, stage["path"], source_summary=summary,
                        storage_verification_sha256=storage_sha)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verified FDIC data-quality and panel stage")
    actions = parser.add_subparsers(dest="action", required=True)
    build = actions.add_parser("build")
    build.add_argument("--drive-root", type=Path, required=True)
    build.add_argument("--code-commit", required=True)
    build.add_argument("--availability-lag-days", type=int, default=90)
    build.add_argument("--replace-incomplete", action="store_true")
    verify = actions.add_parser("verify")
    verify.add_argument("--drive-root", type=Path, required=True)
    for command in (build, verify):
        command.add_argument("--log-file", type=Path, default=None)
    args = parser.parse_args(argv)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_file is not None:
        args.log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(args.log_file, mode="a", encoding="utf-8"))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s UTC | %(levelname)s | %(message)s",
                        handlers=handlers, force=True)
    if args.action == "build":
        value = run_project_stage(args.drive_root, args.code_commit,
                                  lag_days=args.availability_lag_days,
                                  replace_incomplete=args.replace_incomplete)
    else:
        value = verify_project_stage(args.drive_root)
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

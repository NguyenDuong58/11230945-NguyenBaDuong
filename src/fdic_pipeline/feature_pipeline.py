"""Verified FDIC financial feature tables and four-quarter outcome evidence.

The selected experiment and immutable stage manifests determine all input paths.
The Failures reference is used only for event labels; no future event data enter
feature formulas, training predictors, or preprocessing fitting inputs.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import shutil
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .acquisition import DEFAULT_START, DEFAULT_END, quarter_ends, sha256, table_path
from .data_quality import TERMINAL_CLASSES, parse_source_date
from .feature_policy import (
    AVAILABILITY_LAG_DAYS, AVAILABILITY_SENSITIVITY, DEVELOPMENT_END,
    EXCLUDED_PREDICTOR_FIELDS, FEATURE_NAMES, FEATURE_SPECS, FEATURE_RATIONALES, LAST_LABEL_QUARTER,
    SEED, STUDY_END, TARGET_COLUMN, TEST_END, TEST_START, EventLedger,
    calculate_features, label_at_quarter, quarter_index, split_name,
    unfit_preprocessor,
)
from .panel import panel_schema

LOG = logging.getLogger("fdic_features")
TARGET_SPLITS = ("development", "test_holdout")
AUDIT_FILES = (
    "audits/feature_dictionary.csv",
    "audits/sample_summary.csv",
    "audits/label_status.csv",
    "audits/failure_events_by_year.csv",
    "audits/failure_events_by_period.csv",
    "audits/restype_distribution.csv",
    "audits/development_availability_sensitivity.csv",
    "audits/feature_missingness_development.csv",
    "audits/preprocessing_fit.json",
    "figures/development_feature_missingness.png",
    "figures/development_labels_by_year.png",
    "figures/qualifying_failures_by_year.png",
)


class FeatureError(RuntimeError):
    """Invalid upstream provenance, label policy or feature data artifact."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise FeatureError(f"Missing or invalid JSON: {path}") from exc
    if not isinstance(obj, dict):
        raise FeatureError(f"Expected JSON object: {path}")
    return obj


def _write_json(path: Path, obj: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as fp:
            json.dump(dict(obj), fp, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
            fp.write("\n")
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_csv(path: Path, columns: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=columns, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fp:
        return list(csv.DictReader(fp))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def feature_schema():
    import pyarrow as pa
    fields = [pa.field("CERT", pa.int64(), nullable=False),
              pa.field("REPDTE", pa.date32(), nullable=False),
              pa.field("ASSUMED_AVAILABLE_FROM", pa.date32(), nullable=False)]
    fields += [pa.field(x, pa.float64()) for x in FEATURE_NAMES]
    fields += [pa.field(TARGET_COLUMN, pa.int8()),
               pa.field("LABEL_STATUS", pa.string(), nullable=False),
               pa.field("SPLIT", pa.string(), nullable=False)]
    schema = pa.schema(fields)
    if len(FEATURE_NAMES) != 21 or EXCLUDED_PREDICTOR_FIELDS & set(FEATURE_NAMES):
        raise FeatureError("Predictor list includes identifiers or outcome information")
    return schema


def model_schema():
    import pyarrow as pa
    fields = [pa.field("CERT", pa.int64(), nullable=False),
              pa.field("REPDTE", pa.date32(), nullable=False),
              pa.field("ASSUMED_AVAILABLE_FROM", pa.date32(), nullable=False)]
    fields += [pa.field(x, pa.float64()) for x in FEATURE_NAMES]
    fields += [pa.field(TARGET_COLUMN, pa.int8(), nullable=False)]
    return pa.schema(fields)


def _normalise_restype(value: Any) -> str:
    return str(value).strip().upper() if value is not None and str(value).strip() else "MISSING"


def _parse_iso(value: str | None, *, field: str, cert: int) -> date | None:
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise FeatureError(f"Invalid {field} for CERT {cert}: {value!r}") from exc


def event_ledgers(failures: Sequence[Mapping[str, Any]],
                  terminals: Sequence[Mapping[str, str]]) -> tuple[dict[int, EventLedger], dict[str, Any]]:
    """Separate actual FAILURE records from ASSISTANCE; preserve unknown events.

    Institution end dates coinciding with a resolved failure or assistance event
    are not counted a second time as competing non-failure exits.
    """
    terminal_index: dict[int, Mapping[str, str]] = {}
    for r in terminals:
        cert = int(r["CERT"])
        if cert <= 0 or cert in terminal_index:
            raise FeatureError(f"Repeated or invalid terminal certificate: {cert}")
        terminal_index[cert] = r
    by_cert: dict[int, list[tuple[str, date | None, str]]] = defaultdict(list)
    restypes: Counter[str] = Counter()
    missing_cert = 0
    malformed = 0
    dated_test_failure_records = 0
    dated_test_failure_certs: set[int] = set()
    dated_test_failure_certs_in_panel: set[int] = set()
    annual: Counter[int] = Counter()
    dated_dev_failure_certs: set[int] = set()
    source_by_period: dict[str, Counter[str]] = {
        "development_calendar": Counter(), "test_calendar": Counter()
    }
    for record in failures:
        typ = _normalise_restype(record.get("RESTYPE"))
        restypes[typ] += 1
        cert_raw = record.get("CERT")
        cert = int(cert_raw) if cert_raw is not None else None
        if cert is None:
            missing_cert += 1
        raw_day = record.get("FAILDATE")
        try:
            day = parse_source_date(raw_day)
        except ValueError:
            day = None
            malformed += 1
        if day is not None and typ == "FAILURE":
            annual[day.year] += 1
            if date(1984, 1, 1) <= day <= DEVELOPMENT_END:
                dated_dev_failure_certs.add(cert) if cert is not None else None
                source_by_period["development_calendar"][typ] += 1
            if date(2008, 1, 1) <= day <= TEST_END:
                dated_test_failure_records += 1
                if cert is not None:
                    dated_test_failure_certs.add(cert)
                    if cert in terminal_index:
                        dated_test_failure_certs_in_panel.add(cert)
                source_by_period["test_calendar"][typ] += 1
        elif day is not None and typ == "ASSISTANCE":
            if date(1984, 1, 1) <= day <= DEVELOPMENT_END:
                source_by_period["development_calendar"][typ] += 1
            if date(2008, 1, 1) <= day <= TEST_END:
                source_by_period["test_calendar"][typ] += 1
        if cert is not None and cert in terminal_index:
            by_cert[cert].append((typ, day, "unparseable_date" if raw_day and day is None else ""))

    ledgers: dict[int, EventLedger] = {}
    uncertainties: Counter[str] = Counter()
    for cert, source in terminal_index.items():
        last = _parse_iso(source.get("LAST_REPDTE"), field="LAST_REPDTE", cert=cert)
        if last is None:
            raise FeatureError(f"Missing last reporting date for CERT {cert}")
        cls = source.get("TERMINAL_CLASS", "")
        if cls and cls not in TERMINAL_CLASSES:
            raise FeatureError(f"Unknown terminal evidence class for CERT {cert}: {cls}")
        end = _parse_iso(source.get("INSTITUTION_END_DATE"), field="INSTITUTION_END_DATE", cert=cert)
        # Do not establish label maturity using structural events beyond the
        # declared study endpoint, even if the reference API was later updated.
        if end is not None and end > STUDY_END:
            end = None
        records = by_cert.get(cert, [])
        fail_days = {d for typ, d, _ in records if typ == "FAILURE" and d is not None and d <= STUDY_END}
        assist_days = {d for typ, d, _ in records if typ == "ASSISTANCE" and d is not None and d <= STUDY_END}
        unknown = [typ for typ, d, bad in records if typ not in ("FAILURE", "ASSISTANCE") or bad or d is None]
        reason = None
        if cls in ("ambiguous_failure_evidence", "ambiguous_failure_events",
                   "inconsistent_institution_end_date", "unparseable_institution_end_date"):
            reason = "unresolved_terminal_reference"
        if unknown:
            reason = "unclassifiable_failure_reference"
        elif len(fail_days) > 1:
            reason = "multiple_failure_dates"
        elif fail_days & assist_days:
            reason = "simultaneous_failure_assistance"
        fail_day = min(fail_days) if fail_days else None
        # A source end date coinciding with resolution does not constitute another exit.
        other_end = None if end is not None and end in (fail_days | assist_days) else end
        if reason is None and fail_day is not None and fail_day < last:
            reason = "failure_precedes_last_financial_report"
        if reason is None and other_end is not None and other_end < last:
            reason = "institution_end_precedes_last_financial_report"
        if reason:
            uncertainties[reason] += 1
        ledgers[cert] = EventLedger(last_report=last, failure_day=fail_day,
                                    assistance_days=tuple(sorted(assist_days)),
                                    institution_end=other_end, uncertain_reason=reason)
    audit = {
        "failure_source_rows": len(failures),
        "source_rows_missing_cert": missing_cert,
        "source_rows_unparseable_date": malformed,
        "resolution_type_counts": dict(sorted(restypes.items())),
        "test_2008_2014_failure_source_rows": dated_test_failure_records,
        "test_2008_2014_distinct_failure_certs": len(dated_test_failure_certs),
        "test_2008_2014_distinct_failure_certs_in_panel": len(dated_test_failure_certs_in_panel),
        "development_calendar_distinct_failure_certs": len(dated_dev_failure_certs),
        "failure_source_rows_by_year": {str(k):v for k,v in sorted(annual.items())},
        "event_source_rows_by_period": {k: dict(v) for k, v in source_by_period.items()},
        "uncertain_certificates_by_reason": dict(sorted(uncertainties.items())),
    }
    return ledgers, audit


def source_contract(root: Path, *, verify_hashes: bool = True) -> dict[str, Any]:
    """Require published Parts 1–3 and source Parquet/audit fingerprints."""
    from .project_flow import get_active, resolve_stage
    from .scalable_processing import input_contract
    active = get_active(root)
    snap = input_contract(root, check_data_hashes=verify_hashes)
    proc = resolve_stage(root, "processing")
    qual = resolve_stage(root, "quality_panel")
    acq = resolve_stage(root, "acquisition")
    if proc["validation"].get("part2_manifest_sha256") != snap["part2_manifest_sha256"]:
        raise FeatureError("Part 3 evidence does not belong to verified Part 2 panel")
    if proc["validation"].get("quality_verification_sha256") != snap["quality_verification_sha256"]:
        raise FeatureError("Part 3 processing source lineage differs")
    quality_dir = Path(qual["path"])
    qm = _read_json(quality_dir / "part2_manifest.json")
    src_dir = Path(acq["path"])
    am = _read_json(src_dir / "manifest.json")
    fail_entry = am["tables"]["failures"]
    fail_path = table_path(src_dir, "failures")
    if verify_hashes and (not fail_path.is_file() or sha256(fail_path) != fail_entry["sha256"]):
        raise FeatureError("FDIC Failures table differs from verified acquisition")
    event_file = quality_dir / "audits" / "terminal_exits.csv"
    if (verify_hashes and (not event_file.is_file() or sha256(event_file) !=
            qm["audit_sha256"]["audits/terminal_exits.csv"])):
        raise FeatureError("Part 2 institutional terminal audit changed")
    return {
        **snap,
        "quality_path": str(quality_dir),
        "acquisition_path": str(src_dir),
        "failures_sha256": fail_entry["sha256"],
        "terminal_audit_sha256": qm["audit_sha256"]["audits/terminal_exits.csv"],
        "processing_verification_sha256": proc["verification_sha256"],
        "feature_policy": "four_quarters_from_REPDTE_with_90_day_eligibility",
        "feature_code_commit": "",  # Set to the independently checked Part 4 revision.
    }


def load_events(snap: Mapping[str, Any]) -> tuple[dict[int, EventLedger], dict[str, Any]]:
    import pyarrow.parquet as pq
    path = table_path(Path(snap["acquisition_path"]), "failures")
    if sha256(path) != snap["failures_sha256"]:
        raise FeatureError("Failure reference SHA-256 mismatch")
    records = pq.read_table(path, columns=["CERT", "FAILDATE", "RESTYPE"]).to_pylist()
    audit_path = Path(snap["quality_path"]) / "audits" / "terminal_exits.csv"
    if sha256(audit_path) != snap["terminal_audit_sha256"]:
        raise FeatureError("Terminal-audit SHA-256 mismatch")
    return event_ledgers(records, _csv_rows(audit_path))


def policy_lock() -> dict[str, Any]:
    return {
        "study_end": STUDY_END.isoformat(),
        "last_label_quarter": LAST_LABEL_QUARTER.isoformat(),
        "development_end": DEVELOPMENT_END.isoformat(),
        "purge_quarters": ["2007Q1", "2007Q2", "2007Q3", "2007Q4"],
        "test_start": TEST_START.isoformat(), "test_end": TEST_END.isoformat(),
        "label_definition": "1{RESTYPE=FAILURE and FAILDATE in (REPDTE, REPDTE+4 quarters]}",
        "assumed_availability_lag_days": AVAILABILITY_LAG_DAYS,
        "assumption_is_verified_release_date": False,
        "assistance_handling": "competing event; not failure",
        "other_exit_handling": "censored if competing within horizon",
        "uncertain_and_incomplete_followup": "censored, not treated as nonfailure",
        "feature_names": list(FEATURE_NAMES),
        "economic_rationales": FEATURE_RATIONALES,
        "preprocessing": ["SimpleImputer(median,add_indicator=True,keep_empty_features=True)",
                           "RobustScaler(quantile_range=(25,75))"],
        "development_only_fit": True,
        "refit_preprocessing_in_every_part5_fold": True,
        "seed": SEED,
    }


def _feature_partition_path(q: str) -> Path:
    num = (int(q[4:6])-1)//3+1
    return Path("feature_table") / f"year={q[:4]}" / f"quarter={num}" / "part-00000.parquet"


def _draw(output: Path, missing_dev: Mapping[str, int], dev_rows: int,
          year_positive: Mapping[int, int], failure_year: Mapping[int, int]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    figures = output / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    entries = sorted(missing_dev, key=lambda k: (-missing_dev[k], k))[:15]
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.barh(list(reversed(entries)), [100*missing_dev[k]/dev_rows if dev_rows else 0 for k in reversed(entries)])
    ax.set_xlabel("Missing development observations (%)")
    ax.set_title("Training-period feature missingness")
    fig.tight_layout()
    fig.savefig(figures / "development_feature_missingness.png", dpi=160)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 4))
    years = sorted(year_positive)
    ax.bar(years, [year_positive[y] for y in years])
    ax.set_xlabel("Financial reporting year")
    ax.set_ylabel("Positive development bank-quarter labels")
    ax.set_title("Four-quarter positive labels in development")
    fig.tight_layout()
    fig.savefig(figures / "development_labels_by_year.png", dpi=160)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(10, 4))
    yy = sorted(int(y) for y in failure_year)
    ax.bar(yy, [failure_year[str(y)] for y in yy])
    ax.set_xlabel("Failure-event year")
    ax.set_ylabel("FDIC FAILURE source records")
    ax.set_title("Qualifying FDIC failure events by date")
    fig.tight_layout()
    fig.savefig(figures / "qualifying_failures_by_year.png", dpi=160)
    plt.close(fig)


def _fit_development_preprocessor(output: Path, dev_rows: int) -> dict[str, Any]:
    import joblib
    import numpy as np
    import pyarrow.parquet as pq
    import sklearn
    path = output / "model_data" / "development.parquet"
    if pq.ParquetFile(path).metadata.num_rows != dev_rows or not dev_rows:
        raise FeatureError("Development-only preprocessing fit has no valid rows")
    columns = pq.read_table(path, columns=list(FEATURE_NAMES)).to_pandas()
    if list(columns.columns) != list(FEATURE_NAMES) or len(columns) != dev_rows:
        raise FeatureError("Development feature matrix changed during fit")
    pp = unfit_preprocessor()
    pp.fit(columns)
    transformed = pp.transform(columns.iloc[: min(2048, len(columns))])
    if not np.isfinite(transformed).all():
        raise FeatureError("Development-only preprocessing yielded nonfinite values")
    target = output / "model_data" / "development_preprocessor.joblib"
    joblib.dump(pp, target, compress=3)
    metadata = {
        "fit_split": "development", "fit_rows": dev_rows,
        "feature_names": list(FEATURE_NAMES), "sklearn_version": sklearn.__version__,
        "feature_count_in": len(FEATURE_NAMES), "feature_count_out": int(transformed.shape[1]),
        "all_missing_development_features": [x for x in FEATURE_NAMES if columns[x].isna().all()],
        "test_data_used_in_fit": False,
        "refit_required_per_validation_fold": True,
        "joblib_sha256": sha256(target),
    }
    _write_json(output / "audits" / "preprocessing_fit.json", metadata)
    return metadata


def build_feature_tables(panel_files: Sequence[Mapping[str, Any]], panel_root: Path,
                         out: Path, ledgers: Mapping[int, EventLedger],
                         event_audit: Mapping[str, Any],
                         *, code_commit: str, upstream: Mapping[str, Any],
                         test_calendar_min_failures: int = 50,
                         enforce_full_calendar: bool = True) -> dict[str, Any]:
    """Stream chronological panel partitions into feature, development and test artifacts.

    `panel_files` must be supplied by the verified Part 2 manifest in production.
    A subset can be supplied by synthetic integration tests only.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    if out.exists():
        raise FeatureError("Refusing to overwrite a feature-stage directory")
    if len(code_commit) != 40 or any(x not in "0123456789abcdef" for x in code_commit):
        raise FeatureError("A full source code commit must be recorded")
    ordered = list(panel_files)
    if not ordered or [x["quarter"] for x in ordered] != sorted(x["quarter"] for x in ordered):
        raise FeatureError("Input quarters must be strictly chronological")
    if len(set(x["quarter"] for x in ordered)) != len(ordered):
        raise FeatureError("Duplicate input quarters")
    if enforce_full_calendar and [x["quarter"] for x in ordered] != quarter_ends(DEFAULT_START, DEFAULT_END):
        raise FeatureError("Production feature stage requires all 170 historical quarters")
    if event_audit["test_2008_2014_distinct_failure_certs_in_panel"] < test_calendar_min_failures:
        raise FeatureError("Fewer than 50 documented, linked FAILURE events occur in the test calendar window")
    out.mkdir(parents=True, exist_ok=False)
    (out / "model_data").mkdir()
    (out / "audits").mkdir()
    (out / "feature_table").mkdir()
    _write_json(out / "feature_policy.lock.json", policy_lock())
    history: dict[int, dict[int, dict[str, Any]]] = defaultdict(dict)
    counts: Counter[tuple[str, str]] = Counter()
    missing_dev: Counter[str] = Counter()
    positive_development_years: Counter[int] = Counter()
    dev_positive_certs: set[int] = set()
    test_positive_certs: set[int] = set()
    last_key: set[tuple[int, date]] = set()
    partitions: list[dict[str, Any]] = []
    failures_in_panel_certs = set(ledgers)
    unmatched_certs = 0
    model_count = Counter()
    quality_total = 0
    total = 0
    sensitivity: Counter[tuple[int, str]] = Counter()
    writers = {}
    try:
        for split in TARGET_SPLITS:
            writers[split] = pq.ParquetWriter(out / "model_data" / f"{split}.parquet", model_schema(),
                                               compression="snappy")
        for idx, entry in enumerate(ordered, 1):
            source_file = panel_root / entry["file"]
            if not source_file.is_file() or sha256(source_file) != entry["sha256"]:
                raise FeatureError(f"Panel SHA-256 mismatch: {entry['quarter']}")
            parquet = pq.ParquetFile(source_file)
            if parquet.schema_arrow != panel_schema() or parquet.metadata.num_rows != entry["rows"]:
                raise FeatureError(f"Panel schema/row count mismatch: {entry['quarter']}")
            inputs = parquet.read().to_pylist()
            fq: list[dict[str, Any]] = []
            grouped: dict[str, list[dict[str, Any]]] = {k: [] for k in TARGET_SPLITS}
            for row in inputs:
                cert, day = int(row["CERT"]), row["REPDTE"]
                key = (cert, day)
                if key in last_key:
                    raise FeatureError(f"Repeated source key: {key}")
                last_key.add(key)
                if cert not in failures_in_panel_certs:
                    unmatched_certs += 1
                    raise FeatureError(f"Panel CERT without terminal audit: {cert}")
                qid = quarter_index(day)
                known = history[cert]
                if qid in known:
                    raise FeatureError(f"Repeated report quarter for CERT {cert}")
                values = calculate_features(row, known)
                # Preserve report-specific history; never fill an absent quarter.
                known[qid] = {"ASSET": row.get("ASSET"), "DEP": row.get("DEP"), "ROA": row.get("ROA")}
                # Keep only the four calendar quarters relevant to future lags.
                for stale in tuple(known):
                    if stale < qid-4:
                        del known[stale]
                assumed_date = day + timedelta(days=AVAILABILITY_LAG_DAYS)
                if row["ASSUMED_AVAILABLE_FROM"] != assumed_date:
                    raise FeatureError(f"Part 2 assumed availability differs at {key}")
                split = split_name(day)
                y, reason = label_at_quarter(day, ledgers[cert])
                counts[(split, reason)] += 1
                if split == "development":
                    for name, value in values.items():
                        if value is None:
                            missing_dev[name] += 1
                    for lag in AVAILABILITY_SENSITIVITY:
                        yy, why = label_at_quarter(day, ledgers[cert], lag)
                        sensitivity[(lag, "positive" if yy == 1 else "negative" if yy == 0 else "censored")] += 1
                feature = {"CERT": cert, "REPDTE": day,
                           "ASSUMED_AVAILABLE_FROM": assumed_date, **values,
                           TARGET_COLUMN: y, "LABEL_STATUS": reason, "SPLIT": split}
                fq.append(feature)
                if split in TARGET_SPLITS and y is not None:
                    if y == 1:
                        (dev_positive_certs if split == "development" else test_positive_certs).add(cert)
                        if split == "development":
                            positive_development_years[day.year] += 1
                    model_count[(split, "positive" if y == 1 else "negative")] += 1
                    grouped[split].append({"CERT": cert, "REPDTE": day,
                                           "ASSUMED_AVAILABLE_FROM": assumed_date,
                                           **values, TARGET_COLUMN: y})
            relative = _feature_partition_path(entry["quarter"])
            path = out / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(fq, schema=feature_schema())
            pq.write_table(table, path, compression="snappy", row_group_size=100_000)
            for split in TARGET_SPLITS:
                if grouped[split]:
                    writers[split].write_table(pa.Table.from_pylist(grouped[split], schema=model_schema()))
            if table.num_rows != entry["rows"]:
                raise FeatureError("Feature-table row count changed")
            partitions.append({"quarter": entry["quarter"], "file": str(relative),
                               "rows": table.num_rows, "sha256": sha256(path),
                               "size_bytes": path.stat().st_size})
            total += table.num_rows
            quality_total += len(inputs)
            if idx % 20 == 0 or idx == len(ordered):
                LOG.info("Constructed %d/%d feature quarters (%d observations)", idx, len(ordered), total)
    finally:
        for writer in writers.values():
            writer.close()
    if total != sum(item["rows"] for item in ordered) or total != quality_total:
        raise FeatureError("Feature rows do not reconcile to input panel")
    if set(history) != failures_in_panel_certs and enforce_full_calendar:
        raise FeatureError("Feature and terminal certificate populations differ")
    dev_count = sum(model_count[("development", c)] for c in ("positive", "negative"))
    test_count = sum(model_count[("test_holdout", c)] for c in ("positive", "negative"))
    if enforce_full_calendar and (dev_count == 0 or test_count == 0 or model_count[("development", "positive")] == 0):
        raise FeatureError("No eligible modelling observations for the prespecified splits")
    if enforce_full_calendar and len(test_positive_certs) < 50:
        raise FeatureError("Test holdout contains fewer than 50 distinct CERT with eligible positive labels")
    # Explicitly require nonempty file rows for production: synthetic tests may use smaller windows.
    for split, expected in (("development", dev_count), ("test_holdout", test_count)):
        actual = pq.ParquetFile(out / "model_data" / f"{split}.parquet").metadata.num_rows
        if expected != actual:
            raise FeatureError(f"Model sample count mismatch: {split}")
    _write_csv(out / "audits" / "feature_dictionary.csv",
               ("FEATURE", "GROUP", "FORMULA", "INFORMATION_SET", "ECONOMIC_RATIONALE"),
               [{**dict(zip(("FEATURE", "GROUP", "FORMULA", "INFORMATION_SET"), item)),
                 "ECONOMIC_RATIONALE": FEATURE_RATIONALES[item[0]]}
                for item in FEATURE_SPECS])
    split_stats: list[dict[str, Any]] = []
    for name in ("development", "purge_gap", "test_holdout", "unused_post_test", "label_immature"):
        part_rows = sum(count for (s, _), count in counts.items() if s == name)
        positive = model_count[(name, "positive")]
        negative = model_count[(name, "negative")]
        split_stats.append({"SPLIT": name, "FEATURE_ROWS": part_rows,
                            "ELIGIBLE_MODEL_ROWS": positive + negative,
                            "POSITIVE_BANK_QUARTERS": positive,
                            "NEGATIVE_BANK_QUARTERS": negative,
                            "CENSORED_OR_UNUSED_ROWS": part_rows - positive - negative})
    _write_csv(out / "audits" / "sample_summary.csv", tuple(split_stats[0]), split_stats)
    _write_csv(out / "audits" / "label_status.csv", ("SPLIT", "LABEL_STATUS", "ROWS"),
               [{"SPLIT": k[0], "LABEL_STATUS": k[1], "ROWS": v} for k,v in sorted(counts.items())])
    _write_csv(out / "audits" / "failure_events_by_year.csv", ("YEAR", "FAILURE_SOURCE_ROWS"),
               [{"YEAR": k, "FAILURE_SOURCE_ROWS": v} for k,v in event_audit["failure_source_rows_by_year"].items()])
    _write_csv(out / "audits" / "failure_events_by_period.csv", ("PERIOD", "SOURCE_FAILURE_ROWS", "SOURCE_ASSISTANCE_ROWS"),
               [{"PERIOD": name,
                 "SOURCE_FAILURE_ROWS": r.get("FAILURE",0),
                 "SOURCE_ASSISTANCE_ROWS": r.get("ASSISTANCE",0)}
                for name,r in event_audit["event_source_rows_by_period"].items()])
    _write_csv(out / "audits" / "restype_distribution.csv", ("RESTYPE", "SOURCE_ROWS"),
               [{"RESTYPE": k, "SOURCE_ROWS": v} for k,v in event_audit["resolution_type_counts"].items()])
    _write_csv(out / "audits" / "development_availability_sensitivity.csv",
               ("ASSUMED_LAG_DAYS", "POSITIVE_BANK_QUARTERS", "NEGATIVE_BANK_QUARTERS", "CENSORED_BANK_QUARTERS"),
               [{"ASSUMED_LAG_DAYS": lag, "POSITIVE_BANK_QUARTERS": sensitivity[(lag,"positive")],
                 "NEGATIVE_BANK_QUARTERS": sensitivity[(lag,"negative")],
                 "CENSORED_BANK_QUARTERS": sensitivity[(lag,"censored")]}
                for lag in AVAILABILITY_SENSITIVITY])
    _write_csv(out / "audits" / "feature_missingness_development.csv",
               ("FEATURE", "DEVELOPMENT_REPORTS", "MISSING_ROWS", "MISSING_PCT"),
               [{"FEATURE": f,"DEVELOPMENT_REPORTS": sum(n for (sp,_),n in counts.items() if sp=="development"),
                 "MISSING_ROWS": missing_dev[f],
                 "MISSING_PCT": round(100*missing_dev[f] / max(1, sum(n for (sp,_),n in counts.items() if sp=="development")),6)}
                for f in FEATURE_NAMES])
    preprocessing = _fit_development_preprocessor(out, dev_count) if dev_count > 0 else {}
    _draw(out, missing_dev, sum(n for (sp,_),n in counts.items() if sp=="development"),
          positive_development_years, event_audit["failure_source_rows_by_year"])
    model_files = {split: {
        "file": f"model_data/{split}.parquet",
        "rows": sum(model_count[(split,c)] for c in ("positive","negative")),
        "sha256": sha256(out / "model_data" / f"{split}.parquet")}
        for split in TARGET_SPLITS}
    manifest = {
        "schema_version": 1, "created_utc": _now(), "code_commit": code_commit,
        "experiment_id": upstream["experiment_id"],
        "part2_manifest_sha256": upstream["part2_manifest_sha256"],
        "quality_verification_sha256": upstream["quality_verification_sha256"],
        "processing_verification_sha256": upstream["processing_verification_sha256"],
        "source_failures_sha256": upstream["failures_sha256"],
        "source_terminal_audit_sha256": upstream["terminal_audit_sha256"],
        "policy_sha256": sha256(out / "feature_policy.lock.json"),
        "feature_names": list(FEATURE_NAMES),
        "source_rows": total, "partitions": partitions,
        "model_files": model_files,
        "model_counts": {s: {c:model_count[(s,c)] for c in ("positive","negative")} for s in TARGET_SPLITS},
        "distinct_certs_with_positive_labels": {"development":len(dev_positive_certs),
                                                 "test_holdout":len(test_positive_certs)},
        "label_status_counts": [{"split": s, "status": st, "rows": n} for (s,st),n in sorted(counts.items())],
        "source_event_audit": event_audit,
        "preprocessor_sha256": preprocessing.get("joblib_sha256"),
        "audits_sha256": {item: sha256(out / item) for item in AUDIT_FILES},
    }
    _write_json(out / "part4_manifest.json", manifest)
    verdict = verify_feature_stage(out, upstream, compare_published=False,
                                   check_data_values=False,
                                   min_test_failures=test_calendar_min_failures)
    _write_json(out / "part4_validation.json", verdict)
    return verdict


def verify_feature_stage(out: Path, upstream: Mapping[str, Any], *,
                         compare_published: bool = True,
                         check_data_values: bool = True,
                         min_test_failures: int = 50) -> dict[str, Any]:
    """Read-only validation of provenance, feature rows, model split and evidence.

    `check_data_values=True` recomputes every predictor and label from source
    records. It also checks every development/test record against the full
    feature table, without fitting a transformer or evaluating a model.
    """
    import pyarrow.parquet as pq
    output = Path(out)
    m = _read_json(output / "part4_manifest.json")
    if (m.get("experiment_id") != upstream["experiment_id"] or
        m.get("code_commit") != upstream["feature_code_commit"] or
        m.get("part2_manifest_sha256") != upstream["part2_manifest_sha256"] or
        m.get("quality_verification_sha256") != upstream["quality_verification_sha256"] or
        m.get("processing_verification_sha256") != upstream["processing_verification_sha256"] or
        m.get("source_failures_sha256") != upstream["failures_sha256"] or
        m.get("source_terminal_audit_sha256") != upstream["terminal_audit_sha256"] or
        m.get("feature_names") != list(FEATURE_NAMES) or
        m.get("source_rows") != upstream["panel_rows"]):
        raise FeatureError("Part 4 manifest or source lineage mismatch")
    if sha256(output / "feature_policy.lock.json") != m["policy_sha256"] or _read_json(output / "feature_policy.lock.json") != policy_lock():
        raise FeatureError("Locked feature or outcome policy changed")
    if set(m["audits_sha256"]) != set(AUDIT_FILES):
        raise FeatureError("Incomplete evidence inventory")
    for path, digest in m["audits_sha256"].items():
        if not (output/path).is_file() or sha256(output/path) != digest:
            raise FeatureError(f"Audit evidence modified: {path}")
    if sha256(output / "model_data" / "development_preprocessor.joblib") != m["preprocessor_sha256"]:
        raise FeatureError("Development fitted preprocessor changed")
    prep = _read_json(output / "audits" / "preprocessing_fit.json")
    if (prep["test_data_used_in_fit"] or prep["fit_split"] != "development" or
        prep["feature_names"] != list(FEATURE_NAMES) or
        prep["fit_rows"] != m["model_files"]["development"]["rows"] or
        prep["joblib_sha256"] != m["preprocessor_sha256"]):
        raise FeatureError("Training-only preprocessor metadata inconsistency")
    if m["source_event_audit"]["test_2008_2014_distinct_failure_certs_in_panel"] < min_test_failures:
        raise FeatureError("Test period does not contain fifty documented linked failures")
    parts = m["partitions"]
    if len(parts) != len(upstream["partitions"]):
        raise FeatureError("Missing feature quarters")
    for src, entry in zip(upstream["partitions"], parts):
        if src["quarter"] != entry["quarter"] or src["rows"] != entry["rows"] or entry["file"] != str(_feature_partition_path(entry["quarter"])):
            raise FeatureError("Feature-source partition mapping differs")
        p = output / entry["file"]
        if not p.is_file() or p.stat().st_size != entry["size_bytes"] or sha256(p) != entry["sha256"]:
            raise FeatureError(f"Feature partition integrity failure: {p}")
        pf = pq.ParquetFile(p)
        if pf.schema_arrow != feature_schema() or pf.metadata.num_rows != entry["rows"]:
            raise FeatureError(f"Feature partition schema or rows invalid: {p}")
    for split, entry in m["model_files"].items():
        file = output / entry["file"]
        if sha256(file) != entry["sha256"]:
            raise FeatureError("Model sample checksum changed")
        pqfile = pq.ParquetFile(file)
        if pqfile.schema_arrow != model_schema() or pqfile.metadata.num_rows != entry["rows"]:
            raise FeatureError("Modelling sample schema/count mismatch")
    for split in TARGET_SPLITS:
        expected = sum(m["model_counts"][split].values())
        if expected != m["model_files"][split]["rows"]:
            raise FeatureError("Model split reconciliation failed")
    if check_data_values:
        ledgers, reconstructed_events = load_events(upstream)
        if reconstructed_events != m["source_event_audit"]:
            raise FeatureError("Failure resolution audit differs from source")
        history: dict[int, dict[int, dict[str, Any]]] = defaultdict(dict)
        row_counts: Counter[tuple[str,str]] = Counter()
        model_iters = {s: _iter_parquet_rows(output / "model_data" / f"{s}.parquet") for s in TARGET_SPLITS}
        for src, entry in zip(upstream["partitions"], parts):
            src_path = Path(upstream["quality_dir"]) / src["file"]
            if sha256(src_path) != src["sha256"]:
                raise FeatureError("Source panel changed during independent verification")
            # Read physical Parquet fields only. The Dataset-based read_table()
            # can add Hive partition keys (year, quarter) from parent folders,
            # which are not part of the feature/label schema being verified.
            source_rows = pq.ParquetFile(src_path).read().to_pylist()
            actual = pq.ParquetFile(output / entry["file"]).read().to_pylist()
            if len(source_rows) != len(actual):
                raise FeatureError("Feature quarter changed row count")
            for original, candidate in zip(source_rows, actual):
                cert = int(original["CERT"])
                q = original["REPDTE"]
                memo = history[cert]
                expected_features = calculate_features(original, memo)
                qid = quarter_index(q)
                memo[qid] = {"ASSET": original.get("ASSET"),
                             "DEP": original.get("DEP"), "ROA": original.get("ROA")}
                for stale in tuple(memo):
                    if stale < qid-4:
                        del memo[stale]
                y, reason = label_at_quarter(q, ledgers[cert])
                split = split_name(q)
                expected = {"CERT": cert, "REPDTE": q,
                            "ASSUMED_AVAILABLE_FROM": original["ASSUMED_AVAILABLE_FROM"],
                            **expected_features, TARGET_COLUMN: y,
                            "LABEL_STATUS": reason, "SPLIT": split}
                if candidate != expected:
                    missing = sorted(set(expected) - set(candidate))
                    unexpected = sorted(set(candidate) - set(expected))
                    different = [name for name in expected if name in candidate and candidate[name] != expected[name]]
                    raise FeatureError(
                        f"Feature/outcome values differ from source for CERT={cert}, REPDTE={q}; "
                        f"missing_fields={missing}, unexpected_fields={unexpected}, "
                        f"different_fields={different[:10]}"
                    )
                row_counts[(split, reason)] += 1
                if split in TARGET_SPLITS and y is not None:
                    model_row = next(model_iters[split], None)
                    expected_model = {"CERT": cert, "REPDTE": q,
                                      "ASSUMED_AVAILABLE_FROM": original["ASSUMED_AVAILABLE_FROM"],
                                      **expected_features, TARGET_COLUMN: y}
                    if model_row != expected_model:
                        raise FeatureError(f"Model split values differ at CERT={cert}, REPDTE={q}")
        for split, iterator in model_iters.items():
            if next(iterator, None) is not None:
                raise FeatureError(f"Unexpected extra modelling records in {split}")
        recorded = {(x["split"],x["status"]):x["rows"] for x in m["label_status_counts"]}
        if dict(row_counts) != recorded:
            raise FeatureError("Label reason audit differs from independent recomputation")
    verdict = {
        "success": True, "experiment_id": upstream["experiment_id"],
        "code_commit": upstream["feature_code_commit"],
        "part4_manifest_sha256": sha256(output / "part4_manifest.json"),
        "part2_manifest_sha256": upstream["part2_manifest_sha256"],
        "quality_verification_sha256": upstream["quality_verification_sha256"],
        "processing_verification_sha256": upstream["processing_verification_sha256"],
        "feature_rows": m["source_rows"], "feature_count": len(FEATURE_NAMES),
        "verified_quarters": len(parts),
        "development_rows": m["model_files"]["development"]["rows"],
        "test_holdout_rows": m["model_files"]["test_holdout"]["rows"],
        "development_positive_bank_quarters": m["model_counts"]["development"]["positive"],
        "test_positive_bank_quarters": m["model_counts"]["test_holdout"]["positive"],
        "distinct_positive_development_certs": m["distinct_certs_with_positive_labels"]["development"],
        "distinct_positive_test_certs": m["distinct_certs_with_positive_labels"]["test_holdout"],
        "actual_test_failure_source_rows": m["source_event_audit"]["test_2008_2014_failure_source_rows"],
        "actual_test_distinct_failure_certs_in_panel": m["source_event_audit"]["test_2008_2014_distinct_failure_certs_in_panel"],
        "assistance_excluded_from_failure_label": True,
        "train_only_preprocessing_fit": True,
        "historical_publication_times_verified": False,
        "audit_artifacts_verified": len(AUDIT_FILES),
        "read_only_value_verification": check_data_values,
    }
    # A public verdict never depends on whether verification was repeated in
    # an abbreviated mode: the committed stage must have full verification.
    verdict["read_only_value_verification"] = True
    if compare_published and _read_json(output / "part4_validation.json") != verdict:
        raise FeatureError("Published verdict differs from independent verification")
    return verdict


def _iter_parquet_rows(path: Path, batch_size: int = 20000):
    import pyarrow.parquet as pq
    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
        yield from batch.to_pylist()


def build_project_stage(root: Path, code_commit: str, *, replace_incomplete: bool = False) -> dict[str, Any]:
    from .project_flow import HandoffError, publish_stage, resolve_stage, stage_dir
    snap = source_contract(root, verify_hashes=True)
    snap["feature_code_commit"] = code_commit
    output = stage_dir(root, "features")
    try:
        published = resolve_stage(root, "features")
    except HandoffError as exc:
        if "has not passed verification" not in str(exc):
            raise
        published = None
    if published is not None:
        verdict = verify_feature_stage(output, snap)
        if sha256(output / "part4_validation.json") != published["verification_sha256"]:
            raise FeatureError("Published Part 4 validation hash changed")
        LOG.info("Previously published Part 4 verified without changes")
        return verdict
    if output.exists():
        if not replace_incomplete or not output.is_dir() or output.is_symlink():
            raise FeatureError("Unpublished output exists; use --replace-incomplete")
        shutil.rmtree(output)
    ledgers, events = load_events(snap)
    verdict = build_feature_tables(
        snap["partitions"], Path(snap["quality_dir"]), output,
        ledgers, events, code_commit=code_commit, upstream=snap)
    # Verify every derived feature, label and split against the original panel.
    verdict = verify_feature_stage(output, snap, compare_published=True, check_data_values=True)
    published = publish_stage(root, "features", "part4_validation.json")
    if published["verification_sha256"] != sha256(output / "part4_validation.json"):
        raise FeatureError("Feature stage registry verification does not match file")
    return verdict


def verify_project_stage(root: Path) -> dict[str, Any]:
    from .project_flow import resolve_stage
    snap = source_contract(root, verify_hashes=True)
    stage = resolve_stage(root, "features")
    manifest = _read_json(Path(stage["path"]) / "part4_manifest.json")
    snap["feature_code_commit"] = manifest["code_commit"]
    verdict = verify_feature_stage(Path(stage["path"]), snap, check_data_values=True)
    if stage["verification_sha256"] != sha256(Path(stage["path"]) / "part4_validation.json"):
        raise FeatureError("Feature registry digest mismatch")
    return verdict


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FDIC feature engineering and outcome construction")
    actions = parser.add_subparsers(dest="action", required=True)
    b = actions.add_parser("build")
    b.add_argument("--drive-root", type=Path, required=True)
    b.add_argument("--code-commit", required=True)
    b.add_argument("--replace-incomplete", action="store_true")
    v = actions.add_parser("verify")
    v.add_argument("--drive-root", type=Path, required=True)
    for cmd in (b, v):
        cmd.add_argument("--log-file", type=Path)
    args = parser.parse_args(argv)
    handlers = [logging.StreamHandler()]
    if args.log_file:
        args.log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = (build_project_stage(args.drive_root, args.code_commit,
                                  replace_incomplete=args.replace_incomplete)
              if args.action == "build" else verify_project_stage(args.drive_root))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0

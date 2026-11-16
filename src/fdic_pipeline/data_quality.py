"""Deterministic quality and terminal-event policies for FDIC bank-quarter data.

All row-level flags use information in that financial report only. Retrospective
failure and institution records are processed separately and never joined to
predictor rows. Monetary quantities are USD thousands; reported ratios are
percentages. Flags identify observations for review, not automatic corrections.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping

QUALITY_RULES = (
    "DQ_NEGATIVE_DEPOSITS",
    "DQ_DEPOSITS_GT_ASSETS",
    "DQ_NEGATIVE_GROSS_LOANS",
    "DQ_GROSS_LOANS_GT_ASSETS",
    "DQ_NONCURRENT_GT_GROSS_LOANS",
    "DQ_NEGATIVE_EQUITY",
    "DQ_NEGATIVE_CASH_BALANCES",
    "DQ_CASH_GT_ASSETS",
    "DQ_EXTREME_ROA",
    "DQ_EXTREME_ROE",
)

TERMINAL_CLASSES = (
    "confirmed_failure",
    "documented_other_exit",
    "right_censored",
    "unresolved_discontinuation",
    "inconsistent_event_timing",
    "ambiguous_failure_events",
    "ambiguous_failure_evidence",
    "inconsistent_institution_end_date",
    "unparseable_institution_end_date",
)

# These are prespecified analytical windows, not modelling sample selections.
ILLUSTRATIVE_PERIODS = (
    ("1984Q1–2006Q4", date(1984, 1, 1), date(2006, 12, 31)),
    ("2008Q1–2014Q4", date(2008, 1, 1), date(2014, 12, 31)),
)


def finite_number(value: Any) -> bool:
    """Treat None, NaN, infinity and booleans as missing numeric evidence."""
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(float(value)))


def asset_exclusion_reason(assets: Any) -> str | None:
    """The sole row-level exclusion is an unusable core exposure denominator."""
    if not finite_number(assets):
        return "missing_or_nonfinite_assets"
    if float(assets) <= 0:
        return "nonpositive_assets"
    return None


def screening_flags(record: Mapping[str, Any]) -> dict[str, bool]:
    """Non-mutating, current-report-only screening; unusual values are retained."""
    a, d, l = (record.get(n) for n in ("ASSET", "DEP", "LNLSGR"))
    n, e, c = (record.get(nm) for nm in ("NCLNLS", "EQ", "CHBAL"))
    roa, roe = record.get("ROA"), record.get("ROE")
    result = {
        "DQ_NEGATIVE_DEPOSITS": finite_number(d) and d < 0,
        "DQ_DEPOSITS_GT_ASSETS": finite_number(d) and finite_number(a) and a > 0 and d > a,
        "DQ_NEGATIVE_GROSS_LOANS": finite_number(l) and l < 0,
        "DQ_GROSS_LOANS_GT_ASSETS": finite_number(l) and finite_number(a) and a > 0 and l > a,
        "DQ_NONCURRENT_GT_GROSS_LOANS": finite_number(n) and finite_number(l) and l >= 0 and n > l,
        "DQ_NEGATIVE_EQUITY": finite_number(e) and e < 0,
        "DQ_NEGATIVE_CASH_BALANCES": finite_number(c) and c < 0,
        "DQ_CASH_GT_ASSETS": finite_number(c) and finite_number(a) and a > 0 and c > a,
        "DQ_EXTREME_ROA": finite_number(roa) and abs(roa) > 100,
        "DQ_EXTREME_ROE": finite_number(roe) and abs(roe) > 100,
    }
    assert tuple(result) == QUALITY_RULES
    return result


def parse_source_date(raw: Any) -> date | None:
    """Parse known FDIC date encodings; report unrecognized values explicitly."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, bool):
        raise ValueError(f"Invalid date value: {raw!r}")
    value = str(raw).strip()
    if value in {"0", "00000000", "99999999", "9999-12-31"}:
        return None
    # Allow an ISO timestamp with a numeric UTC offset or a Z suffix.
    if "T" in value or (" " in value and value[:4].isdigit()):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except ValueError:
            pass
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognized FDIC date encoding: {raw!r}")


def quarter_ordinal(day: date) -> int:
    return 4 * day.year + (day.month - 1) // 3


def omitted_quarters(previous: date | None, current: date) -> int | None:
    """Count missing intermediate quarters, excluding both observed endpoints."""
    if previous is None:
        return None
    distance = quarter_ordinal(current) - quarter_ordinal(previous)
    if distance <= 0:
        raise ValueError("Financial reports for one CERT are not strictly increasing")
    return distance - 1


def assumed_available_date(repdte: date, lag_days: int = 90) -> date:
    """An explicit scenario, not an observed historical publication timestamp."""
    if not isinstance(lag_days, int) or not 0 <= lag_days <= 366:
        raise ValueError("Availability lag must be an integer from 0 through 366 days")
    return repdte + timedelta(days=lag_days)


def classify_terminal_event(
    last_report: date,
    study_end: date,
    dated_failures: Iterable[date],
    institution_end: date | None,
    *,
    unparseable_or_undated_failure: bool = False,
    unparseable_institution_end: bool = False,
) -> tuple[str, date | None, str]:
    """Retrospective CERT-level classification, never an observed feature.

    Multiple distinct in-window failure dates or undated/unparseable linked
    events are ambiguous. Missing institution end evidence is NOT a merger.
    """
    within = sorted({d for d in dated_failures if d is not None and d <= study_end})
    if unparseable_or_undated_failure:
        return ("ambiguous_failure_evidence", None,
                "CERT-linked failure record has a missing or unparseable date")
    if len(within) > 1:
        return ("ambiguous_failure_events", None,
                "Multiple distinct in-window FAILDATE values for the same CERT")
    if within and within[0] < last_report:
        return ("inconsistent_event_timing", within[0],
                "Dated failure precedes the last financial report")
    if within:
        return ("confirmed_failure", within[0],
                "CERT-linked failure observed by the study end")
    if unparseable_institution_end:
        return ("unparseable_institution_end_date", None,
                "Institution end date has an unrecognized source encoding")
    if institution_end is not None and institution_end < last_report:
        return ("inconsistent_institution_end_date", institution_end,
                "Institution end date precedes the last financial report")
    if institution_end is not None and last_report <= institution_end <= study_end:
        return ("documented_other_exit", institution_end,
                "Dated institution end, with no in-window CERT-linked failure")
    if last_report >= study_end:
        return ("right_censored", None,
                "Bank has a report in the final study quarter")
    return ("unresolved_discontinuation", None,
            "Financial reporting ends without a dated, linked exit event")


def count_null_numerics(record: Mapping[str, Any], fields: Iterable[str]) -> int:
    return sum(record.get(f) is None or not finite_number(record.get(f)) for f in fields)

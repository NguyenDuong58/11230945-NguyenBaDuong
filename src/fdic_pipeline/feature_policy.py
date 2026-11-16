"""Prespecified FDIC four-quarter outcome and historical bank-risk feature policy.

Financial predictors use current or preceding bank-quarter reports. The Failures
and Institutions collections are used only for outcomes, eligibility and audits.
Financial amounts are USD thousands; source ratio fields are percentages.
"""
from __future__ import annotations

import calendar
import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Mapping

STUDY_END = date(2026, 6, 30)
LAST_LABEL_QUARTER = date(2025, 6, 30)
DEVELOPMENT_END = date(2006, 12, 31)
PURGE_START = date(2007, 3, 31)
PURGE_END = date(2007, 12, 31)
TEST_START = date(2008, 3, 31)
TEST_END = date(2014, 12, 31)
AVAILABILITY_LAG_DAYS = 90
AVAILABILITY_SENSITIVITY = (30, 90, 180)
TARGET_COLUMN = "Y_FAIL_4Q"
SEED = 11230945

FEATURE_SPECS = (
    ("LOG_ASSET", "size", "ln(1 + ASSET)", "Current report"),
    ("EQUITY_ASSETS_PCT", "capital", "100 × EQ / ASSET", "Current report"),
    ("DEPOSITS_ASSETS_PCT", "funding", "100 × DEP / ASSET", "Current report"),
    ("CASH_ASSETS_PCT", "liquidity", "100 × CHBAL / ASSET", "Current report"),
    ("SECURITIES_ASSETS_PCT", "liquidity", "100 × SC / ASSET", "Current report"),
    ("LOANS_ASSETS_PCT", "credit", "100 × LNLSGR / ASSET", "Current report"),
    ("BROKERED_DEPOSITS_PCT", "funding", "100 × BRO / DEP", "Current report"),
    ("NONCURRENT_LOANS_PCT", "credit", "100 × NCLNLS / LNLSGR", "Current report"),
    ("ROA_PCT", "earnings", "ROA", "Current report"),
    ("ROE_PCT", "earnings", "ROE", "Current report"),
    ("NIM_PCT", "earnings", "NIMY", "Current report"),
    ("TIER1_LEVERAGE_PCT", "capital", "RBC1AAJ", "Current report"),
    ("TOTAL_RISK_CAPITAL_PCT", "capital", "RBCRWAJ", "Current report"),
    ("ASSET_GROWTH_QOQ_PCT", "growth", "100 × (ASSET[q] / ASSET[q-1] − 1)", "Exact preceding quarter"),
    ("DEPOSIT_GROWTH_QOQ_PCT", "growth", "100 × (DEP[q] / DEP[q-1] − 1)", "Exact preceding quarter"),
    ("ASSET_GROWTH_YOY_PCT", "growth", "100 × (ASSET[q] / ASSET[q-4] − 1)", "Exact year-earlier quarter"),
    ("ROA_CHANGE_YOY_PPT", "earnings", "ROA[q] − ROA[q-4]", "Exact year-earlier quarter"),
    ("PRIOR_REPORT_COUNT", "history", "Prior observed reports for CERT", "Earlier reports only"),
    ("REPORT_GAP_QUARTERS", "history", "Absent quarters before current report", "Earlier reports only"),
    ("DQ_EXTREME_ROE", "quality", "Current-report ROE screening flag", "Current report"),
    ("MISSING_FINANCIAL_COUNT", "quality", "Current-report missing numeric field count", "Current report"),
)
FEATURE_NAMES = tuple(entry[0] for entry in FEATURE_SPECS)

FEATURE_RATIONALES = {
    "LOG_ASSET": "Institution scale and balance-sheet complexity",
    "EQUITY_ASSETS_PCT": "Book-capital loss-absorption capacity",
    "DEPOSITS_ASSETS_PCT": "Reliance on deposit funding",
    "CASH_ASSETS_PCT": "Immediate liquidity buffer relative to assets",
    "SECURITIES_ASSETS_PCT": "Asset allocation and potential liquid holdings",
    "LOANS_ASSETS_PCT": "Exposure to credit intermediation",
    "BROKERED_DEPOSITS_PCT": "Potential funding sensitivity to noncore deposits",
    "NONCURRENT_LOANS_PCT": "Observed deterioration of loan-portfolio performance",
    "ROA_PCT": "Profitability relative to assets",
    "ROE_PCT": "Profitability relative to shareholder equity",
    "NIM_PCT": "Net interest profitability of intermediation",
    "TIER1_LEVERAGE_PCT": "Regulatory capital relative to unweighted exposures",
    "TOTAL_RISK_CAPITAL_PCT": "Regulatory capital relative to risk-weighted assets",
    "ASSET_GROWTH_QOQ_PCT": "Short-term balance-sheet expansion or contraction",
    "DEPOSIT_GROWTH_QOQ_PCT": "Quarterly funding inflows or withdrawals",
    "ASSET_GROWTH_YOY_PCT": "Medium-term balance-sheet dynamics",
    "ROA_CHANGE_YOY_PPT": "Year-over-year changes in profitability",
    "PRIOR_REPORT_COUNT": "Observed institutional reporting history",
    "REPORT_GAP_QUARTERS": "Historical reporting continuity and coverage",
    "DQ_EXTREME_ROE": "Current-quarter earnings-ratio screening indicator",
    "MISSING_FINANCIAL_COUNT": "Completeness of contemporaneous financial reporting",
}
if tuple(FEATURE_RATIONALES) != FEATURE_NAMES:
    raise AssertionError("Feature rationales differ from the ordered feature specification")

EXCLUDED_PREDICTOR_FIELDS = frozenset({
    "CERT", "REPDTE", "ASSUMED_AVAILABLE_FROM", "FAILDATE", "FAILYR", "COST",
    "RESTYPE", "ACTIVE", "CHANGEC1", "ENDEFYMD", "TERMINAL_CLASS",
    "TERMINAL_EVENT_DATE", "EVENT_DATE", "LABEL", "Y", TARGET_COLUMN,
})


def quarter_index(day: date) -> int:
    if day.month not in (3, 6, 9, 12) or day.day != calendar.monthrange(day.year, day.month)[1]:
        raise ValueError(f"Not a calendar quarter-end: {day!r}")
    return 4 * day.year + (day.month // 3 - 1)


def add_quarters(day: date, count: int = 4) -> date:
    idx = quarter_index(day) + count
    year, q = divmod(idx, 4)
    month = 3 * (q + 1)
    return date(year, month, calendar.monthrange(year, month)[1])


def split_name(day: date) -> str:
    quarter_index(day)
    if day > LAST_LABEL_QUARTER:
        return "label_immature"
    if day <= DEVELOPMENT_END:
        return "development"
    if day <= PURGE_END:
        return "purge_gap"
    if day <= TEST_END:
        return "test_holdout"
    return "unused_post_test"


def finite(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        x = float(v)
    except (ValueError, TypeError, OverflowError):
        return None
    return x if math.isfinite(x) else None


def ratio(num: Any, den: Any) -> float | None:
    n, d = finite(num), finite(den)
    if n is None or d is None or d <= 0:
        return None
    return finite(100.0 * n / d)


def growth(now: Any, prev: Any) -> float | None:
    n, p = finite(now), finite(prev)
    if n is None or p is None or p <= 0:
        return None
    return finite(100.0 * (n / p - 1))


def calculate_features(current: Mapping[str, Any], past: Mapping[int, Mapping[str, Any]]) -> dict[str, float | None]:
    """Exactly specified quarter lags; no forward filling across missing reports."""
    q = quarter_index(current["REPDTE"])
    p1, p4 = past.get(q-1, {}), past.get(q-4, {})
    assets = current.get("ASSET")
    asset_value = finite(assets)
    result: dict[str, float | None] = {
        "LOG_ASSET": math.log1p(asset_value) if asset_value is not None and asset_value > 0 else None,
        "EQUITY_ASSETS_PCT": ratio(current.get("EQ"), assets),
        "DEPOSITS_ASSETS_PCT": ratio(current.get("DEP"), assets),
        "CASH_ASSETS_PCT": ratio(current.get("CHBAL"), assets),
        "SECURITIES_ASSETS_PCT": ratio(current.get("SC"), assets),
        "LOANS_ASSETS_PCT": ratio(current.get("LNLSGR"), assets),
        "BROKERED_DEPOSITS_PCT": ratio(current.get("BRO"), current.get("DEP")),
        "NONCURRENT_LOANS_PCT": ratio(current.get("NCLNLS"), current.get("LNLSGR")),
        "ROA_PCT": finite(current.get("ROA")),
        "ROE_PCT": finite(current.get("ROE")),
        "NIM_PCT": finite(current.get("NIMY")),
        "TIER1_LEVERAGE_PCT": finite(current.get("RBC1AAJ")),
        "TOTAL_RISK_CAPITAL_PCT": finite(current.get("RBCRWAJ")),
        "ASSET_GROWTH_QOQ_PCT": growth(assets, p1.get("ASSET")),
        "DEPOSIT_GROWTH_QOQ_PCT": growth(current.get("DEP"), p1.get("DEP")),
        "ASSET_GROWTH_YOY_PCT": growth(assets, p4.get("ASSET")),
        "ROA_CHANGE_YOY_PPT": (finite(current.get("ROA")) - finite(p4.get("ROA")))
            if finite(current.get("ROA")) is not None and finite(p4.get("ROA")) is not None else None,
        "PRIOR_REPORT_COUNT": finite(current.get("PRIOR_REPORT_COUNT")),
        "REPORT_GAP_QUARTERS": finite(current.get("REPORT_GAP_QUARTERS")),
        "DQ_EXTREME_ROE": float(bool(current.get("DQ_EXTREME_ROE"))),
        "MISSING_FINANCIAL_COUNT": finite(current.get("MISSING_FINANCIAL_COUNT")),
    }
    if tuple(result) != FEATURE_NAMES:
        raise AssertionError("Feature order differs from the documented feature dictionary")
    return result


@dataclass(frozen=True)
class EventLedger:
    """CERT-specific outcome evidence; never joined into predictor features."""
    last_report: date
    failure_day: date | None = None
    assistance_days: tuple[date, ...] = ()
    institution_end: date | None = None
    uncertain_reason: str | None = None


def label_at_quarter(day: date, event: EventLedger, lag_days: int = AVAILABILITY_LAG_DAYS) -> tuple[int | None, str]:
    """Study label 1{FAILURE in (q,q+4Q]}, with an availability eligibility gate.

    Other exits, interventions and uncertain follow-up are censored, never false
    positives or invented healthy outcomes. The horizon remains anchored at q.
    """
    quarter_index(day)
    if not isinstance(lag_days, int) or not 0 <= lag_days <= 366:
        raise ValueError("Availability lag must be between 0 and 366 calendar days")
    if day > LAST_LABEL_QUARTER or add_quarters(day, 4) > STUDY_END:
        return None, "label_immature"
    if event.uncertain_reason:
        return None, "uncertain_event_evidence"
    horizon = add_quarters(day, 4)
    available = day + timedelta(days=lag_days)
    if available > horizon:
        return None, "availability_after_horizon"
    if event.failure_day is not None and event.failure_day <= day:
        return None, "report_on_or_after_failure"
    if event.institution_end is not None and event.institution_end <= day:
        return None, "report_on_or_after_other_exit"
    future_assistance = [d for d in event.assistance_days if day < d <= horizon]
    future_exit = event.institution_end if event.institution_end and day < event.institution_end <= horizon else None
    future_failure = event.failure_day if event.failure_day and day < event.failure_day <= horizon else None
    candidates = [(d, "failure") for d in ([future_failure] if future_failure else [])]
    candidates += [(d, "assistance") for d in future_assistance]
    if future_exit:
        candidates.append((future_exit, "other_exit"))
    if candidates:
        earliest = min(d for d, _ in candidates)
        types = {kind for d, kind in candidates if d == earliest}
        if len(types) != 1:
            return None, "simultaneous_competing_events"
        kind = types.pop()
        if earliest <= available:
            return None, "event_before_assumed_availability"
        if kind == "failure":
            return 1, "failure_within_4q"
        return None, "competing_" + kind
    # A dated event after the horizon is direct evidence of survival until then.
    future_dated = ([event.failure_day] if event.failure_day else []) + list(event.assistance_days)
    if event.institution_end:
        future_dated.append(event.institution_end)
    if event.last_report >= horizon or any(d > horizon for d in future_dated):
        return 0, "observed_no_failure_4q"
    return None, "follow_up_incomplete"


def unfit_preprocessor():
    """Clone and fit separately inside each Part 5 development-only temporal fold."""
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import RobustScaler
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
        ("scaler", RobustScaler(quantile_range=(25.0, 75.0))),
    ])

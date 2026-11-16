"""Dependency-free tests for current-quarter screening and exit evidence."""
from datetime import date, datetime
import math
import pytest
from fdic_pipeline.data_quality import (
    QUALITY_RULES, TERMINAL_CLASSES, ILLUSTRATIVE_PERIODS, finite_number,
    asset_exclusion_reason, screening_flags, parse_source_date,
    omitted_quarters, assumed_available_date, classify_terminal_event,
    count_null_numerics,
)
from fdic_pipeline.panel import _event_counts


def test_screening_does_not_mutate_and_is_current_report_only():
    row = {"ASSET": 100.0, "DEP": 105.0, "LNLSGR": -1.0,
           "NCLNLS": 2.0, "EQ": -3.0, "CHBAL": 110.0,
           "ROA": 110.0, "ROE": -150.0}
    before = dict(row)
    flags = screening_flags(row)
    assert tuple(flags) == QUALITY_RULES
    assert all(isinstance(v, bool) for v in flags.values())
    assert flags["DQ_DEPOSITS_GT_ASSETS"]
    assert flags["DQ_NEGATIVE_GROSS_LOANS"]
    assert flags["DQ_NEGATIVE_EQUITY"]
    assert flags["DQ_CASH_GT_ASSETS"]
    assert flags["DQ_EXTREME_ROA"] and flags["DQ_EXTREME_ROE"]
    assert row == before


@pytest.mark.parametrize("value", [None, math.nan, math.inf, -math.inf, 0.0, -1.0, False, "1"])
def test_invalid_assets_are_quarantined(value):
    assert asset_exclusion_reason(value) is not None


@pytest.mark.parametrize("value", [0.01, 5, 1_000_000.0])
def test_positive_assets_are_retained(value):
    assert asset_exclusion_reason(value) is None


def test_zero_and_extreme_earnings_not_deleted():
    row = {"ASSET": 20.0, "DEP": 0.0, "LNLSGR": 2.0,
           "ROA": -120.0, "ROE": 300.0}
    assert asset_exclusion_reason(row["ASSET"]) is None
    flags = screening_flags(row)
    assert flags["DQ_EXTREME_ROA"] and flags["DQ_EXTREME_ROE"]
    assert flags["DQ_NEGATIVE_DEPOSITS"] is False


@pytest.mark.parametrize("raw,expected", [
    (date(2010, 3, 31), date(2010, 3, 31)),
    (datetime(2010, 3, 31, 14, 2), date(2010, 3, 31)),
    ("20100331", date(2010, 3, 31)),
    ("2010-03-31", date(2010, 3, 31)),
    ("03/31/2010", date(2010, 3, 31)),
    ("2010-03-31T00:00:00Z", date(2010, 3, 31)),
    ("0", None), (None, None), ("99999999", None),
])
def test_date_parser(raw, expected):
    assert parse_source_date(raw) == expected


@pytest.mark.parametrize("raw", ["2010-13-01", "19840199", "wut", True])
def test_bad_date_is_not_silently_replaced(raw):
    with pytest.raises(ValueError):
        parse_source_date(raw)


def test_reporting_gap_counts_exclude_observed_quarters():
    assert omitted_quarters(None, date(2020, 3, 31)) is None
    assert omitted_quarters(date(2020, 3, 31), date(2020, 6, 30)) == 0
    assert omitted_quarters(date(2020, 3, 31), date(2020, 12, 31)) == 2
    with pytest.raises(ValueError):
        omitted_quarters(date(2020, 6, 30), date(2020, 3, 31))


def test_lag_is_an_assumption_and_does_not_shift_repdte():
    quarter_end = date(2020, 3, 31)
    assert assumed_available_date(quarter_end, 90) == date(2020, 6, 29)
    assert quarter_end == date(2020, 3, 31)
    with pytest.raises(ValueError):
        assumed_available_date(quarter_end, -1)
    with pytest.raises(ValueError):
        assumed_available_date(quarter_end, 400)


def test_terminal_categorical_policy():
    end = date(2026, 6, 30)
    last = date(2010, 3, 31)
    f = date(2010, 6, 15)
    assert classify_terminal_event(last, end, [f], None)[0] == "confirmed_failure"
    assert classify_terminal_event(last, end, [], date(2010, 6, 30))[0] == "documented_other_exit"
    assert classify_terminal_event(last, end, [], None)[0] == "unresolved_discontinuation"
    assert classify_terminal_event(end, end, [], None)[0] == "right_censored"
    assert classify_terminal_event(last, end, [date(2010, 1, 1)], None)[0] == "inconsistent_event_timing"
    assert classify_terminal_event(last, end, [f, date(2010, 9, 30)], None)[0] == "ambiguous_failure_events"
    assert classify_terminal_event(last, end, [], date(2009, 12, 31))[0] == "inconsistent_institution_end_date"
    assert classify_terminal_event(last, end, [], None, unparseable_or_undated_failure=True)[0] == "ambiguous_failure_evidence"
    assert classify_terminal_event(last, end, [], None, unparseable_institution_end=True)[0] == "unparseable_institution_end_date"
    assert classify_terminal_event(last, end, [date(2027, 3, 31)], None)[0] == "unresolved_discontinuation"
    assert len(TERMINAL_CLASSES) == len(set(TERMINAL_CLASSES))


def test_failure_ambiguity_precedes_institution_end():
    category, event, _ = classify_terminal_event(
        date(2010, 3, 31), date(2026, 6, 30), [], date(2010, 9, 30),
        unparseable_or_undated_failure=True,
    )
    assert category == "ambiguous_failure_evidence" and event is None


def test_missing_field_count_distinguishes_zero_from_missing():
    row = {"ASSET": 0.0, "DEP": None, "ROA": math.nan}
    assert count_null_numerics(row, ("ASSET", "DEP", "ROA")) == 2


def test_event_counts_are_by_faildate_not_positive_labels():
    events = [
        {"CERT": 1, "date": date(2008, 1, 4)},
        {"CERT": 1, "date": date(2008, 1, 4)},
        {"CERT": 2, "date": date(2008, 5, 4)},
        {"CERT": None, "date": date(2009, 4, 1)},
        {"CERT": 3, "date": date(2015, 1, 1)},
        {"CERT": 5, "date": None},
    ]
    years, windows = _event_counts(events, {1, 3})
    assert next(r for r in years if r["YEAR"] == 2008)["DATED_FAILURE_SOURCE_ROWS"] == 3
    test = next(r for r in windows if r["PERIOD"] == "2008Q1–2014Q4")
    assert test["DATED_FAILURE_SOURCE_ROWS"] == 4
    assert test["DISTINCT_DATED_CERT"] == 2
    assert test["DISTINCT_DATED_CERT_IN_FINANCIALS"] == 1
    assert test["DATED_ROWS_MISSING_CERT"] == 1
    assert test["DISTINCT_CERT_DATE_PAIRS"] == 2
    assert len(ILLUSTRATIVE_PERIODS) == 2


def test_screening_does_not_look_at_future_fields():
    base = {"ASSET": 90, "DEP": 100, "LNLSGR": 10, "EQ": 1}
    a = screening_flags(base)
    b = screening_flags({**base, "FAILDATE": "2021-06-04", "ACTIVE": "0",
                         "TERMINAL_CLASS": "confirmed_failure"})
    assert a == b


def test_financial_source_predictor_schema_has_no_outcome_columns():
    from fdic_pipeline.panel import EXCLUDED_PREDICTOR_FIELDS, PANEL_FIELDS
    assert not set(PANEL_FIELDS) & EXCLUDED_PREDICTOR_FIELDS


def test_automated_figures_and_findings_are_materialized(tmp_path):
    from collections import Counter
    from fdic_pipeline.panel import _figures, _findings_md, FIGURES
    missing = [
        {"FIELD": "ROA", "MISSING_PCT": 25.0},
        {"FIELD": "ASSET", "MISSING_PCT": 0.0},
    ]
    coverage = [
        {"QUARTER": "19840331", "RAW_ROWS": 5, "PANEL_ROWS": 4},
        {"QUARTER": "19840630", "RAW_ROWS": 6, "PANEL_ROWS": 6},
    ]
    _figures(tmp_path, missing, coverage, Counter({"DQ_EXTREME_ROA": 3}),
             Counter({"right_censored": 6, "confirmed_failure": 1}))
    for rel in FIGURES:
        content = (tmp_path / rel).read_bytes()
        assert content.startswith(b"\x89PNG\r\n\x1a\n") and len(content) > 1000
    summary = _findings_md(raw_rows=11, panel_rows=10, quarantined=1,
                           gaps=0, missing_fields=missing,
                           terminals=Counter({"right_censored": 6}),
                           source={"manifest_sha256": "a" * 64},
                           lag_days=90, storage_digest="b" * 64)
    assert "10 + 1 = 11" in summary
    assert "unverified release-lag assumption" in summary
    assert "right_censored" in summary

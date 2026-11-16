"""Parquet layout protocol and fraud-resistant fragment-accounting tests."""
from __future__ import annotations
from datetime import date
import pytest
from fdic_pipeline import layout_benchmark as m


def _sample(workload, layout):
    return {"workload": workload, "layout": layout, "rows": 28, "cert_sum": 91,
            "asset_sum": 120.125, "total_fragments": 1 if layout == "flat" else 170,
            "selected_fragments": (1 if layout == "flat" else
                {"full_projection": 170, "one_quarter": 1, "one_year": 4}[workload])}


@pytest.mark.parametrize("workload", m.WORKLOADS)
def test_expected_fragment_selection(workload):
    m.check_comparable(_sample(workload, "flat"), _sample(workload, "partitioned"))


def test_date_filters_exact():
    assert m.workload_dates("full_projection") == (None, None)
    assert m.workload_dates("one_quarter") == (date(2010, 12, 31), date(2010, 12, 31))
    assert m.workload_dates("one_year") == (date(2010, 1, 1), date(2010, 12, 31))
    with pytest.raises(m.BenchmarkError):
        m.workload_dates("unknown")


@pytest.mark.parametrize("field,value", [( "rows", 99), ("cert_sum", 99),
                                         ("asset_sum", 500.0),
                                         ("total_fragments", 169),
                                         ("selected_fragments", 2)])
def test_comparison_rejects_invalid_results(field, value):
    a = _sample("one_quarter", "flat")
    b = _sample("one_quarter", "partitioned")
    b[field] = value
    with pytest.raises(m.BenchmarkError):
        m.check_comparable(a, b)


def test_plot_external_legend_and_chart(tmp_path):
    summary = [{"workload": workload, "layout": layout, "median_seconds": 0.3 + i/10}
               for i, (workload, layout) in enumerate((w, f) for w in m.WORKLOADS for f in m.LAYOUTS)]
    m._plot(tmp_path, summary)
    assert (tmp_path / "layout_scan_times.png").stat().st_size > 1000

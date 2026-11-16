"""Deterministic processing-policy tests, independent of installed big-data engines."""
from datetime import date
import math

import pytest
from fdic_pipeline.processing_engines import (
    ENGINE_NAMES, WORKLOADS, RESULT_COLUMNS, PARQUET_COLS,
    ProcessingError, compare_results, normalise, pandas_result, run_engine,
)
from fdic_pipeline.scalable_processing import (
    BenchmarkError, _summaries, _safe_stage_partitions, _now,
)
from fdic_pipeline.acquisition import DEFAULT_START, DEFAULT_END, quarter_ends


def example(workload="quarter_aggregation"):
    if workload == "quarter_aggregation":
        return [{"REPDTE":"2001-03-31","BANK_QUARTERS":2,"TOTAL_ASSETS":100.0,
                 "TOTAL_DEPOSITS":60.0,"MEAN_ROA":1.1,"EXTREME_ROE_COUNT":1}]
    if workload == "adjacent_asset_growth":
        return [{"REPDTE":"2001-03-31","BANK_QUARTERS":2,
                 "CONSECUTIVE_PAIRS":1,"MEAN_ASSET_GROWTH_PCT":0.12}]
    return [{"YEAR":2001,"STNAME":"AL","BANK_QUARTERS":2,
             "TOTAL_ASSETS":100.0,"TOTAL_GROSS_LOANS":50.0,"MEAN_ROA":None,
             "NEGATIVE_EQUITY_COUNT":1,"EXCESS_LOAN_COUNT":0,"MISSING_FINANCIAL_ROWS":2}]


@pytest.mark.parametrize("workload", WORKLOADS)
def test_exact_results_and_floating_point_tolerance(workload):
    values = example(workload)
    assert compare_results(values, values, workload)["rows_compared"] == 1
    numeric = next(k for k in RESULT_COLUMNS[workload] if k in ("TOTAL_ASSETS","MEAN_ASSET_GROWTH_PCT"))
    changed = [dict(values[0])]
    changed[0][numeric] += 1e-10
    compare_results(values, changed, workload)
    changed[0][numeric] = 50000.0
    with pytest.raises(ProcessingError, match="Non-equivalent"):
        compare_results(values, changed, workload)


@pytest.mark.parametrize("workload", WORKLOADS)
def test_reject_wrong_schema(workload):
    wrong = example(workload)
    del wrong[0][next(iter(wrong[0]))]
    with pytest.raises(ProcessingError, match="Unexpected result columns"):
        normalise(wrong, workload)


def test_null_mean_must_match_null_mean():
    a = example("state_year_risk")
    b = [dict(a[0], MEAN_ROA=0.0)]
    with pytest.raises(ProcessingError, match="Non-equivalent"):
        compare_results(a, b, "state_year_risk")


def test_integer_counts_cannot_vary_or_be_null():
    a = example()
    b = [dict(a[0], BANK_QUARTERS=3)]
    with pytest.raises(ProcessingError):
        compare_results(a, b, "quarter_aggregation")
    b[0]["BANK_QUARTERS"] = None
    with pytest.raises(ProcessingError):
        normalise(b, "quarter_aggregation")


def test_duplicate_grouping_key_rejected():
    with pytest.raises(ProcessingError, match="Repeated result grouping key"):
        normalise(example() + example(), "quarter_aggregation")


def test_invalid_date_rejected():
    with pytest.raises(ProcessingError, match="Invalid REPDTE"):
        normalise([dict(example()[0], REPDTE="2001-02-31")], "quarter_aggregation")


def test_bad_engine_or_workload_fails_closed():
    with pytest.raises(ProcessingError):
        run_engine("not-an-engine", "/tmp/nowhere", "quarter_aggregation")
    with pytest.raises(ProcessingError):
        normalise([], "not-a-workload")


def test_result_definitions_do_not_reference_retrospective_event_fields():
    banned = {"FAILDATE", "COST", "ACTIVE", "CHANGEC1", "TERMINAL_CLASS", "Y"}
    for workload in WORKLOADS:
        assert banned.isdisjoint(PARQUET_COLS[workload])
    assert len(ENGINE_NAMES) == 3 and len(WORKLOADS) == 3


def test_pandas_adjacent_quarter_does_not_bridge_gaps(monkeypatch):
    import pandas as pd
    rows = [
        {"CERT":1, "REPDTE":date(2001,3,31),"ASSET":100.0},
        {"CERT":1, "REPDTE":date(2001,6,30),"ASSET":110.0},
        {"CERT":1, "REPDTE":date(2001,12,31),"ASSET":200.0},
        {"CERT":2, "REPDTE":date(2001,6,30),"ASSET":50.0},
    ]
    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: pd.DataFrame(rows))
    result = pandas_result("/unused", "adjacent_asset_growth")
    assert result[0]["CONSECUTIVE_PAIRS"] == 0
    assert result[1]["CONSECUTIVE_PAIRS"] == 1
    assert result[1]["MEAN_ASSET_GROWTH_PCT"] == pytest.approx(10.0)
    assert result[2]["CONSECUTIVE_PAIRS"] == 0
    assert result[2]["MEAN_ASSET_GROWTH_PCT"] is None


def test_pandas_state_year_nonexclusive_flags(monkeypatch):
    import pandas as pd
    records = [
        {"CERT":1,"REPDTE":date(2001,3,31),"STNAME":None,"ASSET":100.0,"LNLSGR":50.0,
         "ROA":None,"DQ_NEGATIVE_EQUITY":True,"DQ_GROSS_LOANS_GT_ASSETS":True,
         "MISSING_FINANCIAL_COUNT":1},
        {"CERT":2,"REPDTE":date(2001,6,30),"STNAME":None,"ASSET":200.0,"LNLSGR":None,
         "ROA":None,"DQ_NEGATIVE_EQUITY":False,"DQ_GROSS_LOANS_GT_ASSETS":False,
         "MISSING_FINANCIAL_COUNT":0},
    ]
    monkeypatch.setattr(pd, "read_parquet", lambda *a,**k: pd.DataFrame(records))
    result = pandas_result("/unused", "state_year_risk")
    assert result == [{"YEAR":2001,"STNAME":"MISSING","BANK_QUARTERS":2,
                        "TOTAL_ASSETS":300.0,"TOTAL_GROSS_LOANS":50.0,
                        "MEAN_ROA":None,"NEGATIVE_EQUITY_COUNT":1,"EXCESS_LOAN_COUNT":1,
                        "MISSING_FINANCIAL_ROWS":1}]


def test_recompute_medians_and_speedups():
    records=[]
    for sample in ("recent_85","full_170"):
        for workload in WORKLOADS:
            for engine,base in (("pandas",2.0),("polars",1.0),("pyspark",4.0)):
                for iteration in range(3):
                    records.append({"sample":sample,"workload":workload,"engine":engine,
                                    "iteration":iteration,"warmup":False,
                                    "elapsed_seconds":base + iteration * .1,
                                    "peak_process_tree_rss_mib":123.0,"output_rows":2})
    summary,scale=_summaries(records)
    assert len(summary)==18 and len(scale)==9
    entry=next(r for r in summary if r["engine"]=="polars")
    assert entry["pandas_over_engine_speedup"]==pytest.approx(2.1/1.1)


def test_metadata_inventory_rejects_missing_quarter(monkeypatch, tmp_path):
    import fdic_pipeline.panel as panel_module
    class Dummy:
        names=["CERT","REPDTE"]
    monkeypatch.setattr(panel_module,"panel_schema",lambda: Dummy())
    quarters=quarter_ends(DEFAULT_START,DEFAULT_END)
    rows=[]
    for q in quarters:
        year=q[:4]; number=(int(q[4:6])-1)//3+1
        rows.append({"quarter":q,"file":f"panel/year={year}/quarter={number}/part-00000.parquet",
                     "rows":1,"sha256":"a"*64,"size_bytes":100})
    manifest={"expected_quarters":170,"scope":{"start":quarters[0],"end":quarters[-1]},
              "panel_fields":Dummy.names,"lag_is_verified_publication":False,
              "partitions":rows,"panel_rows":170,"quarantined_rows":0,"raw_rows":170}
    from fdic_pipeline.scalable_processing import _safe_stage_partitions
    result,total=_safe_stage_partitions(manifest,tmp_path)
    assert total==170 and len(result)==170
    manifest["partitions"].pop()
    with pytest.raises(BenchmarkError,match="170 quarter"):
        _safe_stage_partitions(manifest,tmp_path)

"""Real pandas, Polars and Spark execution on one synthetic quarter-partitioned panel."""
from datetime import date
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pl = pytest.importorskip("polars")
pyspark = pytest.importorskip("pyspark")

from fdic_pipeline.panel import panel_schema
from fdic_pipeline.processing_engines import (
    WORKLOADS, compare_results, pandas_result, polars_result, spark_result,
    spark_session, plan_text,
)


def make_panel(tmp_path: Path) -> Path:
    panel=tmp_path/"panel"
    sample=[
       (1984,1,[
           {"CERT":1,"REPDTE":date(1984,3,31),"ASSET":100.0,"DEP":70.0,"LNLSGR":80.0,
            "ROA":1.0,"STNAME":"AL", "DQ_EXTREME_ROE":False, "DQ_NEGATIVE_EQUITY":False,
            "DQ_GROSS_LOANS_GT_ASSETS":False, "MISSING_FINANCIAL_COUNT":0},
           {"CERT":2,"REPDTE":date(1984,3,31),"ASSET":50.0,"DEP":None,"LNLSGR":None,
            "ROA":None,"STNAME":None,"DQ_EXTREME_ROE":True,"DQ_NEGATIVE_EQUITY":True,
            "DQ_GROSS_LOANS_GT_ASSETS":False,"MISSING_FINANCIAL_COUNT":2},
       ]),
       (1984,2,[
           {"CERT":1,"REPDTE":date(1984,6,30),"ASSET":110.0,"DEP":75.0,"LNLSGR":87.0,
            "ROA":1.3,"STNAME":"AL","DQ_EXTREME_ROE":False,"DQ_NEGATIVE_EQUITY":False,
            "DQ_GROSS_LOANS_GT_ASSETS":False,"MISSING_FINANCIAL_COUNT":0},
       ]),
       (1984,4,[
           {"CERT":1,"REPDTE":date(1984,12,31),"ASSET":150.0,"DEP":92.0,"LNLSGR":161.0,
            "ROA":-0.8,"STNAME":"AL","DQ_EXTREME_ROE":True,"DQ_NEGATIVE_EQUITY":True,
            "DQ_GROSS_LOANS_GT_ASSETS":True,"MISSING_FINANCIAL_COUNT":0},
       ]),
       (2000,1,[
           {"CERT":3,"REPDTE":date(2000,3,31),"ASSET":40.0,"DEP":20.0,"LNLSGR":30.0,
            "ROA":2.0,"STNAME":"DC","DQ_EXTREME_ROE":False,"DQ_NEGATIVE_EQUITY":False,
            "DQ_GROSS_LOANS_GT_ASSETS":False,"MISSING_FINANCIAL_COUNT":1},
       ]),
       (2000,2,[
           {"CERT":3,"REPDTE":date(2000,6,30),"ASSET":50.0,"DEP":30.0,"LNLSGR":45.0,
            "ROA":None,"STNAME":"DC","DQ_EXTREME_ROE":True,"DQ_NEGATIVE_EQUITY":False,
            "DQ_GROSS_LOANS_GT_ASSETS":False,"MISSING_FINANCIAL_COUNT":1},
       ]),
    ]
    schema=panel_schema()
    for year,quarter,records in sample:
        dest=panel/f"year={year}"/f"quarter={quarter}"/"part-00000.parquet"
        dest.parent.mkdir(parents=True,exist_ok=True)
        prepared=[]
        for rec in records:
            row={field.name:None for field in schema}
            for field in schema:
                if field.name.startswith("DQ_"):
                    row[field.name]=False
            row.update({"PRIOR_REPORT_COUNT":0,"DQ_FLAG_COUNT":0,
                        "ASSUMED_AVAILABLE_FROM":rec["REPDTE"]})
            row.update(rec)
            prepared.append(row)
        pq.write_table(pa.Table.from_pylist(prepared,schema=schema),dest,compression="snappy")
    return panel


@pytest.fixture(scope="module")
def spark():
    spark=spark_session()
    yield spark
    spark.stop()


@pytest.mark.parametrize("workload",WORKLOADS)
def test_three_real_engines_match(tmp_path,spark,workload):
    panel=make_panel(tmp_path)
    a=pandas_result(panel,workload)
    b=polars_result(panel,workload)
    c=spark_result(spark,panel,workload)
    assert compare_results(a,b,workload)["rows_compared"]==len(a)
    assert compare_results(a,c,workload)["rows_compared"]==len(a)
    assert "scan" in plan_text("polars",panel,workload).lower()
    assert len(plan_text("pyspark",panel,workload,spark=spark))>50
    if workload=="adjacent_asset_growth":
        assert next(r for r in a if r["REPDTE"]=="1984-12-31")["CONSECUTIVE_PAIRS"]==0
        assert next(r for r in a if r["REPDTE"]=="2000-06-30")["MEAN_ASSET_GROWTH_PCT"]==pytest.approx(25.)

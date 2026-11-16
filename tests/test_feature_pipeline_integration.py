"""Synthetic Parquet integration tests for feature construction and validation.

No FDIC records are generated or modified; synthetic schemas reproduce the
published 48-column panel contract and the source resolution categories.
"""
from datetime import date, timedelta
import csv
import json
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from fdic_pipeline.acquisition import arrow_schema, sha256, table_path
from fdic_pipeline.panel import panel_schema
from fdic_pipeline.data_quality import QUALITY_RULES
from fdic_pipeline.feature_pipeline import (
    FeatureError, build_feature_tables, verify_feature_stage,
    event_ledgers, model_schema, feature_schema, policy_lock,
)
from fdic_pipeline.feature_policy import FEATURE_NAMES


def _test_input(tmp_path: Path):
    panel_root = tmp_path / "quality_panel"
    dates = [date(2006,6,30),date(2006,9,30),date(2006,12,31),
             date(2008,3,31),date(2008,6,30),date(2009,3,31)]
    rows = {
        dates[0]: [1,3], dates[1]: [1,3], dates[2]: [1,3],
        dates[3]: [2,3,4,5],dates[4]:[3],dates[5]:[3],
    }
    contents = []
    for day in dates:
        quarter_index=(day.month-1)//3+1
        output=panel_root / "panel" / f"year={day.year}" / f"quarter={quarter_index}" / "part-00000.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        seq=[]
        for cert in rows[day]:
            row={"CERT":cert,"REPDTE":day,"STNAME":"NY","ASSET":120.0+cert,
                 "EQ":8.0,"DEP":100.0,"LNLSGR":60.0,"BRO":3.0,"ROA":1.0,
                 "CHBAL":14.0,"SC":12.0,"NCLNLS":2.0,"NIMY":2.3,
                 "RBC1AAJ":9.0,"RBCRWAJ":11.0,
                 "PRIOR_REPORT_COUNT":0,"REPORT_GAP_QUARTERS":None,
                 "DQ_EXTREME_ROE":False,"MISSING_FINANCIAL_COUNT":3,
                 "ASSUMED_AVAILABLE_FROM":day+timedelta(days=90),
                 "DQ_FLAG_COUNT":0}
            row.update({name: False for name in QUALITY_RULES})
            seq.append(row)
        pq.write_table(pa.Table.from_pylist(seq,schema=panel_schema()),output,compression="snappy")
        contents.append({"quarter":day.strftime("%Y%m%d"),
                         "file":str(output.relative_to(panel_root)),
                         "rows":len(seq),"sha256":sha256(output),"size_bytes":output.stat().st_size})
    terminals=[
        {"CERT":"1","LAST_REPDTE":"2006-12-31","INSTITUTION_END_DATE":"2007-09-30"},
        {"CERT":"2","LAST_REPDTE":"2008-03-31","INSTITUTION_END_DATE":"2008-10-01"},
        {"CERT":"3","LAST_REPDTE":"2009-03-31","INSTITUTION_END_DATE":""},
        {"CERT":"4","LAST_REPDTE":"2008-03-31","INSTITUTION_END_DATE":"2008-10-01"},
        {"CERT":"5","LAST_REPDTE":"2008-03-31","INSTITUTION_END_DATE":""},
    ]
    failures=[
        {"CERT":1,"RESTYPE":"FAILURE","FAILDATE":"2007-09-30"},
        {"CERT":2,"RESTYPE":"ASSISTANCE","FAILDATE":"2008-10-01"},
        {"CERT":4,"RESTYPE":"FAILURE","FAILDATE":"2008-10-01"},
        {"CERT":5,"RESTYPE":"UNKNOWN","FAILDATE":"2008-12-01"},
        {"CERT":None,"RESTYPE":"FAILURE","FAILDATE":"2008-11-02"},
    ]
    # Write the complete synthetic evidence needed by the independent verifier.
    # Verification reconstructs event ledgers from these files rather than trusting
    # the in-memory objects used during construction.
    acquisition_dir = tmp_path / "acquisition"
    acquisition_dir.mkdir()
    failures_file = table_path(acquisition_dir, "failures")
    pq.write_table(pa.Table.from_pylist(failures, schema=arrow_schema("failures")),
                   failures_file, compression="snappy")
    audit_dir = panel_root / "audits"
    audit_dir.mkdir()
    terminal_file = audit_dir / "terminal_exits.csv"
    terminal_columns = ("CERT", "LAST_REPDTE", "INSTITUTION_END_DATE")
    with terminal_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=terminal_columns)
        writer.writeheader()
        writer.writerows(terminals)

    ledgers,audit=event_ledgers(failures,terminals)
    snap={"experiment_id":"experiment_20261116T115513Z_d23d2b10",
          "panel_rows":sum(i["rows"] for i in contents),
          "partitions":contents,"quality_dir":str(panel_root),
          "part2_manifest_sha256":"a"*64,"quality_verification_sha256":"b"*64,
          "processing_verification_sha256":"c"*64,
          "failures_sha256":sha256(failures_file),
          "terminal_audit_sha256":sha256(terminal_file),
          "acquisition_path":str(acquisition_dir),
          "quality_path":str(panel_root),
          "feature_code_commit":"f"*40}
    return panel_root,contents,ledgers,audit,snap


def _build(tmp_path):
    root,parts,events,audit,upstream=_test_input(tmp_path)
    out=tmp_path / "features"
    verdict=build_feature_tables(parts,root,out,events,audit,
                                 code_commit="f"*40,upstream=upstream,
                                 test_calendar_min_failures=1,
                                 enforce_full_calendar=False)
    return out,verdict,upstream,events,audit


def test_feature_table_and_development_only_preprocessor(tmp_path):
    import joblib
    out,verdict,snap,events,audit=_build(tmp_path)
    assert verdict["success"]
    assert verdict["feature_count"]==21
    assert verdict["feature_rows"]==snap["panel_rows"]
    assert verdict["verified_quarters"]==len(snap["partitions"])
    assert verdict["actual_test_failure_source_rows"]==2
    assert verdict["actual_test_distinct_failure_certs_in_panel"]==1
    assert verdict["development_rows"]>0 and verdict["test_holdout_rows"]>0
    assert verdict["development_positive_bank_quarters"]>0
    assert verdict["test_positive_bank_quarters"]>0
    assert audit["source_rows_missing_cert"]==1
    m=json.loads((out/"part4_manifest.json").read_text())
    assert m["source_rows"] == sum(x["rows"] for x in snap["partitions"])
    assert (out/"feature_policy.lock.json").is_file()
    assert json.loads((out/"feature_policy.lock.json").read_text())==policy_lock()
    train=pq.read_table(out/"model_data"/"development.parquet").to_pylist()
    test=pq.read_table(out/"model_data"/"test_holdout.parquet").to_pylist()
    assert all(set(row)==set(model_schema().names) for row in train+test)
    assert any(r["CERT"]==1 and r["Y_FAIL_4Q"]==1 for r in train)
    assert any(r["CERT"]==4 and r["Y_FAIL_4Q"]==1 for r in test)
    assert not any(r["CERT"]==2 for r in test),"assistance must be censored"
    assert not any(r["CERT"]==5 for r in test),"unknown event evidence must be censored"
    prep=json.loads((out/"audits"/"preprocessing_fit.json").read_text())
    assert prep["test_data_used_in_fit"] is False
    assert prep["fit_rows"]==len(train)
    pipeline=joblib.load(out/"model_data"/"development_preprocessor.joblib")
    assert list(pipeline.feature_names_in_)==list(FEATURE_NAMES)
    assert verify_feature_stage(out,snap,min_test_failures=1)==verdict


def test_modified_feature_quarter_rejected(tmp_path):
    out,_,snap,events,audit=_build(tmp_path)
    entry=json.loads((out/"part4_manifest.json").read_text())["partitions"][0]
    with (out/entry["file"]).open("ab") as fp:
        fp.write(b"tampered")
    with pytest.raises(FeatureError,match="integrity"):
        verify_feature_stage(out,snap,min_test_failures=1)


def test_model_file_modification_rejected(tmp_path):
    out,_,snap,events,audit=_build(tmp_path)
    path=out/"model_data"/"test_holdout.parquet"
    with path.open("ab") as fp:
        fp.write(b"tampered")
    with pytest.raises(FeatureError,match="checksum"):
        verify_feature_stage(out,snap,min_test_failures=1)


def test_missing_or_wrong_parent_digest_rejected(tmp_path):
    out,_,snap,events,audit=_build(tmp_path)
    snap["processing_verification_sha256"]="0"*64
    with pytest.raises(FeatureError,match="lineage"):
        verify_feature_stage(out,snap,min_test_failures=1)


def test_requires_minimum_test_failures_in_production(tmp_path):
    root,parts,events,audit,upstream=_test_input(tmp_path)
    with pytest.raises(FeatureError,match="Fewer than 50"):
        build_feature_tables(parts,root,tmp_path/"invalid",events,audit,
                             code_commit="f"*40,upstream=upstream,
                             enforce_full_calendar=False)


def test_readonly_integrity_rejects_rewritten_model_counts(tmp_path):
    out,_,snap,events,audit=_build(tmp_path)
    path=out/"part4_manifest.json"
    manifest=json.loads(path.read_text())
    manifest["model_counts"]["test_holdout"]["positive"] += 1
    path.write_text(json.dumps(manifest))
    with pytest.raises(FeatureError):
        verify_feature_stage(out,snap,min_test_failures=1)


def test_partitioned_feature_file_read_preserves_physical_schema(tmp_path):
    """Directory-derived partition columns are not part of the report schema."""
    p = tmp_path / "feature_table" / "year=2006" / "quarter=2" / "part-00000.parquet"
    p.parent.mkdir(parents=True)
    physical = pa.Table.from_pylist([
        {"CERT": 1, "REPDTE": date(2006, 6, 30), "EXAMPLE": 2.5}
    ])
    pq.write_table(physical, p)
    actual = pq.ParquetFile(p).read()
    assert actual.schema == physical.schema
    assert actual.to_pylist() == physical.to_pylist()
    assert "year" not in actual.column_names
    assert "quarter" not in actual.column_names

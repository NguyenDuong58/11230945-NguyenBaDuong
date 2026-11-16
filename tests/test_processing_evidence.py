"""Independent evidence verification, source fingerprinting and stage invariants."""
import csv
import json
from pathlib import Path

import pytest
from fdic_pipeline.acquisition import sha256
from fdic_pipeline.processing_engines import ENGINE_NAMES, WORKLOADS
from fdic_pipeline.scalable_processing import (
    EVIDENCE, SAMPLE_NAMES, BenchmarkError, _summaries, _write_csv,
    _atomic_json, verify_evidence,
)


def mock_evidence(tmp_path: Path):
    out=tmp_path/"processing"
    out.mkdir()
    snap={"experiment_id":"experiment_20261116T115513Z_d23d2b10",
          "part3_commit":"e"*40,"part2_manifest_sha256":"b"*64,
          "quality_verification_sha256":"c"*64,"storage_verification_sha256":"d"*64,
          "panel_rows":10,"partitions":[{"quarter":str(i)} for i in range(170)],
          "samples":{"recent_85":{"rows":5,"quarters":85,"first":"a","last":"b"},
                     "full_170":{"rows":10,"quarters":170,"first":"c","last":"b"}}}
    _atomic_json(out/"input_snapshot.json",snap)
    _atomic_json(out/"environment.json",{"seed":11230945})
    results_by_workload={
       "quarter_aggregation": [
           {"REPDTE":"2000-03-31","BANK_QUARTERS":2,"TOTAL_ASSETS":100.0,"TOTAL_DEPOSITS":60.0,"MEAN_ROA":1.0,"EXTREME_ROE_COUNT":0},
           {"REPDTE":"2000-06-30","BANK_QUARTERS":2,"TOTAL_ASSETS":120.0,"TOTAL_DEPOSITS":70.0,"MEAN_ROA":1.1,"EXTREME_ROE_COUNT":1}],
       "adjacent_asset_growth": [
           {"REPDTE":"2000-03-31","BANK_QUARTERS":2,"CONSECUTIVE_PAIRS":0,"MEAN_ASSET_GROWTH_PCT":None},
           {"REPDTE":"2000-06-30","BANK_QUARTERS":2,"CONSECUTIVE_PAIRS":1,"MEAN_ASSET_GROWTH_PCT":10.0}],
       "state_year_risk": [
           {"YEAR":2000,"STNAME":"AL","BANK_QUARTERS":2,"TOTAL_ASSETS":100.0,"TOTAL_GROSS_LOANS":65.0,"MEAN_ROA":None,"NEGATIVE_EQUITY_COUNT":0,"EXCESS_LOAN_COUNT":0,"MISSING_FINANCIAL_ROWS":1},
           {"YEAR":2000,"STNAME":"DC","BANK_QUARTERS":2,"TOTAL_ASSETS":120.0,"TOTAL_GROSS_LOANS":68.0,"MEAN_ROA":1.3,"NEGATIVE_EQUITY_COUNT":1,"EXCESS_LOAN_COUNT":1,"MISSING_FINANCIAL_ROWS":1}]
    }
    trial_results=[]
    trials=[]
    compares=[]
    for i in (-1,0,1,2):
        for sample in SAMPLE_NAMES:
            for workload in WORKLOADS:
                for engine in ENGINE_NAMES:
                    trial_results.append({"iteration":i,"sample":sample,"workload":workload,
                                          "engine":engine,"result":results_by_workload[workload]})
                    trials.append({"iteration":i,"warmup":i<0,"sample":sample,"workload":workload,
                                   "engine":engine,"elapsed_seconds":1.0+ENGINE_NAMES.index(engine)*.1,
                                   "peak_process_tree_rss_mib":123.0,"start_process_tree_rss_mib":100.,
                                   "output_rows":2})
                for engine in ("polars","pyspark"):
                    compares.append({"iteration":i,"warmup":i<0,"sample":sample,"workload":workload,
                                     "comparison":"pandas_vs_"+engine,"rows_compared":2,
                                     "max_absolute_difference":0.0})
    summary, scaling=_summaries(trials)
    (out/"trial_results.jsonl").write_text("".join(json.dumps(row)+"\n" for row in trial_results), encoding="utf-8")
    _write_csv(out/"trials.csv",trials,
               ("iteration","warmup","sample","workload","engine","elapsed_seconds",
                "peak_process_tree_rss_mib","start_process_tree_rss_mib","output_rows"))
    _write_csv(out/"equivalence.csv",compares,
               ("iteration","warmup","sample","workload","comparison","rows_compared",
                "max_absolute_difference"))
    _write_csv(out/"performance_summary.csv",summary,
               ("sample","workload","engine","runs","median_seconds",
                "median_peak_process_tree_rss_mib","pandas_over_engine_speedup","output_rows"))
    _write_csv(out/"scaling_summary.csv",scaling,
               ("engine","workload","recent_85_median_seconds","full_170_median_seconds",
                "full_over_recent_runtime_ratio"))
    plan_hash={}
    for workload in WORKLOADS:
        for engine in ("polars","pyspark"):
            name=f"plans/{engine}_{workload}.txt"
            (out/name).parent.mkdir(exist_ok=True)
            (out/name).write_text("SCAN parquet; aggregate; test plan", encoding="utf-8")
            plan_hash[name]=sha256(out/name)
    (out/"plan_inspection.csv").write_text("plan,exists\ntest,1\n",encoding="utf-8")
    (out/"plan_discussion.md").write_text("Synthetic plan review",encoding="utf-8")
    for name in EVIDENCE:
        if not (out/name).exists():
            (out/name).parent.mkdir(parents=True,exist_ok=True)
            (out/name).write_bytes(b"test evidence")
    result_hash={}
    for sample in SAMPLE_NAMES:
        for workload in WORKLOADS:
            name=f"results/{sample}_{workload}.json"
            _atomic_json(out/name,{"rows":results_by_workload[workload]})
            result_hash[name]=sha256(out/name)
    m={"experiment_id":snap["experiment_id"],"code_commit":snap["part3_commit"],
       "source_rows":snap["panel_rows"],"partitions":170,
       "part2_manifest_sha256":snap["part2_manifest_sha256"],
       "quality_verification_sha256":snap["quality_verification_sha256"],
       "storage_verification_sha256":snap["storage_verification_sha256"],
       "sample_specs":snap["samples"],"workloads":list(WORKLOADS),"engines":list(ENGINE_NAMES),
       "warmups":1,"repetitions":3,"measured_trials":54,"warmup_trials":18,
       "comparisons":48,"evidence_sha256":{x:sha256(out/x) for x in EVIDENCE},
       "plan_sha256":plan_hash,"reference_result_sha256":result_hash}
    _atomic_json(out/"part3_manifest.json",m)
    return out,snap


def test_verifier_recomputes_independent_medians(tmp_path):
    out,snap=mock_evidence(tmp_path)
    result=verify_evidence(out,snap)
    assert result["success"] is True
    assert result["measured_trials"]==54
    assert result["warmup_trials"]==18
    assert result["numerical_comparisons"]==48
    assert result["plan_files"]==6


def test_verifier_rejects_hash_tampering(tmp_path):
    out,snap=mock_evidence(tmp_path)
    with (out/"trials.csv").open("ab") as f:f.write(b"altered")
    with pytest.raises(BenchmarkError,match="Evidence file changed"):
        verify_evidence(out,snap)


def test_verifier_rejects_wrong_source_id(tmp_path):
    out,snap=mock_evidence(tmp_path)
    snap["quality_verification_sha256"]="a"*64
    with pytest.raises(BenchmarkError,match="does not match verified source"):
        verify_evidence(out,snap)


def test_verifier_rejects_modified_reference_results(tmp_path):
    out,snap=mock_evidence(tmp_path)
    file=out/"results"/"recent_85_state_year_risk.json"
    file.write_text("{}",encoding="utf-8")
    with pytest.raises(BenchmarkError,match="Reference output table changed"):
        verify_evidence(out,snap)


def test_verifier_rejects_median_replacement_even_with_rehashed_file(tmp_path):
    out,snap=mock_evidence(tmp_path)
    file=out/"performance_summary.csv"
    with file.open(newline="",encoding="utf-8") as f:
        rows=list(csv.DictReader(f));names=list(rows[0])
    rows[0]["median_seconds"]="9999"
    _write_csv(file,rows,tuple(names))
    manifest=json.loads((out/"part3_manifest.json").read_text())
    manifest["evidence_sha256"]["performance_summary.csv"]=sha256(file)
    _atomic_json(out/"part3_manifest.json",manifest)
    with pytest.raises(BenchmarkError,match="Summary mismatch"):
        verify_evidence(out,snap)


def test_end_to_end_orchestration_with_deterministic_engine_fixture(tmp_path, monkeypatch):
    """Exercise all trial rounds and evidence creation without pretending to benchmark FDIC."""
    import fdic_pipeline.scalable_processing as proc
    from fdic_pipeline.processing_engines import RESULT_COLUMNS
    out = tmp_path / "processing"
    scratch = tmp_path / "scratch"
    snapshot={"experiment_id":"experiment_20261116T115513Z_d23d2b10",
              "part3_commit":"e"*40,"part2_manifest_sha256":"b"*64,
              "quality_verification_sha256":"c"*64,"storage_verification_sha256":"d"*64,
              "panel_rows":10,"partitions":[{"quarter":str(i)} for i in range(170)],
              "samples":{"recent_85":{"rows":5,"quarters":85,"first":"a","last":"b"},
                         "full_170":{"rows":10,"quarters":170,"first":"c","last":"b"}}}
    sample_outputs={
      "quarter_aggregation":[{"REPDTE":"2000-03-31","BANK_QUARTERS":2,"TOTAL_ASSETS":100.0,"TOTAL_DEPOSITS":60.0,"MEAN_ROA":1.2,"EXTREME_ROE_COUNT":1}],
      "adjacent_asset_growth":[{"REPDTE":"2000-03-31","BANK_QUARTERS":2,"CONSECUTIVE_PAIRS":0,"MEAN_ASSET_GROWTH_PCT":None}],
      "state_year_risk":[{"YEAR":2000,"STNAME":"AL","BANK_QUARTERS":2,"TOTAL_ASSETS":100.0,"TOTAL_GROSS_LOANS":60.0,"MEAN_ROA":1.2,"NEGATIVE_EQUITY_COUNT":1,"EXCESS_LOAN_COUNT":0,"MISSING_FINANCIAL_ROWS":2}],
    }
    monkeypatch.setattr(proc,"prepare_samples",lambda snap, scratch: {s:str(scratch/s/"panel") for s in SAMPLE_NAMES})
    monkeypatch.setattr(proc,"_environment",lambda repetitions,warmups: {"repetitions":repetitions,"warmups":warmups})
    calls={"count":0}
    def simulated_worker(script,panel,engine,workload):
        calls["count"]+=1
        return {"engine":engine,"workload":workload,"elapsed_seconds":1+ENGINE_NAMES.index(engine)*.1,
                "peak_process_tree_rss_mib":123.,"start_process_tree_rss_mib":90.,
                "output_rows":1,"result":sample_outputs[workload]}
    monkeypatch.setattr(proc,"_child_run",simulated_worker)
    def simulated_plans(panels,stage):
        rows=[]
        for workload in WORKLOADS:
            for engine in ("polars","pyspark"):
                name=f"plans/{engine}_{workload}.txt"
                (stage/name).parent.mkdir(parents=True,exist_ok=True)
                (stage/name).write_text("Scan parquet; Aggregate",encoding="utf-8")
                rows.append({"plan_file":name,"plan_sha256":sha256(stage/name)})
        (stage/"plan_inspection.csv").write_text("name,observed\nexample,1\n",encoding="utf-8")
        (stage/"plan_discussion.md").write_text("Synthetic plan inspection",encoding="utf-8")
        return rows
    monkeypatch.setattr(proc,"_collect_plans",simulated_plans)
    verdict=proc.build_evidence(out,scratch,tmp_path/"unused.py",snapshot,repetitions=3,warmups=1)
    assert calls["count"]==72
    assert verdict["measured_trials"]==54
    assert verdict["warmup_trials"]==18
    assert verdict["numerical_comparisons"]==48
    assert verdict["plan_files"]==6
    assert proc.verify_evidence(out,snapshot)==verdict


def test_input_contract_resolves_only_selected_experiment_and_parent_hash(tmp_path, monkeypatch):
    """Real registry semantics, synthetic metadata; no FDIC records are invented."""
    import fdic_pipeline.panel as panel_module
    from fdic_pipeline.acquisition import DEFAULT_START, DEFAULT_END, quarter_ends
    from fdic_pipeline.project_flow import initialize, stage_dir, publish_stage, digest
    from fdic_pipeline.scalable_processing import input_contract
    class StubSchema:
        names=["CERT","REPDTE"]
    monkeypatch.setattr(panel_module,"panel_schema",lambda: StubSchema())
    initialize(tmp_path,"a"*40)
    acq=stage_dir(tmp_path,"acquisition")
    acq.mkdir()
    (acq/"acquisition_gate.json").write_text(json.dumps({"success":True,
        "source_manifest_sha256":"f"*64}),encoding="utf-8")
    publish_stage(tmp_path,"acquisition","acquisition_gate.json")
    st=stage_dir(tmp_path,"storage")
    st.mkdir()
    (st/"part1_validation.json").write_text('{"success":true}',encoding="utf-8")
    st_pub=publish_stage(tmp_path,"storage","part1_validation.json")
    stage=stage_dir(tmp_path,"quality_panel")
    stage.mkdir()
    quarters=quarter_ends(DEFAULT_START,DEFAULT_END)
    partitions=[]
    for q in quarters:
        partitions.append({"quarter":q,
          "file":f"panel/year={q[:4]}/quarter={(int(q[4:6])-1)//3+1}/part-00000.parquet",
          "rows":1,"sha256":"c"*64,"size_bytes":100})
    manifest={"source_manifest_sha256":"f"*64,
              "storage_verification_sha256":st_pub["verification_sha256"],
              "code_commit":"b"*40,"expected_quarters":170,
              "scope":{"start":quarters[0],"end":quarters[-1]},
              "panel_fields":StubSchema.names,"lag_is_verified_publication":False,
              "partitions":partitions,"panel_rows":170,"quarantined_rows":0,"raw_rows":170}
    _atomic_json(stage/"part2_manifest.json",manifest)
    _atomic_json(stage/"part2_validation.json",{"success":True,
                 "code_commit":"b"*40,
                 "part2_manifest_sha256":sha256(stage/"part2_manifest.json")})
    publish_stage(tmp_path,"quality_panel","part2_validation.json")
    verified=input_contract(tmp_path,check_data_hashes=False)
    assert verified["panel_rows"]==170
    assert verified["samples"]["recent_85"]["rows"]==85
    assert verified["storage_verification_sha256"]==st_pub["verification_sha256"]
    # If the published quality manifest is modified, the reader must stop.
    manifest["panel_rows"]=171
    _atomic_json(stage/"part2_manifest.json",manifest)
    with pytest.raises(BenchmarkError,match="manifest digest"):
        input_contract(tmp_path,check_data_hashes=False)

"""Experiment identity, immutable handoffs and lineage regression tests."""
from __future__ import annotations
import json
from pathlib import Path
import pytest
from fdic_pipeline import project_flow as flow

COMMIT = "a" * 40


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_initialize_resume_and_active_pointer(tmp_path):
    initial = flow.initialize(tmp_path, COMMIT)
    assert initial["config"]["git_commit"] == COMMIT
    assert flow.initialize(tmp_path, COMMIT)["experiment_id"] == initial["experiment_id"]
    assert flow.stage_dir(tmp_path, "acquisition") == initial["experiment_dir"] / "acquisition"
    assert flow.get_active(tmp_path)["experiment_id"] == initial["experiment_id"]
    with pytest.raises(flow.HandoffError, match="source revision"):
        flow.initialize(tmp_path, "b" * 40)


def test_published_stage_requires_success_and_is_immutable(tmp_path):
    flow.initialize(tmp_path, COMMIT)
    path = flow.stage_dir(tmp_path, "acquisition") / "acquisition_gate.json"
    _write(path, {"success": False})
    with pytest.raises(flow.HandoffError, match="unsuccessful"):
        flow.publish_stage(tmp_path, "acquisition", path.name)
    _write(path, {"success": True, "rows": 3})
    published = flow.publish_stage(tmp_path, "acquisition", path.name)
    assert published["status"] == "PASSED"
    assert flow.publish_stage(tmp_path, "acquisition", path.name)["status"] == "PASSED"
    _write(path, {"success": True, "rows": 4})
    with pytest.raises(flow.HandoffError, match="immutable"):
        flow.publish_stage(tmp_path, "acquisition", path.name)
    with pytest.raises(flow.HandoffError, match="hash mismatch"):
        flow.resolve_stage(tmp_path, "acquisition")


def test_parent_required_and_linked(tmp_path):
    flow.initialize(tmp_path, COMMIT)
    storage_file = flow.stage_dir(tmp_path, "storage") / "part1_validation.json"
    _write(storage_file, {"success": True})
    with pytest.raises(flow.HandoffError, match="not passed"):
        flow.publish_stage(tmp_path, "storage", storage_file.name)
    source_file = flow.stage_dir(tmp_path, "acquisition") / "acquisition_gate.json"
    _write(source_file, {"success": True})
    flow.publish_stage(tmp_path, "acquisition", source_file.name)
    flow.publish_stage(tmp_path, "storage", storage_file.name)
    assert flow.resolve_stage(tmp_path, "storage")["parent_verification_sha256"] == flow.digest(source_file)
    source_file.write_text('{"success":false}', encoding="utf-8")
    with pytest.raises(flow.HandoffError):
        flow.resolve_stage(tmp_path, "storage")


def test_new_experiment_does_not_rewrite_previous(tmp_path):
    first = flow.initialize(tmp_path, COMMIT)
    current = flow.initialize(tmp_path, COMMIT, create_new=True)
    assert current["experiment_id"] != first["experiment_id"]
    assert (first["experiment_dir"] / "experiment_config.lock.json").is_file()
    assert len(list((tmp_path / "experiments").iterdir())) == 2


def test_prevent_path_escape_and_bad_ids(tmp_path):
    flow.initialize(tmp_path, COMMIT)
    _write(flow.stage_dir(tmp_path, "acquisition") / "valid.json", {"success": True})
    with pytest.raises(flow.HandoffError, match="basename"):
        flow.publish_stage(tmp_path, "acquisition", "../valid.json")
    with pytest.raises(flow.HandoffError, match="Unknown stage"):
        flow.stage_dir(tmp_path, "not_a_stage")

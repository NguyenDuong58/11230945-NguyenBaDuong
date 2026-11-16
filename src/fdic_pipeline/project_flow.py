"""Immutable experiment identity and verified stage-to-stage handoffs.

The active pointer selects an experiment; it is not a file-selection heuristic.
Each stage records the SHA-256 of its validation artifact and the exact parent
validation digest. Published stage references cannot be overwritten.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

EXPERIMENTS = "experiments"
STAGES = ("acquisition", "storage", "quality_panel", "processing", "features", "modelling")
PARENTS = {
    "acquisition": None, "storage": "acquisition",
    "quality_panel": "acquisition", "processing": "quality_panel",
    "features": "quality_panel", "modelling": "features",
}


class HandoffError(RuntimeError):
    """Invalid experiment state, parent lineage or artifact integrity."""


def utc_time() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def digest(path: Path) -> str:
    sha = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for part in iter(lambda: handle.read(1024 * 1024), b""):
            sha.update(part)
    return sha.hexdigest()


def _write_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise HandoffError(f"Missing or invalid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise HandoffError(f"Expected JSON object: {path}")
    return value


def _experiment_dir(root: Path, experiment_id: str) -> Path:
    if not re.fullmatch(r"experiment_\d{8}T\d{6}Z_[a-f0-9]{8}", experiment_id):
        raise HandoffError("Malformed experiment identifier")
    return Path(root) / EXPERIMENTS / experiment_id


def _active_file(root: Path) -> Path:
    return Path(root) / "control" / "active_experiment.json"


def get_active(root: Path) -> dict[str, Any]:
    """Read a specifically selected experiment, not the latest directory."""
    pointer = _read(_active_file(root))
    eid = pointer.get("experiment_id")
    folder = _experiment_dir(root, eid)
    cfg = _read(folder / "experiment_config.lock.json")
    if cfg.get("experiment_id") != eid or pointer.get("config_sha256") != digest(folder / "experiment_config.lock.json"):
        raise HandoffError("Active experiment pointer differs from locked experiment configuration")
    if cfg.get("repository") != "NguyenDuong58/11230945-NguyenBaDuong":
        raise HandoffError("Unexpected repository in locked configuration")
    if not re.fullmatch(r"[0-9a-f]{40}", str(cfg.get("git_commit", ""))):
        raise HandoffError("Locked source revision must be a complete git commit SHA")
    return {"experiment_id": eid, "experiment_dir": folder, "config": cfg}


def initialize(root: Path, git_commit: str, *, create_new: bool = False) -> dict[str, Any]:
    """Start an experiment or resume the explicitly active experiment.

    create_new=True rotates the active pointer but does not mutate prior runs.
    """
    root = Path(root)
    if not re.fullmatch(r"[0-9a-f]{40}", git_commit):
        raise HandoffError("Expected a 40-character git commit SHA")
    if _active_file(root).exists() and not create_new:
        active = get_active(root)
        if active["config"]["git_commit"] != git_commit:
            raise HandoffError("Clone the source revision recorded in the active experiment")
        return active
    eid = datetime.now(timezone.utc).strftime("experiment_%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex[:8]
    folder = _experiment_dir(root, eid)
    folder.mkdir(parents=True, exist_ok=False)
    config = {
        "experiment_id": eid,
        "repository": "NguyenDuong58/11230945-NguyenBaDuong",
        "git_commit": git_commit,
        "created_utc": utc_time(),
        "seed": 11230945,
        "financials_from": "1984-03-31",
        "financials_to": "2026-06-30",
    }
    _write_atomic(folder / "experiment_config.lock.json", config)
    _write_atomic(folder / "experiment_manifest.json", {"experiment_id": eid, "stages": {}})
    _write_atomic(_active_file(root), {
        "experiment_id": eid, "config_sha256": digest(folder / "experiment_config.lock.json")
    })
    return get_active(root)


def stage_dir(root: Path, stage: str) -> Path:
    if stage not in STAGES:
        raise HandoffError(f"Unknown stage: {stage}")
    return get_active(root)["experiment_dir"] / stage


def resolve_stage(root: Path, stage: str) -> dict[str, Any]:
    """Validate published stage and immediate upstream lineage before use."""
    if stage not in STAGES:
        raise HandoffError(f"Unknown stage: {stage}")
    active = get_active(root)
    folder = active["experiment_dir"]
    manifest = _read(folder / "experiment_manifest.json")
    if manifest.get("experiment_id") != active["experiment_id"]:
        raise HandoffError("Experiment manifest identity mismatch")
    ref = manifest.get("stages", {}).get(stage)
    if not isinstance(ref, dict) or ref.get("status") != "PASSED":
        raise HandoffError(f"Stage has not passed verification: {stage}")
    expected_path = folder / stage / ref.get("verification_name", "")
    if not expected_path.is_file() or digest(expected_path) != ref.get("verification_sha256"):
        raise HandoffError(f"Stage validation hash mismatch: {stage}")
    verdict = _read(expected_path)
    if verdict.get("success") is not True:
        raise HandoffError(f"Stage validation did not pass: {stage}")
    parent = PARENTS[stage]
    if parent is not None:
        upstream = resolve_stage(root, parent)
        if ref.get("parent_verification_sha256") != upstream["verification_sha256"]:
            raise HandoffError(f"Parent lineage mismatch: {stage}")
    return {**ref, "path": folder / stage, "validation": verdict}


def publish_stage(root: Path, stage: str, verification_name: str) -> dict[str, Any]:
    """Publish an immutable stage, only after checking parent and validation."""
    if stage not in STAGES:
        raise HandoffError(f"Unknown stage: {stage}")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*\.json", verification_name):
        raise HandoffError("Stage validation must be a JSON basename, not a relative path")
    active = get_active(root)
    folder = active["experiment_dir"]
    validation = folder / stage / verification_name
    verdict = _read(validation)
    if verdict.get("success") is not True:
        raise HandoffError("Cannot publish an unsuccessful stage")
    parent = PARENTS[stage]
    parent_hash = resolve_stage(root, parent)["verification_sha256"] if parent else None
    item = {
        "status": "PASSED", "verification_name": verification_name,
        "verification_sha256": digest(validation),
        "parent_verification_sha256": parent_hash,
        "published_utc": utc_time(),
    }
    file = folder / "experiment_manifest.json"
    manifest = _read(file)
    if manifest.get("experiment_id") != active["experiment_id"]:
        raise HandoffError("Cannot modify the manifest of a different experiment")
    stages = manifest.setdefault("stages", {})
    if stage in stages:
        previous = stages[stage]
        if any(previous.get(k) != item[k] for k in
               ("status", "verification_name", "verification_sha256", "parent_verification_sha256")):
            raise HandoffError(f"Published stage is immutable: {stage}")
        return resolve_stage(root, stage)
    stages[stage] = item
    _write_atomic(file, manifest)
    return resolve_stage(root, stage)

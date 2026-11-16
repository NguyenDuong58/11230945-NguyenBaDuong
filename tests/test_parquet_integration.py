"""Actual Parquet integration checks; run on Colab after requirements install."""

from datetime import date
from pathlib import Path
import sys

import pytest

pa = pytest.importorskip("pyarrow", reason="requires pinned pyarrow dependency")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fdic_pipeline import acquisition as a  # noqa: E402


class FixtureClient:
    def __init__(self):
        self.calls = []

    def fetch_all(self, endpoint, fields, filters=None):
        self.calls.append((endpoint, filters))
        if endpoint == "financials":
            q = filters.split(":")[1]
            quarter = a.parse_fdic_date(q).isoformat()
            rows = [
                {"CERT": "1", "REPDTE": quarter, "STNAME": "Example", "ASSET": "100.5", "DEP": 90},
                {"CERT": "2", "REPDTE": q, "STNAME": "Example", "ASSET": 200, "DEP": None},
            ]
        elif endpoint == "failures":
            rows = [
                {"CERT": 1, "NAME": "Failed Bank", "FAILDATE": "2010-01-01", "COST": 55},
                {"CERT": None, "NAME": "Historical event A", "FAILDATE": "1934-01-01"},
                {"NAME": "Historical event B", "FAILDATE": "1935-01-01"},
            ]
        else:
            rows = [
                {"CERT": 1, "NAME": "Bank A", "ACTIVE": "0"},
                {"CERT": 2, "NAME": "Bank B", "ACTIVE": "1"},
            ]
        return rows, len(rows)


def settings(path):
    return a.Settings(start=date(2023, 3, 31), end=date(2023, 6, 30),
                      out=path, pause_seconds=0)


def test_end_to_end_resume_corruption_and_verify(tmp_path):
    cfg = settings(tmp_path)
    client = FixtureClient()
    first = a.run(cfg, client)
    assert first["success"]
    assert first["expected_quarters"] == 2
    assert first["financial_rows"] == 4
    assert first["distinct_financial_certs"] == 2
    assert first["tables"]["failures"]["rows"] == 3
    assert first["tables"]["failures"]["missing_cert_rows"] == 2
    assert len(client.calls) == 4  # 2 quarterly files + 2 reference tables

    original_report_hash = a.sha256(tmp_path / "verification_report.json")
    a.run(cfg, client)
    assert len(client.calls) == 4  # file hashes and schemas verified, no downloads
    assert a.sha256(tmp_path / "verification_report.json") == original_report_hash
    a.run(a.Settings(**{**cfg.__dict__, "verify_only": True}))
    assert a.sha256(tmp_path / "verification_report.json") == original_report_hash

    file = a.financial_path(tmp_path, "20230331")
    file.write_bytes(b"intentionally corrupted")
    with pytest.raises(a.AcquisitionError, match="invalid files"):
        a.run(a.Settings(**{**cfg.__dict__, "verify_only": True}))

    third = a.run(cfg, client)
    assert third["success"]
    assert len(client.calls) == 5  # only one corrupted quarter re-fetched

    file.unlink()
    with pytest.raises(a.AcquisitionError):
        a.run(a.Settings(**{**cfg.__dict__, "verify_only": True}))


def test_wrong_quarter_rejected():
    rows = [{"CERT": 10, "REPDTE": "2023-06-30", "ASSET": 100}]
    with pytest.raises(a.AcquisitionError, match="Cross-quarter"):
        a.make_table("financials", rows, "20230331")


def test_duplicate_bank_quarter_rejected():
    rows = [
        {"CERT": 10, "REPDTE": "20230331", "ASSET": 100},
        {"CERT": 10, "REPDTE": "20230331", "ASSET": 101},
    ]
    with pytest.raises(a.AcquisitionError, match="repeated"):
        a.make_table("financials", rows, "20230331")


def test_schema_is_explicit():
    schema = a.arrow_schema("financials")
    assert str(schema.field("CERT").type) == "int64"
    assert str(schema.field("REPDTE").type) == "date32[day]"
    assert str(schema.field("ASSET").type) == "double"
    assert len(schema) == 33


def test_failure_null_cert_kept_in_parquet(tmp_path):
    from pyarrow import parquet as pq

    records = [
        {"CERT": "19", "NAME": "Recorded event", "FAILDATE": "2012-01-01"},
        {"CERT": None, "NAME": "Unlinked historical event", "FAILDATE": "1934-01-01"},
        {"NAME": "Event without CERT field", "FAILDATE": "1935-01-01"},
        {"CERT": "  ", "NAME": "Event with blank CERT", "FAILDATE": "1936-01-01"},
    ]
    table = a.make_table("failures", records)
    assert table.column("CERT").to_pylist() == [19, None, None, None]
    output = tmp_path / "failures.parquet"
    a.write_parquet_atomic(output, table, "failures")
    assert a.inspect_parquet(output, "failures", expected_rows=4) == 4
    loaded = pq.read_table(output)
    assert loaded.num_rows == 4
    assert loaded.column("CERT").null_count == 3


def test_failure_non_missing_invalid_cert_still_rejected():
    with pytest.raises(a.AcquisitionError, match="Non-integer CERT"):
        a.make_table("failures", [{"CERT": "not-a-cert", "NAME": "Invalid"}])
    with pytest.raises(a.AcquisitionError, match="Missing CERT"):
        a.make_table("financials", [{"REPDTE": "20230331", "ASSET": 1}], "20230331")
    with pytest.raises(a.AcquisitionError, match="Missing CERT"):
        a.make_table("institutions", [{"NAME": "Missing identifier"}])

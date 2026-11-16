"""Network-free tests for the FDIC acquisition client and integrity rules."""

import json
from datetime import date
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fdic_pipeline import acquisition as a  # noqa: E402


class Response:
    def __init__(self, status=200, payload=None, headers=None, body="error"):
        self.status_code = status
        self.payload = payload
        self.headers = headers or {}
        self.text = body

    def json(self):
        if isinstance(self.payload, ValueError):
            raise self.payload
        return self.payload


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.headers = {}
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, params, timeout))
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        return value


def settings(tmp_path, **overrides):
    kw = {
        "start": date(2023, 3, 31), "end": date(2023, 6, 30),
        "out": tmp_path, "pause_seconds": 0, "page_size": 2,
        "max_retries": 3,
    }
    kw.update(overrides)
    return a.Settings(**kw)


def payload(records, total):
    return {"meta": {"total": total}, "data": [{"data": r} for r in records]}


def test_full_exam_scope_is_170_quarters():
    quarters = a.quarter_ends(a.DEFAULT_START, a.DEFAULT_END)
    assert len(quarters) == 170
    assert (quarters[0], quarters[-1]) == ("19840331", "20260630")


@pytest.mark.parametrize("start,end", [
    (date(2023, 1, 1), date(2023, 6, 30)),
    (date(2024, 6, 30), date(2024, 3, 31)),
])
def test_reject_invalid_date_bounds(start, end):
    with pytest.raises(ValueError):
        a.quarter_ends(start, end)


@pytest.mark.parametrize("source,expected", [
    ("2023-03-31", date(2023, 3, 31)),
    ("20230331", date(2023, 3, 31)),
    (20230331, date(2023, 3, 31)),
    ("2023-03-31T00:00:00.000", date(2023, 3, 31)),
])
def test_date_formats(source, expected):
    assert a.parse_fdic_date(source) == expected


def test_bad_dates_fail_closed():
    for value in (None, "2023-02-30", "March 2023", "202303", True):
        with pytest.raises(a.AcquisitionError):
            a.parse_fdic_date(value)


def test_http_pagination_and_parameters(tmp_path):
    session = Session([
        Response(payload=payload([{"CERT": 1}, {"CERT": 2}], 3)),
        Response(payload=payload([{"CERT": 3}], 3)),
    ])
    client = a.FDICClient(settings(tmp_path), session=session, sleep=lambda _: None)
    rows, total = client.fetch_all("financials", ["CERT"], filters="REPDTE:20230331")
    assert total == 3
    assert [r["CERT"] for r in rows] == [1, 2, 3]
    assert [call[1]["offset"] for call in session.calls] == [0, 2]
    assert all(call[1]["filters"] == "REPDTE:20230331" for call in session.calls)
    assert all(call[1]["limit"] == 2 for call in session.calls)


def test_changed_api_total_is_error(tmp_path):
    session = Session([
        Response(payload=payload([{"CERT": 1}, {"CERT": 2}], 3)),
        Response(payload=payload([{"CERT": 3}], 4)),
    ])
    client = a.FDICClient(settings(tmp_path), session=session, sleep=lambda _: None)
    with pytest.raises(a.AcquisitionError, match="changed during pagination"):
        client.fetch_all("financials", ["CERT"])


def test_premature_empty_page_is_error(tmp_path):
    session = Session([
        Response(payload=payload([{"CERT": 1}, {"CERT": 2}], 4)),
        Response(payload=payload([], 4)),
    ])
    client = a.FDICClient(settings(tmp_path), session=session, sleep=lambda _: None)
    with pytest.raises(a.AcquisitionError, match="Premature empty"):
        client.fetch_all("financials", ["CERT"])


def test_retry_429_honors_retry_after(tmp_path):
    sleep_calls = []
    session = Session([
        Response(status=429, headers={"Retry-After": "4"}),
        Response(payload=payload([{"CERT": 1}], 1)),
    ])
    client = a.FDICClient(settings(tmp_path), session=session, sleep=sleep_calls.append)
    assert client.fetch_all("failures", ["CERT"])[1] == 1
    assert sleep_calls == [4.0]
    assert len(session.calls) == 2


def test_connection_retry(tmp_path):
    session = Session([
        requests.Timeout("interrupted"),
        Response(payload=payload([{"CERT": 1}], 1)),
    ])
    waits = []
    client = a.FDICClient(settings(tmp_path), session=session, sleep=waits.append)
    assert client.fetch_all("institutions", ["CERT"])[1] == 1
    assert waits == [1.0]


def test_http_400_is_not_retried(tmp_path):
    session = Session([Response(status=400, body="Invalid field")])
    client = a.FDICClient(settings(tmp_path), session=session, sleep=lambda _: None)
    with pytest.raises(a.AcquisitionError, match="HTTP 400"):
        client.fetch_all("financials", ["NOT_A_FIELD"])
    assert len(session.calls) == 1


def test_invalid_json_is_rejected(tmp_path):
    session = Session([Response(payload=ValueError("not json"))])
    client = a.FDICClient(settings(tmp_path), session=session, sleep=lambda _: None)
    with pytest.raises(a.AcquisitionError, match="Invalid JSON"):
        client.fetch_all("financials", ["CERT"])


def test_numeric_conversion_is_strict():
    assert a.convert_number("123.5", "ASSET") == 123.5
    assert a.convert_number(None, "ASSET") is None
    for value in ("N/A", "not-an-amount", float("inf"), True):
        with pytest.raises(a.AcquisitionError):
            a.convert_number(value, "ASSET")


def test_no_silent_integer_rounding_of_cert():
    assert a.convert_cert("00042") == 42
    for value in (0, -2, "42.5", None, True):
        with pytest.raises(a.AcquisitionError):
            a.convert_cert(value)


def test_manifest_scope_change_blocked(tmp_path):
    original = settings(tmp_path)
    manifest = a.load_manifest(tmp_path, original)
    a.save_manifest(tmp_path, manifest)
    assert a.load_manifest(tmp_path, original)["spec"]["start"] == "2023-03-31"
    changed = settings(tmp_path, end=date(2023, 9, 30))
    with pytest.raises(a.AcquisitionError, match="different dataset scope"):
        a.load_manifest(tmp_path, changed)


def test_verify_only_requires_existing_manifest(tmp_path):
    with pytest.raises(a.AcquisitionError, match="Missing manifest"):
        a.load_manifest(tmp_path, settings(tmp_path, verify_only=True))


def test_manifest_is_really_json(tmp_path):
    manifest = a.load_manifest(tmp_path, settings(tmp_path))
    a.save_manifest(tmp_path, manifest)
    stored = json.loads((tmp_path / "manifest.json").read_text())
    assert stored["financials"] == {}
    assert stored["tables"] == {}
    assert stored["spec"]["source"] == a.API_BASE


def test_no_cache_if_checksum_or_schema_is_wrong(monkeypatch, tmp_path):
    target = tmp_path / "financials_20230331.parquet"
    target.write_bytes(b"example test payload")
    entry = {"rows": 2, "api_total": 2, "complete": True,
             "sha256": a.sha256(target), "size_bytes": target.stat().st_size}
    assert not a.entry_is_valid(target, {**entry, "sha256": "deadbeef"}, "financials", "20230331")
    monkeypatch.setattr(a, "inspect_parquet", lambda *args: 2)
    assert a.entry_is_valid(target, entry, "financials", "20230331")
    monkeypatch.setattr(a, "inspect_parquet", lambda *args: (_ for _ in ()).throw(a.AcquisitionError("bad schema")))
    assert not a.entry_is_valid(target, entry, "financials", "20230331")


def test_download_unit_rejects_empty_response(tmp_path):
    fake_client = SimpleNamespace(fetch_all=lambda *args, **kwargs: ([], 0))
    with pytest.raises(a.AcquisitionError, match="Empty"):
        a.download_unit(fake_client, "financials", tmp_path / "q.parquet", "20230331")


def test_mocked_storage_end_to_end_and_resume(tmp_path, monkeypatch):
    """Exercise orchestration without pretending that JSON is real Parquet.

    The Parquet codec itself is tested separately in test_parquet_integration.py.
    """

    class FakeClient:
        def __init__(self):
            self.calls = []

        def fetch_all(self, endpoint, fields, filters=None):
            self.calls.append((endpoint, filters))
            if endpoint == "financials":
                records = [{"CERT": i, "REPDTE": filters.split(":")[1]}
                           for i in (11, 12)]
            elif endpoint == "failures":
                records = [
                    {"CERT": 11, "FAILDATE": "2010-01-01"},
                    {"CERT": None, "FAILDATE": "1934-01-01"},
                ]
            else:
                records = [{"CERT": 11}, {"CERT": 12}]
            return records, len(records)

    def fake_make_table(endpoint, records, quarter=None):
        return SimpleNamespace(records=records, num_rows=len(records))

    def fake_write(path, table, endpoint, quarter=None):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(table.records))
        return {"rows": table.num_rows, "api_total": table.num_rows,
                "sha256": a.sha256(path), "size_bytes": path.stat().st_size,
                "file": path.name, "complete": True, "downloaded_at_utc": a.utc_now()}

    def fake_inspect(path, endpoint, count=None, quarter=None):
        rows = json.loads(path.read_text())
        if count is not None and count != len(rows):
            raise a.AcquisitionError("wrong count")
        if quarter and any(row["REPDTE"] != quarter for row in rows):
            raise a.AcquisitionError("wrong quarter")
        return len(rows)

    monkeypatch.setattr(a, "make_table", fake_make_table)
    monkeypatch.setattr(a, "write_parquet_atomic", fake_write)
    monkeypatch.setattr(a, "inspect_parquet", fake_inspect)
    monkeypatch.setattr(a, "read_cert_ids", lambda path: [r["CERT"] for r in json.loads(path.read_text())])
    monkeypatch.setattr(a, "count_null_cert_rows", lambda path: sum(
        record.get("CERT") is None for record in json.loads(path.read_text())
    ))
    cfg = settings(tmp_path)
    client = FakeClient()
    outcome = a.run(cfg, client)
    assert outcome["success"] is True
    assert outcome["financial_rows"] == 4
    assert outcome["tables"]["failures"]["rows"] == 2
    assert outcome["tables"]["failures"]["missing_cert_rows"] == 1
    assert len(client.calls) == 4
    a.run(cfg, client)
    assert len(client.calls) == 4
    damaged_path = a.financial_path(tmp_path, "20230331")
    damaged_path.write_text("bad")
    with pytest.raises(a.AcquisitionError, match="invalid files"):
        a.run(settings(tmp_path, verify_only=True))
    a.run(cfg, client)
    assert len(client.calls) == 5
    assert a.financial_path(tmp_path, "20230331").exists()


def test_failures_nullable_cert_policy_without_parquet(monkeypatch):
    """Check the conversion rule without claiming an Arrow/Parquet round-trip."""
    import types

    class FakeTable:
        def __init__(self, rows):
            self.rows = rows
            self.num_rows = len(rows)

        def column(self, name):
            return types.SimpleNamespace(to_pylist=lambda: [row[name] for row in self.rows])

    fake_arrow = types.SimpleNamespace(
        Table=types.SimpleNamespace(from_pylist=lambda rows, schema: FakeTable(rows))
    )
    monkeypatch.setitem(sys.modules, "pyarrow", fake_arrow)
    monkeypatch.setattr(a, "arrow_schema", lambda endpoint: None)

    records = [
        {"CERT": 1, "NAME": "Recorded failure"},
        {"CERT": None, "NAME": "Historical failure"},
        {"NAME": "Historical failure without CERT key"},
        {"CERT": "  ", "NAME": "Historical failure with blank CERT"},
    ]
    table = a.make_table("failures", records)
    assert table.column("CERT").to_pylist() == [1, None, None, None]

    with pytest.raises(a.AcquisitionError, match="Non-integer CERT"):
        a.make_table("failures", [{"CERT": "invalid"}])
    with pytest.raises(a.AcquisitionError, match="Missing CERT"):
        a.make_table("financials", [{"REPDTE": "20230331"}], "20230331")
    with pytest.raises(a.AcquisitionError, match="Missing CERT"):
        a.make_table("institutions", [{"NAME": "Unknown"}])

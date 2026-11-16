"""Part 1 / Acquisition: typed, resumable retrieval of FDIC BankFind data.

Financials are stored per reporting quarter; Failures and Institutions are
retrieved in full. Validation preserves source nulls; exits and failure labels
are deliberately not inferred during acquisition.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import requests

# 1. Data scope, field selection and retrieval policy
API_BASE = "https://api.fdic.gov/banks"
SCHEMA_VERSION = 1
DEFAULT_START = date(1984, 3, 31)
DEFAULT_END = date(2026, 6, 30)
DEFAULT_PAGE_SIZE = 10_000

# Fields cover institution identity, funding, liquidity, asset quality,
# earnings and regulatory capital. Missing source values are not imputed.
FINANCIAL_FIELDS = (
    "CERT", "REPDTE", "STNAME", "ASSET", "DEP", "DEPDOM", "EQ", "BRO",
    "CHBAL", "SC", "LNLSGR", "LNLSNET", "LNRE", "LNCI", "LNCON",
    "LNATRES", "NCLNLS", "P3ASSET", "P9ASSET", "NAASSET", "NTLNLS",
    "ORE", "NETINC", "INTINC", "EINTEXP", "NONII", "NONIX", "ELNATR",
    "ROA", "ROE", "NIMY", "RBC1AAJ", "RBCRWAJ",
)
FAILURE_FIELDS = (
    "CERT", "NAME", "CITYST", "FAILDATE", "FAILYR", "COST",
    "QBFASSET", "QBFDEP", "RESTYPE",
)
INSTITUTION_FIELDS = (
    "CERT", "NAME", "STNAME", "BKCLASS", "ACTIVE", "ESTYMD",
    "ENDEFYMD", "CHANGEC1", "INSDATE",
)

ENDPOINT_FIELDS = {
    "financials": FINANCIAL_FIELDS,
    "failures": FAILURE_FIELDS,
    "institutions": INSTITUTION_FIELDS,
}
NUMERIC_FIELDS = {
    "financials": set(FINANCIAL_FIELDS) - {"CERT", "REPDTE", "STNAME"},
    "failures": {"FAILYR", "COST", "QBFASSET", "QBFDEP"},
    "institutions": set(),
}
PRIMARY_KEYS = {
    "financials": ("CERT", "REPDTE"),
    "failures": (),  # Preserve source records; no undocumented failure-event key.
    "institutions": ("CERT",),
}
RETRIABLE_CODES = {408, 425, 429, 500, 502, 503, 504}
LOG = logging.getLogger("fdic_acquisition")


class AcquisitionError(RuntimeError):
    """API, data-integrity, or local file validation failure."""


@dataclass(frozen=True)
class Settings:
    start: date = DEFAULT_START
    end: date = DEFAULT_END
    out: Path = Path("data_raw/fdic")
    page_size: int = DEFAULT_PAGE_SIZE
    pause_seconds: float = 0.25
    max_retries: int = 5
    timeout_seconds: int = 90
    force: bool = False
    verify_only: bool = False
    base_url: str = API_BASE

    def __post_init__(self) -> None:
        quarter_ends(self.start, self.end)
        if not 1 <= self.page_size <= DEFAULT_PAGE_SIZE:
            raise ValueError("page_size must be between 1 and 10000")
        if self.max_retries < 1 or self.timeout_seconds < 1 or self.pause_seconds < 0:
            raise ValueError("Invalid retry, timeout, or pause setting")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# 2. Reporting calendar, parsing and source manifest

def quarter_ends(start: date, end: date) -> list[str]:
    """Return inclusive quarter-end dates as YYYYMMDD; reject non-quarter bounds."""
    calendar = ((3, 31), (6, 30), (9, 30), (12, 31))
    valid_start = (start.month, start.day) in calendar
    valid_end = (end.month, end.day) in calendar
    if not valid_start or not valid_end or start > end:
        raise ValueError("start and end must be ordered calendar quarter-end dates")
    return [f"{y}{m:02d}{d:02d}"
            for y in range(start.year, end.year + 1)
            for m, d in calendar
            if start <= date(y, m, d) <= end]


def parse_fdic_date(value: Any) -> date:
    """Accept YYYYMMDD, YYYY-MM-DD, or ISO timestamp date components."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None or isinstance(value, bool):
        raise AcquisitionError(f"Invalid FDIC date: {value!r}")
    s = str(value).strip()
    try:
        if re.fullmatch(r"\d{8}", s):
            return datetime.strptime(s, "%Y%m%d").date()
        if re.match(r"^\d{4}-\d{2}-\d{2}(?:$|[T ])", s):
            return date.fromisoformat(s[:10])
    except ValueError as exc:
        raise AcquisitionError(f"Invalid FDIC date: {value!r}") from exc
    raise AcquisitionError(f"Unknown FDIC date representation: {value!r}")


def spec_for(settings: Settings) -> dict[str, Any]:
    return {
        "source": settings.base_url,
        "start": settings.start.isoformat(),
        "end": settings.end.isoformat(),
        "financials_fields": list(FINANCIAL_FIELDS),
        "failures_fields": list(FAILURE_FIELDS),
        "institutions_fields": list(INSTITUTION_FIELDS),
        "schema_version": SCHEMA_VERSION,
    }


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, obj: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.part")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(obj, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def load_manifest(out: Path, settings: Settings) -> dict[str, Any]:
    path = out / "manifest.json"
    spec = spec_for(settings)
    if not path.exists():
        if settings.verify_only:
            raise AcquisitionError(f"Missing manifest: {path}")
        return {
            "spec": spec, "created_at_utc": utc_now(),
            "financials": {}, "tables": {},
        }
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AcquisitionError(f"Unreadable manifest: {path}") from exc
    if manifest.get("spec") != spec:
        raise AcquisitionError(
            "Output directory uses a different dataset scope or schema. "
            "Use a new --out directory; never merge incompatible acquisitions."
        )
    if not isinstance(manifest.get("financials"), dict) or not isinstance(manifest.get("tables"), dict):
        raise AcquisitionError("Malformed manifest sections")
    return manifest


def save_manifest(out: Path, manifest: dict[str, Any]) -> None:
    manifest["updated_at_utc"] = utc_now()
    atomic_json(out / "manifest.json", manifest)


# 3. HTTP request and pagination integrity

class FDICClient:
    """HTTP client with retry policy and verified offset pagination."""

    def __init__(self, settings: Settings, session: requests.Session | None = None,
                 sleep: Callable[[float], None] = time.sleep):
        self.settings = settings
        self.session = session if session is not None else requests.Session()
        self.sleep = sleep
        self.session.headers.update({
            "User-Agent": "BFT65B-acquisition/1.0 (academic research)",
            "Accept": "application/json",
        })

    def request_json(self, endpoint: str, params: Mapping[str, Any]) -> dict[str, Any]:
        url = f"{self.settings.base_url.rstrip('/')}/{endpoint}"
        last_error: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                response = self.session.get(
                    url, params=dict(params), timeout=self.settings.timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = exc
                retryable = True
                retry_after = 0.0
            else:
                retryable = response.status_code in RETRIABLE_CODES
                retry_after = 0.0
                if response.status_code == 200:
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        raise AcquisitionError(f"Invalid JSON from {endpoint}") from exc
                    if not isinstance(payload, dict):
                        raise AcquisitionError(f"Unexpected API payload from {endpoint}")
                    if self.settings.pause_seconds:
                        self.sleep(self.settings.pause_seconds)
                    return payload
                last_error = AcquisitionError(
                    f"HTTP {response.status_code} for {endpoint}: "
                    f"{response.text[:250]}"
                )
                if not retryable:
                    raise last_error
                if response.headers.get("Retry-After", "").isdigit():
                    retry_after = float(response.headers["Retry-After"])

            if not retryable or attempt == self.settings.max_retries:
                break
            delay = min(60.0, max(retry_after, 2.0 ** (attempt - 1)))
            LOG.warning("Retrying %s after attempt %d (%s)", endpoint, attempt, last_error)
            self.sleep(delay)
        raise AcquisitionError(
            f"Request failed after {self.settings.max_retries} attempts: {endpoint}"
        ) from last_error

    def fetch_all(self, endpoint: str, fields: Sequence[str],
                  filters: str | None = None) -> tuple[list[dict[str, Any]], int]:
        """Read every result; detect changed totals, short/empty pages, bad keys."""
        if endpoint not in ENDPOINT_FIELDS:
            raise ValueError(f"Unsupported endpoint: {endpoint}")
        rows: list[dict[str, Any]] = []
        offset = 0
        total: int | None = None
        while total is None or offset < total:
            params: dict[str, Any] = {
                "fields": ",".join(fields),
                "limit": self.settings.page_size,
                "offset": offset,
                "sort_by": "CERT",
                "sort_order": "ASC",
                "format": "json",
            }
            if filters is not None:
                params["filters"] = filters
            payload = self.request_json(endpoint, params)
            try:
                page_total = int(payload["meta"]["total"])
                raw_page = payload["data"]
                if not isinstance(raw_page, list) or page_total < 0:
                    raise TypeError("Invalid total or data array")
                page = [item["data"] for item in raw_page]
                if not all(isinstance(row, dict) for row in page):
                    raise TypeError("Non-dictionary records")
            except (KeyError, TypeError, ValueError) as exc:
                raise AcquisitionError(f"Unexpected API response shape: {endpoint}") from exc
            if total is None:
                total = page_total
            elif total != page_total:
                raise AcquisitionError(
                    f"Total count changed during pagination for {endpoint}: "
                    f"{total} -> {page_total}; restart the unit"
                )
            if not page and offset < total:
                raise AcquisitionError(f"Premature empty API page at offset {offset}")
            if len(page) > self.settings.page_size or offset + len(page) > total:
                raise AcquisitionError("API page exceeded its declared limit or total")
            rows.extend(page)
            offset += len(page)
        if len(rows) != total:
            raise AcquisitionError(f"Page count mismatch: {len(rows)} != {total}")
        return rows, total


# 4. Typed records and Parquet integrity

def arrow_schema(endpoint: str) -> Any:
    """Stable, explicit typed Parquet schema for all acquisition batches."""
    import pyarrow as pa

    columns = []
    for field in ENDPOINT_FIELDS[endpoint]:
        if field == "CERT":
            kind = pa.int64()
        elif field == "REPDTE":
            kind = pa.date32()
        elif field in NUMERIC_FIELDS[endpoint]:
            kind = pa.float64()
        else:
            kind = pa.string()
        columns.append(pa.field(field, kind, nullable=True))
    return pa.schema(columns)


def convert_cert(value: Any) -> int:
    if value is None or isinstance(value, bool):
        raise AcquisitionError(f"Missing or invalid CERT: {value!r}")
    text = str(value).strip()
    if not re.fullmatch(r"\d+", text):
        raise AcquisitionError(f"Non-integer CERT: {value!r}")
    n = int(text)
    if n <= 0:
        raise AcquisitionError(f"CERT must be positive: {value!r}")
    return n


def convert_number(value: Any, name: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise AcquisitionError(f"Boolean value for numeric field {name}")
    try:
        n = float(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise AcquisitionError(f"Unexpected nonnumeric value for {name}: {value!r}") from exc
    if not math.isfinite(n):
        raise AcquisitionError(f"Non-finite numeric value for {name}: {value!r}")
    return n


def make_table(endpoint: str, records: Sequence[Mapping[str, Any]],
               quarter: str | None = None) -> Any:
    """Type records without silent coercion; reject malformed keys/values."""
    import pyarrow as pa

    if not records:
        raise AcquisitionError(f"No records returned for {endpoint}")
    formatted = []
    for idx, record in enumerate(records):
        # Some historical failure events have no FDIC certificate number.
        # Keep those source events; a missing CERT prevents a CERT-based join
        # but is not grounds for deleting a recorded failure.
        if endpoint != "failures" and "CERT" not in record:
            raise AcquisitionError(f"Missing CERT in {endpoint}, row {idx}")
        converted: dict[str, Any] = {}
        for name in ENDPOINT_FIELDS[endpoint]:
            value = record.get(name)
            if name == "CERT":
                if endpoint == "failures" and (
                    value is None or (isinstance(value, str) and not value.strip())
                ):
                    converted[name] = None
                else:
                    converted[name] = convert_cert(value)
            elif name == "REPDTE":
                if value is None:
                    raise AcquisitionError(f"Missing REPDTE in row {idx}")
                converted[name] = parse_fdic_date(value)
            elif name in NUMERIC_FIELDS[endpoint]:
                converted[name] = convert_number(value, name)
            else:
                converted[name] = str(value) if value is not None else None
        if quarter is not None and converted["REPDTE"].strftime("%Y%m%d") != quarter:
            raise AcquisitionError(f"Cross-quarter record in {quarter}: {converted['REPDTE']}")
        formatted.append(converted)
    table = pa.Table.from_pylist(formatted, schema=arrow_schema(endpoint))
    assert_unique(table, endpoint)
    return table


def assert_unique(table: Any, endpoint: str) -> None:
    columns = PRIMARY_KEYS[endpoint]
    if not columns:
        return
    values = zip(*(table.column(col).to_pylist() for col in columns))
    seen: set[tuple[Any, ...]] = set()
    for key in values:
        if any(value is None for value in key) or key in seen:
            raise AcquisitionError(f"Null or repeated {columns} in {endpoint}: {key}")
        seen.add(key)


def inspect_parquet(path: Path, endpoint: str, expected_rows: int | None = None,
                    quarter: str | None = None) -> int:
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    n = metadata.num_rows
    if expected_rows is not None and n != expected_rows:
        raise AcquisitionError(f"Row count mismatch in {path.name}: {n} != {expected_rows}")
    if parquet_file.schema_arrow != arrow_schema(endpoint):
        raise AcquisitionError(f"Unexpected Parquet schema in {path.name}")
    keys = list(PRIMARY_KEYS[endpoint])
    if "CERT" not in keys:
        keys.append("CERT")
    if endpoint == "financials":
        keys = ["CERT", "REPDTE"]
    table = parquet_file.read(columns=keys)
    if table.num_rows != n:
        raise AcquisitionError(f"Unable to read all rows from {path.name}")
    if endpoint in {"financials", "institutions"}:
        assert_unique(table, endpoint)
    # In the historical Failures endpoint, CERT may legitimately be null.
    # Parquet integrity is still checked (schema, row count, hash); null
    # certificate values are recorded in the audit rather than discarded.
    if quarter is not None:
        observed_dates = set(table.column("REPDTE").to_pylist())
        if observed_dates != {parse_fdic_date(quarter)}:
            raise AcquisitionError(f"Wrong reporting date in {path.name}")
    return n


def write_parquet_atomic(path: Path, table: Any, endpoint: str,
                         quarter: str | None = None) -> dict[str, Any]:
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.part")
    try:
        pq.write_table(table, temp, compression="snappy", use_dictionary=True)
        count = inspect_parquet(temp, endpoint, table.num_rows, quarter)
        digest = sha256(temp)
        size = temp.stat().st_size
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)
    return {
        "rows": count, "api_total": count, "sha256": digest,
        "size_bytes": size, "file": str(path.name),
        "downloaded_at_utc": utc_now(), "complete": True,
    }


def entry_is_valid(path: Path, record: Any, endpoint: str,
                   quarter: str | None = None) -> bool:
    if not isinstance(record, dict) or not record.get("complete") or not path.exists():
        return False
    if record.get("rows") != record.get("api_total"):
        return False
    try:
        if record.get("sha256") != sha256(path):
            return False
        if inspect_parquet(path, endpoint, record["rows"], quarter) != record["rows"]:
            return False
        if path.stat().st_size != record.get("size_bytes"):
            return False
    except Exception as exc:
        # Parquet exceptions may vary by pyarrow version; a damaged cache is never trusted.
        LOG.warning("Cached artifact rejected: %s (%s)", path, exc)
        return False
    return True


# 5. Quarter-partitioned data acquisition

def financial_path(out: Path, quarter: str) -> Path:
    """Hive-style year/quarter directory layout; one file per reporting quarter."""
    reporting_date = parse_fdic_date(quarter)
    quarter_number = (reporting_date.month - 1) // 3 + 1
    return (out / "financials" / f"year={reporting_date.year}"
            / f"quarter={quarter_number}" / "part-00000.parquet")


def table_path(out: Path, endpoint: str) -> Path:
    return out / f"{endpoint}.parquet"


def download_unit(client: FDICClient, endpoint: str, target: Path,
                  quarter: str | None = None) -> dict[str, Any]:
    filt = f"REPDTE:{quarter}" if quarter is not None else None
    records, total = client.fetch_all(endpoint, ENDPOINT_FIELDS[endpoint], filters=filt)
    if total == 0:
        raise AcquisitionError(f"Empty {endpoint} result for {quarter or 'complete table'}")
    table = make_table(endpoint, records, quarter)
    if table.num_rows != total:
        raise AcquisitionError(f"Non-matching API total for {endpoint}")
    entry = write_parquet_atomic(target, table, endpoint, quarter)
    entry["api_total"] = total
    if endpoint == "failures":
        missing_cert_rows = sum(
            record.get("CERT") is None
            or (isinstance(record.get("CERT"), str) and not record["CERT"].strip())
            for record in records
        )
        entry["missing_cert_rows"] = missing_cert_rows
        if missing_cert_rows:
            LOG.warning(
                "failures: %d of %d source events lack CERT; retained as null "
                "for the Part 2 linkage audit",
                missing_cert_rows, total,
            )
    # Important for field-dictionary audits: missing keys and explicit nulls differ.
    entry["unreturned_fields"] = [
        name for name in ENDPOINT_FIELDS[endpoint]
        if not any(name in record for record in records)
    ]
    if entry["unreturned_fields"]:
        LOG.warning("%s %s: fields absent from all source rows: %s",
                    endpoint, quarter or "full table", entry["unreturned_fields"])
    return entry


def read_cert_ids(path: Path) -> list[int]:
    """Load only the certificate column for the global unique-bank count."""
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).read(columns=["CERT"]).column("CERT").to_pylist()


def count_null_cert_rows(path: Path) -> int:
    """Read the nullable certificate column from an existing Parquet artifact."""
    import pyarrow.parquet as pq

    return int(pq.ParquetFile(path).read(columns=["CERT"]).column("CERT").null_count)


# 6. Whole-period verification and summary

def verify_dataset(out: Path, manifest: Mapping[str, Any],
                   quarters: Sequence[str], *, readonly: bool = False) -> dict[str, Any]:
    """Audit source artifacts; readonly mode preserves the published report hash."""
    missing: list[str] = []
    bad: list[str] = []
    rows_total = 0
    unique_certs: set[int] = set()
    for quarter in quarters:
        path = financial_path(out, quarter)
        entry = manifest["financials"].get(quarter)
        if entry is None:
            missing.append(quarter)
            continue
        if not entry_is_valid(path, entry, "financials", quarter):
            bad.append(quarter)
            continue
        rows_total += int(entry["rows"])
        # The narrow key scan is used only for the global bank count.
        unique_certs.update(read_cert_ids(path))
    table_info: dict[str, dict[str, Any]] = {}
    for endpoint in ("failures", "institutions"):
        path = table_path(out, endpoint)
        entry = manifest["tables"].get(endpoint)
        ok = entry_is_valid(path, entry, endpoint)
        if not ok:
            bad.append(endpoint)
        else:
            missing_cert_rows = None
            if endpoint == "failures":
                # Verify the source-data audit counter against actual Parquet.
                missing_cert_rows = count_null_cert_rows(path)
                expected_missing = entry.get("missing_cert_rows")
                if expected_missing is not None and missing_cert_rows != expected_missing:
                    bad.append(endpoint)
                    continue
            table_info[endpoint] = {
                "rows": entry["rows"], "file": str(path.relative_to(out)),
                "sha256": entry["sha256"],
            }
            if endpoint == "failures":
                table_info[endpoint]["missing_cert_rows"] = missing_cert_rows
    result = {
        "verified_at_utc": utc_now(),
        "success": not missing and not bad,
        "expected_quarters": len(quarters),
        "verified_quarters": len(quarters) - len(missing) - len([q for q in bad if q in quarters]),
        "missing_quarters": missing, "invalid_artifacts": bad,
        "financial_rows": rows_total,
        "distinct_financial_certs": len(unique_certs),
        "tables": table_info,
        "scope": {"start": quarters[0], "end": quarters[-1]},
    }
    stored_path = out / "verification_report.json"
    if not result["success"]:
        if not readonly:
            atomic_json(stored_path, result)
        raise AcquisitionError(
            f"Dataset verification failed. Missing quarters: {missing}; "
            f"invalid files: {bad}. See verification_report.json"
        )
    if stored_path.exists():
        prior = json.loads(stored_path.read_text(encoding="utf-8"))
        current_values = {k: v for k, v in result.items() if k != "verified_at_utc"}
        prior_values = {k: v for k, v in prior.items() if k != "verified_at_utc"}
        if current_values == prior_values:
            # A successful verification must not change the original evidence hash.
            return prior
        if readonly:
            raise AcquisitionError("Read-only audit differs from the published verification report")
    elif readonly:
        raise AcquisitionError("Read-only audit requires an existing verification report")
    atomic_json(stored_path, result)
    return result


def run(settings: Settings, client: FDICClient | None = None) -> dict[str, Any]:
    quarters = quarter_ends(settings.start, settings.end)
    settings.out.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(settings.out, settings)
    if not settings.verify_only:
        client = client if client is not None else FDICClient(settings)
        for i, q in enumerate(quarters, start=1):
            path = financial_path(settings.out, q)
            current = manifest["financials"].get(q)
            if not settings.force and entry_is_valid(path, current, "financials", q):
                LOG.info("financials [%d/%d] %s: valid cache, skipped", i, len(quarters), q)
                continue
            LOG.info("financials [%d/%d] %s: downloading", i, len(quarters), q)
            entry = download_unit(client, "financials", path, q)
            manifest["financials"][q] = entry
            save_manifest(settings.out, manifest)
            LOG.info("financials %s: %d rows, %d bytes, verified", q,
                     entry["rows"], entry["size_bytes"])
        for endpoint in ("failures", "institutions"):
            path = table_path(settings.out, endpoint)
            current = manifest["tables"].get(endpoint)
            if not settings.force and entry_is_valid(path, current, endpoint):
                LOG.info("%s: valid cache, skipped", endpoint)
                continue
            LOG.info("%s: downloading", endpoint)
            entry = download_unit(client, endpoint, path)
            manifest["tables"][endpoint] = entry
            save_manifest(settings.out, manifest)
            LOG.info("%s: %d rows, verified", endpoint, entry["rows"])
    summary = verify_dataset(settings.out, manifest, quarters, readonly=settings.verify_only)
    LOG.info("VERIFIED: %d quarters; %d financial rows; %d distinct CERTs; "
             "failures=%d (CERT missing=%d); institutions=%d",
             summary["verified_quarters"], summary["financial_rows"],
             summary["distinct_financial_certs"],
             summary["tables"]["failures"]["rows"],
             summary["tables"]["failures"]["missing_cert_rows"],
             summary["tables"]["institutions"]["rows"])
    return summary


def setup_logging(out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    log_dir = out / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = log_dir / f"acquisition_{stamp}_{os.getpid()}.log"
    LOG.setLevel(logging.INFO)
    LOG.handlers.clear()
    formatter = logging.Formatter("%(asctime)s UTC | %(levelname)s | %(message)s")
    formatter.converter = time.gmtime
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")):
        handler.setFormatter(formatter)
        LOG.addHandler(handler)
    return log_path


# 7. Command-line interface

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=DEFAULT_START.isoformat())
    parser.add_argument("--end", default=DEFAULT_END.isoformat())
    parser.add_argument("--out", default="data_raw/fdic")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--pause", type=float, default=0.25)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--force", action="store_true", help="Redownload all units; preserve scope")
    parser.add_argument("--verify-only", action="store_true", help="Audit files; no HTTP calls")
    args = parser.parse_args(argv)
    try:
        settings = Settings(
            start=date.fromisoformat(args.start), end=date.fromisoformat(args.end),
            out=Path(args.out).expanduser().resolve(), page_size=args.page_size,
            pause_seconds=args.pause, max_retries=args.max_retries,
            timeout_seconds=args.timeout, force=args.force, verify_only=args.verify_only,
        )
        log_path = setup_logging(settings.out)
        LOG.info("Scope: %s to %s | output=%s | verify_only=%s",
                 settings.start, settings.end, settings.out, settings.verify_only)
        LOG.info("Log: %s", log_path)
        run(settings)
        return 0
    except (AcquisitionError, OSError, ValueError, ImportError) as exc:
        LOG.error("Acquisition not completed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

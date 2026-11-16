"""Real PyArrow Dataset integration: 170 fragments and pruning counts."""
from __future__ import annotations
from datetime import date
import pytest

pa = pytest.importorskip("pyarrow", reason="requires pinned PyArrow")
import pyarrow.parquet as pq
from fdic_pipeline.acquisition import arrow_schema, financial_path, quarter_ends, DEFAULT_START, DEFAULT_END
from fdic_pipeline.layout_benchmark import worker, check_comparable, WORKLOADS


def test_partition_pruning_and_numerical_equivalence(tmp_path):
    folder = tmp_path / "source_snapshot"
    pair = tmp_path / "format_pair"
    pair.mkdir()
    all_rows = []
    for index, stamp in enumerate(quarter_ends(DEFAULT_START, DEFAULT_END)):
        day = date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:]))
        row = {"CERT": 100 + index, "REPDTE": day, "ASSET": float(index) + .5,
               "DEP": float(index), "ROA": 1.0, "EQ": 5.0}
        all_rows.append(row)
        target = financial_path(folder, stamp)
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist([row], schema=arrow_schema("financials")),
                       target, compression="snappy")
    pq.write_table(pa.Table.from_pylist(all_rows, schema=arrow_schema("financials")),
                   pair / "financials.snappy.parquet", compression="snappy")
    for workload in WORKLOADS:
        flat = worker(tmp_path, "flat", workload)
        partitioned = worker(tmp_path, "partitioned", workload)
        check_comparable(flat, partitioned)
        assert flat["rows"] == partitioned["rows"]
    assert worker(tmp_path, "partitioned", "one_quarter")["rows"] == 1
    assert worker(tmp_path, "partitioned", "one_year")["rows"] == 4

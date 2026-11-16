"""Part 3 core transformations on the same quality-panel Parquet input.

The three engine implementations use equivalent current/past-report information.
Neither failure records nor retrospective institution status are read.
"""
from __future__ import annotations

import math
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

ENGINE_NAMES = ("pandas", "polars", "pyspark")
WORKLOADS = ("quarter_aggregation", "adjacent_asset_growth", "state_year_risk")
REPORT_START = date(2000, 3, 31)
RESULT_COLUMNS = {
    "quarter_aggregation": (
        "REPDTE", "BANK_QUARTERS", "TOTAL_ASSETS", "TOTAL_DEPOSITS",
        "MEAN_ROA", "EXTREME_ROE_COUNT",
    ),
    "adjacent_asset_growth": (
        "REPDTE", "BANK_QUARTERS", "CONSECUTIVE_PAIRS", "MEAN_ASSET_GROWTH_PCT",
    ),
    "state_year_risk": (
        "YEAR", "STNAME", "BANK_QUARTERS", "TOTAL_ASSETS", "TOTAL_GROSS_LOANS",
        "MEAN_ROA", "NEGATIVE_EQUITY_COUNT", "EXCESS_LOAN_COUNT", "MISSING_FINANCIAL_ROWS",
    ),
}
INTEGER_COLUMNS = frozenset({
    "YEAR", "BANK_QUARTERS", "EXTREME_ROE_COUNT", "CONSECUTIVE_PAIRS",
    "NEGATIVE_EQUITY_COUNT", "EXCESS_LOAN_COUNT", "MISSING_FINANCIAL_ROWS",
})
KEY_COLUMNS = {
    "quarter_aggregation": ("REPDTE",),
    "adjacent_asset_growth": ("REPDTE",),
    "state_year_risk": ("YEAR", "STNAME"),
}
PARQUET_COLS = {
    "quarter_aggregation": ("CERT", "REPDTE", "ASSET", "DEP", "ROA", "DQ_EXTREME_ROE"),
    "adjacent_asset_growth": ("CERT", "REPDTE", "ASSET"),
    "state_year_risk": (
        "CERT", "REPDTE", "STNAME", "ASSET", "LNLSGR", "ROA", "DQ_NEGATIVE_EQUITY",
        "DQ_GROSS_LOANS_GT_ASSETS", "MISSING_FINANCIAL_COUNT",
    ),
}

class ProcessingError(RuntimeError):
    """Invalid transformation, noncomparable output or invalid execution plan."""


def _validate(workload: str) -> None:
    if workload not in WORKLOADS:
        raise ProcessingError(f"Unknown workload: {workload}")


def normalise(records: list[dict[str, Any]], workload: str) -> list[dict[str, Any]]:
    """Strictly normalize result rows; prevent silent null-to-zero coercion."""
    _validate(workload)
    required = set(RESULT_COLUMNS[workload])
    rows = []
    for record in records:
        if set(record) != required:
            raise ProcessingError(f"Unexpected result columns: {set(record) ^ required}")
        converted = {}
        for key in RESULT_COLUMNS[workload]:
            value = record[key]
            if key == "REPDTE":
                if isinstance(value, datetime):
                    value = value.date()
                if isinstance(value, date):
                    value = value.isoformat()
                if not isinstance(value, str):
                    raise ProcessingError("REPDTE must be an ISO date")
                try:
                    value = date.fromisoformat(value).isoformat()
                except ValueError as exc:
                    raise ProcessingError("Invalid REPDTE") from exc
            elif key == "STNAME":
                if not isinstance(value, str) or value == "":
                    raise ProcessingError("Unexpected blank STATE grouping key")
            elif key in INTEGER_COLUMNS:
                if isinstance(value, bool) or value is None:
                    raise ProcessingError(f"Unexpected integer aggregate: {key}")
                try:
                    number = int(value)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ProcessingError(f"Invalid integer aggregate: {key}") from exc
                if number < 0 or number != value:
                    raise ProcessingError(f"Invalid integer aggregate: {key}")
                value = number
            elif value is None or (isinstance(value, float) and math.isnan(value)):
                if key == "TOTAL_ASSETS":
                    raise ProcessingError("TOTAL_ASSETS cannot be null")
                value = None
            else:
                value = float(value)
                if not math.isfinite(value):
                    raise ProcessingError(f"Non-finite aggregate: {key}")
            converted[key] = value
        rows.append(converted)
    rows.sort(key=lambda r: tuple(r[c] for c in KEY_COLUMNS[workload]))
    keys = [tuple(r[c] for c in KEY_COLUMNS[workload]) for r in rows]
    if len(keys) != len(set(keys)):
        raise ProcessingError("Repeated result grouping key")
    return rows


def compare_results(left: list[dict[str, Any]], right: list[dict[str, Any]],
                    workload: str, *, rel_tol: float = 1e-10,
                    abs_tol: float = 1e-5) -> dict[str, float | int]:
    """Compare every row and value; aggregate counts and nulls must match exactly."""
    a = normalise(left, workload)
    b = normalise(right, workload)
    if len(a) != len(b):
        raise ProcessingError(f"Result row counts differ: {len(a)} != {len(b)}")
    max_abs_error = 0.0
    for i, (x, y) in enumerate(zip(a, b)):
        for name in RESULT_COLUMNS[workload]:
            u, v = x[name], y[name]
            if name in KEY_COLUMNS[workload] or name in INTEGER_COLUMNS or u is None or v is None:
                if u != v:
                    raise ProcessingError(f"Non-equivalent {workload} row {i} {name}: {u} != {v}")
            else:
                max_abs_error = max(max_abs_error, abs(u - v))
                if not math.isclose(u, v, rel_tol=rel_tol, abs_tol=abs_tol):
                    raise ProcessingError(f"Non-equivalent {workload} row {i} {name}: {u} != {v}")
    return {"rows_compared": len(a), "max_absolute_difference": max_abs_error}


def pandas_result(panel: Path, workload: str) -> list[dict[str, Any]]:
    """Eager pandas baseline; I/O and materialization occur in the timed call."""
    import numpy as np
    import pandas as pd
    _validate(workload)
    filters = [("REPDTE", ">=", REPORT_START)] if workload == "quarter_aggregation" else None
    df = pd.read_parquet(str(panel), engine="pyarrow", columns=list(PARQUET_COLS[workload]), filters=filters)
    df["REPDTE"] = pd.to_datetime(df["REPDTE"])
    if workload == "quarter_aggregation":
        df["DQ_EXTREME_ROE"] = df["DQ_EXTREME_ROE"].astype("int64")
        ans = df.groupby("REPDTE", sort=True).agg(
            BANK_QUARTERS=("CERT", "size"), TOTAL_ASSETS=("ASSET", "sum"),
            TOTAL_DEPOSITS=("DEP", "sum"), MEAN_ROA=("ROA", "mean"),
            EXTREME_ROE_COUNT=("DQ_EXTREME_ROE", "sum"),
        ).reset_index()
    elif workload == "adjacent_asset_growth":
        df = df.sort_values(["CERT", "REPDTE"], kind="mergesort")
        q = df["REPDTE"].dt.year * 4 + df["REPDTE"].dt.quarter
        df["_Q"] = q
        grp = df.groupby("CERT", sort=False)
        prevq = grp["_Q"].shift(1)
        preva = grp["ASSET"].shift(1)
        adjacent = (q - prevq == 1)
        df["_ADJ"] = adjacent.astype("int64")
        # Missing quarters do not imply a one-quarter growth observation.
        df["_GROWTH"] = np.where(adjacent & (preva > 0), 100.0 * (df["ASSET"] / preva - 1), np.nan)
        ans = df.groupby("REPDTE", sort=True).agg(
            BANK_QUARTERS=("CERT", "size"), CONSECUTIVE_PAIRS=("_ADJ", "sum"),
            MEAN_ASSET_GROWTH_PCT=("_GROWTH", "mean"),
        ).reset_index()
    else:
        df["YEAR"] = df["REPDTE"].dt.year
        df["STNAME"] = df["STNAME"].fillna("").astype(str).str.strip().replace("", "MISSING")
        df["_NEG"] = df["DQ_NEGATIVE_EQUITY"].astype("int64")
        df["_EXCESS"] = df["DQ_GROSS_LOANS_GT_ASSETS"].astype("int64")
        df["_MISS"] = (df["MISSING_FINANCIAL_COUNT"] > 0).astype("int64")
        ans = df.groupby(["YEAR", "STNAME"], sort=True).agg(
            BANK_QUARTERS=("CERT", "size"), TOTAL_ASSETS=("ASSET", "sum"),
            TOTAL_GROSS_LOANS=("LNLSGR", "sum"), MEAN_ROA=("ROA", "mean"),
            NEGATIVE_EQUITY_COUNT=("_NEG", "sum"), EXCESS_LOAN_COUNT=("_EXCESS", "sum"),
            MISSING_FINANCIAL_ROWS=("_MISS", "sum"),
        ).reset_index()
    records = ans.astype(object).where(pd.notna(ans), None).to_dict(orient="records")
    return normalise(records, workload)


def polars_query(panel: Path, workload: str) -> Any:
    """LazyFrame transformations with projection/predicate pushdown where possible."""
    import polars as pl
    _validate(workload)
    scan = pl.scan_parquet(str(Path(panel) / "**" / "part-00000.parquet"), hive_partitioning=False)
    scan = scan.select(list(PARQUET_COLS[workload]))
    if workload == "quarter_aggregation":
        return (scan.filter(pl.col("REPDTE") >= pl.lit(REPORT_START))
                .group_by("REPDTE").agg(
                    pl.len().alias("BANK_QUARTERS"),
                    pl.col("ASSET").sum().alias("TOTAL_ASSETS"),
                    pl.col("DEP").sum().alias("TOTAL_DEPOSITS"),
                    pl.col("ROA").mean().alias("MEAN_ROA"),
                    pl.col("DQ_EXTREME_ROE").cast(pl.Int64).sum().alias("EXTREME_ROE_COUNT"),
                ).sort("REPDTE"))
    if workload == "adjacent_asset_growth":
        return (scan.sort("CERT", "REPDTE")
                .with_columns((pl.col("REPDTE").dt.year() * 4 + pl.col("REPDTE").dt.quarter()).alias("_Q"))
                .with_columns(
                    pl.col("_Q").shift().over("CERT").alias("_PREV_Q"),
                    pl.col("ASSET").shift().over("CERT").alias("_PREV_ASSET"),
                ).with_columns(
                    (pl.col("_Q") - pl.col("_PREV_Q") == 1).fill_null(False).alias("_ADJ"),
                ).with_columns(
                    pl.when(pl.col("_ADJ") & (pl.col("_PREV_ASSET") > 0))
                    .then(100 * (pl.col("ASSET") / pl.col("_PREV_ASSET") - 1))
                    .otherwise(None).alias("_GROWTH"),
                ).group_by("REPDTE").agg(
                    pl.len().alias("BANK_QUARTERS"),
                    pl.col("_ADJ").cast(pl.Int64).sum().alias("CONSECUTIVE_PAIRS"),
                    pl.col("_GROWTH").mean().alias("MEAN_ASSET_GROWTH_PCT"),
                ).sort("REPDTE"))
    return (scan.with_columns(
                pl.col("REPDTE").dt.year().alias("YEAR"),
                pl.when(pl.col("STNAME").is_null() | (pl.col("STNAME").str.strip_chars() == ""))
                .then(pl.lit("MISSING")).otherwise(pl.col("STNAME").str.strip_chars())
                .alias("STNAME"),
                (pl.col("MISSING_FINANCIAL_COUNT") > 0).cast(pl.Int64).alias("_MISS"),
            ).group_by("YEAR", "STNAME").agg(
                pl.len().alias("BANK_QUARTERS"),
                pl.col("ASSET").sum().alias("TOTAL_ASSETS"),
                pl.col("LNLSGR").sum().alias("TOTAL_GROSS_LOANS"),
                pl.col("ROA").mean().alias("MEAN_ROA"),
                pl.col("DQ_NEGATIVE_EQUITY").cast(pl.Int64).sum().alias("NEGATIVE_EQUITY_COUNT"),
                pl.col("DQ_GROSS_LOANS_GT_ASSETS").cast(pl.Int64).sum().alias("EXCESS_LOAN_COUNT"),
                pl.col("_MISS").sum().alias("MISSING_FINANCIAL_ROWS"),
            ).sort("YEAR", "STNAME"))


def polars_result(panel: Path, workload: str) -> list[dict[str, Any]]:
    return normalise(polars_query(panel, workload).collect().to_dicts(), workload)


def spark_session() -> Any:
    """Two-worker local Spark; not a multi-node cluster."""
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]")
               .appName("BFT65B-Part3")
               .config("spark.ui.enabled", "false")
               .config("spark.driver.memory", "3g")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.parquet.filterPushdown", "true")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    return session


def spark_query(spark: Any, panel: Path, workload: str) -> Any:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    _validate(workload)
    source = spark.read.option("basePath", str(panel)).parquet(str(panel))
    source = source.select(*PARQUET_COLS[workload])
    if workload == "quarter_aggregation":
        return (source.filter(F.col("REPDTE") >= F.lit(REPORT_START.isoformat()))
                .groupBy("REPDTE").agg(
                    F.count(F.lit(1)).alias("BANK_QUARTERS"),
                    F.sum("ASSET").alias("TOTAL_ASSETS"),
                    F.coalesce(F.sum("DEP"), F.lit(0.0)).alias("TOTAL_DEPOSITS"),
                    F.avg("ROA").alias("MEAN_ROA"),
                    F.sum(F.col("DQ_EXTREME_ROE").cast("long")).alias("EXTREME_ROE_COUNT"),
                ).orderBy("REPDTE"))
    if workload == "adjacent_asset_growth":
        window = Window.partitionBy("CERT").orderBy("REPDTE")
        df = (source.withColumn("_Q", F.year("REPDTE") * 4 + F.quarter("REPDTE"))
              .withColumn("_PREV_Q", F.lag("_Q").over(window))
              .withColumn("_PREV_ASSET", F.lag("ASSET").over(window))
              .withColumn("_ADJ", F.when(F.col("_Q") - F.col("_PREV_Q") == 1, 1).otherwise(0)))
        df = df.withColumn("_GROWTH", F.when(
            (F.col("_ADJ") == 1) & (F.col("_PREV_ASSET") > 0),
            100.0 * (F.col("ASSET") / F.col("_PREV_ASSET") - 1.0),
        ))
        return (df.groupBy("REPDTE").agg(
            F.count(F.lit(1)).alias("BANK_QUARTERS"),
            F.sum("_ADJ").alias("CONSECUTIVE_PAIRS"),
            F.avg("_GROWTH").alias("MEAN_ASSET_GROWTH_PCT"),
        ).orderBy("REPDTE"))
    df = source.withColumn("YEAR", F.year("REPDTE"))
    df = df.withColumn("STNAME", F.when(
        F.col("STNAME").isNull() | (F.trim(F.col("STNAME")) == ""),
        F.lit("MISSING"),
    ).otherwise(F.trim(F.col("STNAME"))))
    return (df.groupBy("YEAR", "STNAME").agg(
        F.count(F.lit(1)).alias("BANK_QUARTERS"),
        F.sum("ASSET").alias("TOTAL_ASSETS"),
        F.coalesce(F.sum("LNLSGR"), F.lit(0.0)).alias("TOTAL_GROSS_LOANS"),
        F.avg("ROA").alias("MEAN_ROA"),
        F.sum(F.col("DQ_NEGATIVE_EQUITY").cast("long")).alias("NEGATIVE_EQUITY_COUNT"),
        F.sum(F.col("DQ_GROSS_LOANS_GT_ASSETS").cast("long")).alias("EXCESS_LOAN_COUNT"),
        F.sum(F.when(F.col("MISSING_FINANCIAL_COUNT") > 0, 1).otherwise(0)).alias("MISSING_FINANCIAL_ROWS"),
    ).orderBy("YEAR", "STNAME"))


def spark_result(spark: Any, panel: Path, workload: str) -> list[dict[str, Any]]:
    return normalise([r.asDict(recursive=True) for r in spark_query(spark, panel, workload).collect()], workload)


def run_engine(engine: str, panel: Path, workload: str, spark: Any = None) -> list[dict[str, Any]]:
    if engine == "pandas":
        return pandas_result(panel, workload)
    if engine == "polars":
        return polars_result(panel, workload)
    if engine == "pyspark":
        if spark is None:
            raise ProcessingError("PySpark requires a SparkSession")
        return spark_result(spark, panel, workload)
    raise ProcessingError(f"Unknown processing engine: {engine}")


def plan_text(engine: str, panel: Path, workload: str, spark: Any = None) -> str:
    _validate(workload)
    if engine == "polars":
        return polars_query(panel, workload).explain(optimized=True)
    if engine == "pyspark":
        if spark is None:
            raise ProcessingError("SparkSession required for explain")
        return spark_query(spark, panel, workload)._jdf.queryExecution().toString()
    raise ProcessingError("Execution plans requested only for Polars and PySpark")

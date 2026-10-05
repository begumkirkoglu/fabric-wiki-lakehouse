"""Data-quality checks run by the pipeline after each load.

Each check returns (name, passed, detail). Results are appended to
``ops_quality_results`` and a failing *blocking* check raises, which fails the pipeline
activity (and triggers its retry / alert).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wikilake import tables


def check_unique(df: DataFrame, key: list[str], name: str):
    dupes = df.groupBy(*key).count().where("count > 1").count()
    return name, dupes == 0, f"{dupes} duplicate keys"


def check_not_null(df: DataFrame, cols: list[str], name: str):
    cond = None
    for c in cols:
        cond = F.col(c).isNull() if cond is None else (cond | F.col(c).isNull())
    bad = df.where(cond).count()
    return name, bad == 0, f"{bad} rows with nulls in {cols}"


def check_freshness(df: DataFrame, ts_col: str, max_age: timedelta, name: str, now: datetime | None = None):
    latest = df.agg(F.max(ts_col)).first()[0]
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    ok = latest is not None and now - latest <= max_age
    return name, ok, f"latest {ts_col} = {latest}"


def check_pages_consistent(edits: DataFrame, pages: DataFrame, name: str):
    """Latest state must equal the max revision found in the event history."""
    expected = edits.groupBy("wiki", "page_title").agg(F.max("revision_new").alias("expected"))
    bad = (pages.join(expected, ["wiki", "page_title"], "full_outer")
           .where(~F.col("revision_new").eqNullSafe(F.col("expected"))).count())
    return name, bad == 0, f"{bad} pages whose latest revision disagrees with history"


def run(spark: SparkSession, freshness_minutes: int = 120, blocking: bool = True) -> list[dict]:
    edits = spark.read.table(tables.SILVER_EDITS)
    pages = spark.read.table(tables.SILVER_PAGES)
    checks = [
        check_unique(edits, ["event_id"], "silver_edits_unique_event_id"),
        check_not_null(edits, ["event_id", "wiki", "page_title", "event_time"], "silver_edits_keys_not_null"),
        check_unique(pages, ["wiki", "page_title"], "silver_pages_unique_page"),
        check_pages_consistent(edits, pages, "silver_pages_match_history"),
        check_freshness(edits, "event_time", timedelta(minutes=freshness_minutes), "silver_edits_fresh"),
    ]
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [{"run_at": run_at, "check": n, "passed": bool(p), "detail": d} for n, p, d in checks]
    spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(tables.QUALITY_LOG)
    failed = [r["check"] for r in rows if not r["passed"]]
    if failed and blocking:
        raise AssertionError(f"data quality failed: {failed}")
    return rows

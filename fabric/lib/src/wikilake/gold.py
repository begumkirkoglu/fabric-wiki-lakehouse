"""Silver -> gold aggregates for reporting and the Data Agent.

Recomputed for a sliding window of recent days and written with Delta ``replaceWhere``,
so a run is idempotent: re-running it for the same days gives the same result and never
duplicates rows. Older days stay untouched.
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from wikilake import tables


def hourly(edits: DataFrame) -> DataFrame:
    return (
        edits.withColumn("event_hour", F.date_trunc("hour", "event_time"))
        .groupBy("event_date", "event_hour", "wiki")
        .agg(
            F.count("*").alias("edits"),
            F.sum(F.when(F.col("change_type") == "new", 1).otherwise(0)).alias("new_pages"),
            F.sum(F.when(F.col("user_type") == "bot", 1).otherwise(0)).alias("bot_edits"),
            F.sum(F.when(F.col("user_type") == "anonymous", 1).otherwise(0)).alias("anonymous_edits"),
            F.sum(F.when(F.col("user_type") == "registered", 1).otherwise(0)).alias("registered_edits"),
            F.sum(F.greatest(F.col("byte_delta"), F.lit(0))).alias("bytes_added"),
            F.sum(F.least(F.col("byte_delta"), F.lit(0))).alias("bytes_removed"),
            F.countDistinct("page_title").alias("distinct_pages"),
        )
        .withColumn("bot_share", F.round(F.col("bot_edits") / F.col("edits"), 4))
    )


def top_pages(edits: DataFrame, n: int = 50) -> DataFrame:
    daily = (edits.where("is_article")
             .groupBy("event_date", "wiki", "page_title")
             .agg(F.count("*").alias("edits"), F.sum("byte_delta").alias("net_bytes"),
                  F.max("event_time").alias("last_edit_time")))
    w = Window.partitionBy("event_date", "wiki").orderBy(F.col("edits").desc(), F.col("page_title"))
    return daily.withColumn("rank", F.row_number().over(w)).where(F.col("rank") <= n)


def _write_window(df: DataFrame, table: str, start: date) -> None:
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"event_date >= '{start.isoformat()}'")
     .partitionBy("event_date").saveAsTable(table))


def run(spark: SparkSession, lookback_days: int = 2, today: date | None = None) -> dict:
    start = (today or date.today()) - timedelta(days=lookback_days)
    edits = spark.read.table(tables.SILVER_EDITS).where(F.col("event_date") >= F.lit(start))
    h, t = hourly(edits), top_pages(edits)
    _write_window(h, tables.GOLD_HOURLY, start)
    _write_window(t, tables.GOLD_TOP_PAGES, start)
    return {"from": start.isoformat(), "hourly_rows": h.count(), "top_page_rows": t.count()}

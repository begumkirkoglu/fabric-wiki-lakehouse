"""Bronze -> silver for the live Wikipedia edit stream.

Semantics
* **Event history** (``silver_wiki_edits``): every edit exactly once. The Eventstream
  path is at-least-once (producer retries, endpoint redelivery), so we de-duplicate on
  ``event_id`` inside each micro-batch *and* against the table with a MERGE.
* **Latest state** (``silver_page_latest``): one row per (wiki, page) holding its newest
  revision. Order is decided by ``revision_new`` (monotonic per page in MediaWiki), not by
  arrival time, so late or out-of-order events can never overwrite a newer revision.

The streaming job runs with ``trigger(availableNow=True)``: it processes everything new
since the last checkpoint and stops - so the same notebook works on a pipeline schedule
(incremental batch) or can be switched to a continuous trigger.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from wikilake import tables


def clean(df: DataFrame) -> DataFrame:
    """Type, validate and enrich raw bronze rows."""
    return (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("namespace", F.col("namespace").cast("int"))
        .withColumn("rc_id", F.col("rc_id").cast("long"))
        .withColumn("revision_old", F.col("revision_old").cast("long"))
        .withColumn("revision_new", F.col("revision_new").cast("long"))
        .withColumn("length_old", F.col("length_old").cast("long"))
        .withColumn("length_new", F.col("length_new").cast("long"))
        .withColumn("byte_delta", F.col("byte_delta").cast("long"))
        .withColumn("comment_length", F.col("comment_length").cast("int"))
        .where(F.col("event_id").isNotNull() & F.col("wiki").isNotNull()
               & F.col("page_title").isNotNull() & F.col("event_time").isNotNull()
               & F.col("change_type").isin("edit", "new"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("is_article", F.col("namespace") == 0)
        .withColumn("silver_loaded_at", F.current_timestamp())
    )


def dedupe(df: DataFrame) -> DataFrame:
    """One row per event_id within a batch (keep the earliest ingested copy)."""
    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").asc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def latest_per_page(df: DataFrame) -> DataFrame:
    """Newest revision per (wiki, page_title) within a batch."""
    w = Window.partitionBy("wiki", "page_title").orderBy(
        F.col("revision_new").desc_nulls_last(), F.col("event_time").desc())
    return (df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
            .select("wiki", "page_title", "namespace", "page_url", "revision_new",
                    F.col("event_time").alias("last_edit_time"), F.col("user_type").alias("last_user_type"),
                    F.col("length_new").alias("page_length"), "silver_loaded_at"))


def _merge_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    events = dedupe(clean(batch))
    events.createOrReplaceTempView("batch_events")
    spark.sql(f"""
        MERGE INTO {tables.SILVER_EDITS} t
        USING batch_events s
        ON t.event_id = s.event_id AND t.event_date = s.event_date
        WHEN NOT MATCHED THEN INSERT *
    """)
    latest_per_page(events).createOrReplaceTempView("batch_pages")
    spark.sql(f"""
        MERGE INTO {tables.SILVER_PAGES} t
        USING batch_pages s
        ON t.wiki = s.wiki AND t.page_title = s.page_title
        WHEN MATCHED AND s.revision_new > t.revision_new THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def ensure_tables(spark: SparkSession, bronze_sample: DataFrame) -> None:
    """Create silver tables (empty, with the right schema) on the first run."""
    events = dedupe(clean(bronze_sample.limit(0)))
    if not spark.catalog.tableExists(tables.SILVER_EDITS):
        events.write.format("delta").partitionBy("event_date").saveAsTable(tables.SILVER_EDITS)
    if not spark.catalog.tableExists(tables.SILVER_PAGES):
        latest_per_page(events).write.format("delta").saveAsTable(tables.SILVER_PAGES)


def run_stream(spark: SparkSession, checkpoint: str | None = None, continuous: bool = False):
    """Incrementally process new bronze rows into both silver tables."""
    bronze = spark.readStream.table(tables.BRONZE_EDITS)
    ensure_tables(spark, spark.read.table(tables.BRONZE_EDITS))
    writer = (bronze.writeStream.foreachBatch(_merge_batch)
              .option("checkpointLocation", checkpoint or f"{tables.CHECKPOINT_ROOT}/silver_wiki_edits"))
    writer = writer.trigger(processingTime="1 minute") if continuous else writer.trigger(availableNow=True)
    query = writer.start()
    if not continuous:
        query.awaitTermination()
    return query

"""Bulk load of the public NYC TLC trip records, one month (one file) at a time.

Why month by month: the full history is hundreds of millions of rows. Each month is an
independent chunk - downloaded, normalized and written on its own - so memory stays
bounded, a failure loses at most one month, and a re-run resumes where it stopped.

* Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (one Parquet file per month)
* Idempotent: each month is written with ``replaceWhere pickup_month = 'YYYY-MM'``, and
  recorded in ``ops_taxi_ingest_log``; completed months are skipped unless ``force=True``.
* Schema drift: column names and types changed over the years (``airport_fee`` vs
  ``Airport_fee``, ``passenger_count`` as double vs long). Every file is cast to one
  canonical schema before it is written.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wikilake import tables

log = logging.getLogger("wikilake.nyc_taxi")

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{month}.parquet"

CANONICAL = {  # column -> Spark type
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "ratecodeid": "int",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


def months(start: str, end: str) -> list[str]:
    """'2024-01', '2024-03' -> ['2024-01', '2024-02', '2024-03']"""
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def normalize(df: DataFrame, month: str, color: str = "yellow") -> DataFrame:
    """Canonical names/types + lineage columns; drop rows outside the file's month."""
    lower = {c: c.lower() for c in df.columns}
    for old, new in lower.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    cols = []
    for name, typ in CANONICAL.items():
        cols.append((F.col(name) if name in df.columns else F.lit(None)).cast(typ).alias(name))
    first_day = datetime.strptime(month + "-01", "%Y-%m-%d").date()
    nxt = date(first_day.year + first_day.month // 12, first_day.month % 12 + 1, 1)
    return (
        df.select(*cols)
        .where((F.col("tpep_pickup_datetime") >= F.lit(first_day))
               & (F.col("tpep_pickup_datetime") < F.lit(nxt)))  # files contain stray rows from other months
        .withColumn("pickup_month", F.lit(month))
        .withColumn("source_file", F.lit(os.path.basename(SOURCE_URL.format(color=color, month=month))))
    )


def download(month: str, dest_dir: str, color: str = "yellow", session=None) -> tuple[str, int]:
    """Stream one monthly file to the Lakehouse Files area (no full file in memory)."""
    import requests

    url = SOURCE_URL.format(color=color, month=month)
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, os.path.basename(url))
    tmp = path + ".part"
    http = session or requests.Session()
    with http.get(url, stream=True, timeout=(10, 300)) as resp:
        resp.raise_for_status()
        size = 0
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
                size += len(chunk)
    os.replace(tmp, path)
    return path, size


def done_months(spark: SparkSession) -> set[str]:
    if not spark.catalog.tableExists(tables.TAXI_LOG):
        return set()
    return {r.month for r in spark.read.table(tables.TAXI_LOG).where("status = 'loaded'").select("month").collect()}


def load(spark: SparkSession, start: str, end: str, color: str = "yellow", force: bool = False,
         local_dir: str = "/lakehouse/default/Files/raw/nyc_taxi", spark_dir: str = "Files/raw/nyc_taxi") -> list[dict]:
    """Load months [start, end]. ``local_dir`` is the Lakehouse Files mount seen by Python,
    ``spark_dir`` the same folder as Spark addresses it."""
    skip = set() if force else done_months(spark)
    results = []
    for month in months(start, end):
        if month in skip:
            log.info("skip %s (already loaded)", month)
            continue
        _, size = download(month, local_dir, color)
        raw = spark.read.parquet(f"{spark_dir}/{color}_tripdata_{month}.parquet")
        df = normalize(raw, month, color)
        (df.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"pickup_month = '{month}'")
         .partitionBy("pickup_month").saveAsTable(tables.TAXI_TRIPS))
        rows = spark.read.table(tables.TAXI_TRIPS).where(F.col("pickup_month") == month).count()
        entry = {"month": month, "rows": rows, "source_bytes": size, "status": "loaded",
                 "loaded_at": datetime.now(timezone.utc).replace(tzinfo=None)}
        (spark.createDataFrame([entry]).write.format("delta").mode("append").saveAsTable(tables.TAXI_LOG))
        log.info("loaded %s: %d rows", month, rows)
        results.append(entry)
    return results

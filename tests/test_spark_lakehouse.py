"""Silver / gold / quality logic on local Spark, fed with real producer output.

Delta-specific I/O (MERGE, replaceWhere) needs the Fabric runtime; here we test the
DataFrame logic those statements use, and capture the MERGE SQL that would run.
"""
from datetime import date, datetime, timedelta, timezone

import pytest

from conftest import FIXTURES
from wiki_producer.sse import parse_sse
from wiki_producer.transform import to_record

pytest.importorskip("pyspark")
from pyspark.sql import Row  # noqa: E402

from wikilake import gold, quality, silver  # noqa: E402


def bronze_rows():
    lines = (FIXTURES / "recentchange_sample.sse").read_text().splitlines(keepends=True)
    out = []
    for m in parse_sse(lines):
        try:
            r = to_record(m.json(), now=datetime(2026, 10, 5, 12, tzinfo=timezone.utc))
        except ValueError:
            continue
        if r:
            out.append(r)
    return out


@pytest.fixture()
def bronze(spark):
    rows = bronze_rows()
    dup = dict(rows[0], ingested_at="2026-10-05T13:00:00+00:00")     # at-least-once redelivery
    stale = dict(rows[1], event_id="late-1", revision_new=1, event_time="2026-10-05T11:00:00+00:00")  # late, old revision
    broken = dict(rows[2], event_id=None)                               # invalid
    return spark.createDataFrame(rows + [dup, stale, broken])


def test_clean_and_dedupe(bronze):
    cleaned = silver.clean(bronze)
    assert cleaned.where("event_id IS NULL").count() == 0
    assert dict(cleaned.dtypes)["event_time"] == "timestamp"
    events = silver.dedupe(cleaned)
    assert events.count() == events.select("event_id").distinct().count() == len(bronze_rows()) + 1
    first = events.where(f"event_id = '{bronze_rows()[0]['event_id']}'").selectExpr("hour(ingested_at) AS h").first()
    assert first.h == 12  # earliest copy kept (12:00 UTC, not the 13:00 redelivery)


def test_latest_per_page_uses_revision_not_arrival(bronze):
    events = silver.dedupe(silver.clean(bronze))
    pages = silver.latest_per_page(events)
    assert pages.count() == pages.select("wiki", "page_title").distinct().count()
    expected = {(r.wiki, r.page_title): r.m for r in
                events.groupBy("wiki", "page_title").agg({"revision_new": "max"})
                .withColumnRenamed("max(revision_new)", "m").collect()}
    assert {(r.wiki, r.page_title): r.revision_new for r in pages.collect()} == expected


def test_merge_batch_statements(spark, bronze, monkeypatch):
    statements = []
    monkeypatch.setattr(spark, "sql", lambda q: statements.append(" ".join(q.split())))
    silver._merge_batch(bronze, 0)
    assert "MERGE INTO silver_wiki_edits" in statements[0] and "WHEN NOT MATCHED THEN INSERT *" in statements[0]
    assert "WHEN MATCHED AND s.revision_new > t.revision_new THEN UPDATE SET *" in statements[1]
    assert spark.table("batch_events").count() == len(bronze_rows()) + 1


def test_gold_hourly_and_top_pages(bronze):
    events = silver.dedupe(silver.clean(bronze))
    h = gold.hourly(events)
    total = sum(r.edits for r in h.collect())
    assert total == events.count()
    for r in h.collect():
        assert r.bot_edits + r.anonymous_edits + r.registered_edits == r.edits
        assert 0 <= r.bot_share <= 1 and r.bytes_added >= 0 >= r.bytes_removed
    t = gold.top_pages(events, n=2)
    assert t.groupBy("event_date", "wiki").count().agg({"count": "max"}).first()[0] <= 2


def test_quality_checks_detect_problems(spark, bronze):
    events = silver.dedupe(silver.clean(bronze))
    pages = silver.latest_per_page(events)
    assert quality.check_unique(events, ["event_id"], "u")[1]
    assert not quality.check_unique(silver.clean(bronze), ["event_id"], "u")[1]   # duplicate before dedupe
    assert quality.check_pages_consistent(events, pages, "c")[1]
    wrong = pages.withColumn("revision_new", pages.revision_new - 1)
    assert not quality.check_pages_consistent(events, wrong, "c")[1]
    latest = events.agg({"event_time": "max"}).first()[0]
    assert quality.check_freshness(events, "event_time", timedelta(hours=1), "f", now=latest + timedelta(minutes=5))[1]
    assert not quality.check_freshness(events, "event_time", timedelta(minutes=1), "f",
                                       now=datetime(2026, 10, 6))[1]

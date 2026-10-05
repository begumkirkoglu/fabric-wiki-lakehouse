# Fabric Data Agent – setup

**Data source:** the `wiki_lakehouse` Lakehouse. Select these tables only:
`gold_wiki_hourly`, `gold_top_pages_daily`, `silver_page_latest`, `silver_wiki_edits`,
`bronze_nyc_taxi_trips`.

## Agent instructions (paste into *Agent instructions*)

```
You answer questions about live Wikipedia editing activity and NYC taxi trips using only the
selected Lakehouse tables. Never estimate or invent numbers; every number must come from a query.

Choosing a table:
- Volumes, trends, bot/anonymous/registered shares by wiki and time -> gold_wiki_hourly
  (one row per wiki per hour; columns edits, new_pages, bot_edits, anonymous_edits,
  registered_edits, bytes_added, bytes_removed, distinct_pages, bot_share).
- "Most edited pages" on a day -> gold_top_pages_daily (rank 1 = most edits).
- Current state of a page (latest revision, last edit time, size) -> silver_page_latest
  (one row per wiki + page_title).
- Anything per individual edit, or periods not in gold -> silver_wiki_edits (event history,
  one row per edit, already de-duplicated).
- Taxi questions -> bronze_nyc_taxi_trips (one row per trip; pickup_month = 'YYYY-MM').

Semantics:
- "Latest state" questions use silver_page_latest; "history" or "how many edits" questions use
  silver_wiki_edits or the gold tables. Say which one you used.
- wiki codes: enwiki = English, dewiki = German, trwiki = Turkish Wikipedia.
- user_type is bot / anonymous / registered; anonymous means a logged-out editor (temporary
  account or IP). There are no user names, temporary account names or IP addresses in the data.
- Times are UTC.

Answer format: one sentence with the direct answer, then a small table. Percentages with
1 decimal. Mention the time range covered.
```

## Example queries (add under *Example queries* for the Lakehouse)

| Question | SQL (Lakehouse SQL analytics endpoint, T-SQL) |
|---|---|
| How many edits per wiki in the last 24 hours? | `SELECT wiki, SUM(edits) AS edits FROM gold_wiki_hourly WHERE event_hour >= DATEADD(hour, -24, SYSUTCDATETIME()) GROUP BY wiki ORDER BY edits DESC;` |
| What share of English Wikipedia edits were made by bots today? | `SELECT CAST(100.0 * SUM(bot_edits) / NULLIF(SUM(edits),0) AS DECIMAL(5,1)) AS bot_pct FROM gold_wiki_hourly WHERE wiki = 'enwiki' AND event_date = CAST(SYSUTCDATETIME() AS DATE);` |
| Top 10 most edited Turkish Wikipedia articles today | `SELECT TOP 10 page_title, edits FROM gold_top_pages_daily WHERE wiki = 'trwiki' AND event_date = CAST(SYSUTCDATETIME() AS DATE) ORDER BY [rank];` |
| What is the latest revision of the page "Istanbul" on enwiki? | `SELECT revision_new, last_edit_time, page_length FROM silver_page_latest WHERE wiki = 'enwiki' AND page_title = 'Istanbul';` |
| Which hour had the most new pages on dewiki? | `SELECT TOP 1 event_hour, new_pages FROM gold_wiki_hourly WHERE wiki = 'dewiki' ORDER BY new_pages DESC;` |
| Average taxi fare and trip distance by month | `SELECT pickup_month, AVG(fare_amount) AS avg_fare, AVG(trip_distance) AS avg_miles, COUNT(*) AS trips FROM bronze_nyc_taxi_trips GROUP BY pickup_month ORDER BY pickup_month;` |
| Share of taxi trips paid by card | `SELECT CAST(100.0 * SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,1)) AS card_pct FROM bronze_nyc_taxi_trips;` |

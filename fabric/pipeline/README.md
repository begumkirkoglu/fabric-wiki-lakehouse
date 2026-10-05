# Data Pipelines

Two pipelines, deliberately separate: the live wiki flow runs every hour; the taxi bulk
load is a one-off / monthly job with a very different size and runtime, so it can never
delay or fail the hourly run.

## `pl_wiki_lakehouse` (scheduled hourly, can be run manually)

```
[Notebook] nb_10_silver_wiki_edits ──on success──► [Notebook] nb_20_gold_wiki ──on success──► [Notebook] nb_30_quality_checks
          continuous = false                               lookback_days = 2                        freshness_minutes = 120
                 │ on failure                                     │ on failure                              │ on failure
                 └────────────────────────────► [Teams / Outlook activity] "wiki lakehouse failed: @{activity().error.message}"
```

| Setting | Value | Why |
|---|---|---|
| Schedule | every 1 hour | Silver uses `availableNow`, so each run processes exactly what arrived since the last checkpoint |
| Retry (each notebook) | 2, interval 120 s | Transient Spark / capacity errors. Safe: silver resumes from its checkpoint, MERGE de-duplicates, gold uses `replaceWhere` |
| Timeout | 30 min per activity | A stuck run fails loudly instead of overlapping the next one |
| Concurrency | 1 | Two runs never write the same tables at once |
| Parameters | set on each Notebook activity's *Base parameters* | Override defaults without editing notebooks |

The notebooks return a JSON summary with `notebookutils.notebook.exit(...)`, visible in the
activity output (`@activity('nb_10_silver_wiki_edits').output.result.exitValue`).

## `pl_nyc_taxi_bulk` (manual or monthly)

One Notebook activity: `nb_40_bulk_nyc_taxi` with `start_month`, `end_month`, `force`.
Retry 1. Re-running after a failure skips months already in `ops_taxi_ingest_log`.

## Creating them

Fabric UI → workspace → **New item → Data pipeline** → add **Notebook** activities, link them
with the *On success* / *On failure* connectors as above, set retries on the **General** tab,
then **Schedule** on the pipeline toolbar.

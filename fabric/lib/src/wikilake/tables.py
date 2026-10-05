"""Table names in one place (all live in the notebook's default Lakehouse)."""

BRONZE_EDITS = "bronze_wiki_edits"        # written by the Eventstream Lakehouse destination
SILVER_EDITS = "silver_wiki_edits"        # typed, de-duplicated event history
SILVER_PAGES = "silver_page_latest"       # one row per page: its latest revision
GOLD_HOURLY = "gold_wiki_hourly"          # edits per wiki per hour, by user type
GOLD_TOP_PAGES = "gold_top_pages_daily"   # most edited pages per wiki per day
TAXI_TRIPS = "bronze_nyc_taxi_trips"      # bulk-loaded public dataset
TAXI_LOG = "ops_taxi_ingest_log"          # one row per loaded month (resume / idempotency)
QUALITY_LOG = "ops_quality_results"       # every data-quality run

CHECKPOINT_ROOT = "Files/checkpoints"

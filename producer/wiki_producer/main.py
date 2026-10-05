"""Stream live Wikipedia edits into a Fabric Eventstream.

  # dry run: 200 records to a local file
  python -m wiki_producer.main --sink jsonl --output sample.jsonl --max-events 200

  # live: to the Eventstream custom endpoint for 30 minutes, English + German + Turkish Wikipedia
  export EVENTSTREAM_CONNECTION_STRING='Endpoint=sb://...;EntityPath=es_...'
  python -m wiki_producer.main --wikis enwiki,dewiki,trwiki --duration 1800
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import time

from wiki_producer.sinks import EventstreamSink, JsonlSink
from wiki_producer.sse import WikimediaStream
from wiki_producer.transform import to_record

log = logging.getLogger("wiki_producer")


def run(stream, sink, wikis: set[str] | None, max_events: int, duration: float,
        report_every: float = 15.0) -> dict:
    stats = {"seen": 0, "kept": 0, "skipped": 0, "bad": 0}
    stop = {"flag": False}

    def handle(signum, _frame):
        stop["flag"] = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle)
        except ValueError:
            pass

    started = last_report = time.monotonic()
    try:
        for msg in stream.messages():
            stats["seen"] += 1
            try:
                record = to_record(msg.json(), wikis)
            except Exception:  # malformed event: count it, never stop the stream
                stats["bad"] += 1
                continue
            if record is None:
                stats["skipped"] += 1
            else:
                sink.send(record)
                stats["kept"] += 1
            now = time.monotonic()
            if now - last_report >= report_every:
                last_report = now
                log.info("progress %s", stats)
            if stop["flag"] or (max_events and stats["kept"] >= max_events) \
                    or (duration and now - started >= duration):
                break
    finally:
        sink.close()
    stats["sent"] = getattr(sink, "sent", stats["kept"])
    stats["failed"] = getattr(sink, "failed", 0)
    stats["reconnects"] = getattr(stream, "reconnects", 0)
    stats["elapsed_seconds"] = round(time.monotonic() - started, 1)
    log.info("finished %s", stats)
    return stats


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description="Wikimedia recent changes -> Fabric Eventstream")
    p.add_argument("--sink", choices=["eventstream", "jsonl"], default="eventstream")
    p.add_argument("--output", default="sample_wiki_edits.jsonl", help="file for --sink jsonl")
    p.add_argument("--wikis", default=os.getenv("WIKIS", ""), help="comma list, e.g. enwiki,trwiki (empty = all)")
    p.add_argument("--max-events", type=int, default=0, help="stop after N kept events (0 = no limit)")
    p.add_argument("--duration", type=float, default=0, help="stop after N seconds (0 = no limit)")
    args = p.parse_args()

    user_agent = os.getenv("WIKI_USER_AGENT")
    if not user_agent:
        raise SystemExit("Set WIKI_USER_AGENT, e.g. 'fabric-wiki-lakehouse/1.0 (you@example.com)' (Wikimedia policy)")
    wikis = {w.strip() for w in args.wikis.split(",") if w.strip()} or None

    if args.sink == "jsonl":
        sink = JsonlSink(args.output)
    else:
        conn = os.getenv("EVENTSTREAM_CONNECTION_STRING")
        if not conn:
            raise SystemExit("Set EVENTSTREAM_CONNECTION_STRING (Eventstream > custom endpoint > Event Hub > SAS key)")
        sink = EventstreamSink(conn)
    run(WikimediaStream(user_agent), sink, wikis, args.max_events, args.duration)


if __name__ == "__main__":
    main()

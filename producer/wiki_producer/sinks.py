"""Where records go.

``EventstreamSink`` sends to a Fabric Eventstream *custom endpoint* over the Event Hubs
protocol (the endpoint gives you an Event Hubs connection string with ``EntityPath``).
Records are batched (size + time window) and the partition key is ``wiki`` so events
of one wiki keep their order.

``JsonlSink`` writes newline-delimited JSON locally (dry run / sample data).
"""
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from pathlib import Path

log = logging.getLogger("wiki_producer.sink")


class JsonlSink:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")
        self.sent = 0

    def send(self, record: dict) -> None:
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.sent += 1

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class EventstreamSink:
    def __init__(self, connection_string: str, max_batch_wait: float = 2.0, client=None):
        if client is None:
            from azure.eventhub import EventHubProducerClient

            client = EventHubProducerClient.from_connection_string(connection_string)
        self.client = client
        self.max_batch_wait = max_batch_wait
        self._pending: dict[str, list[str]] = defaultdict(list)
        self._last_flush = time.monotonic()
        self.sent = 0
        self.failed = 0

    def send(self, record: dict) -> None:
        self._pending[record["wiki"]].append(json.dumps(record, ensure_ascii=False))
        if time.monotonic() - self._last_flush >= self.max_batch_wait:
            self.flush()

    def flush(self) -> None:
        from azure.eventhub import EventData

        for key, bodies in list(self._pending.items()):
            batch = self.client.create_batch(partition_key=key)
            for body in bodies:
                try:
                    batch.add(EventData(body))
                except ValueError:  # batch full (1 MB) -> send and start a new one
                    self._send(batch)
                    batch = self.client.create_batch(partition_key=key)
                    batch.add(EventData(body))
            self._send(batch)
        self._pending.clear()
        self._last_flush = time.monotonic()

    def _send(self, batch) -> None:
        n = len(batch)
        if not n:
            return
        for attempt in range(1, 6):
            try:
                self.client.send_batch(batch)
                self.sent += n
                return
            except Exception as exc:  # throttling / transient network errors
                if attempt == 5:
                    self.failed += n
                    log.error("dropping batch of %d events after retries: %s", n, exc)
                    return
                time.sleep(2 ** attempt)

    def close(self) -> None:
        self.flush()
        self.client.close()

"""Minimal Server-Sent Events client for Wikimedia EventStreams.

Why not a library: the protocol is tiny, and owning it lets us implement the two
things that matter in production:

* **resume** - every SSE message carries an ``id``; on reconnect we send it back as
  ``Last-Event-ID`` and the server continues from there (no gap, no replay).
* **backoff** - reconnect with exponential backoff on network errors / 5xx.

Wikimedia asks every client to send a descriptive ``User-Agent`` with contact info.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Iterable, Iterator

log = logging.getLogger("wiki_producer.sse")

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


@dataclass
class SseMessage:
    event: str
    id: str | None
    data: str

    def json(self) -> dict:
        return json.loads(self.data)


def parse_sse(lines: Iterable[str]) -> Iterator[SseMessage]:
    """Turn raw SSE lines into messages (spec: blank line ends a message)."""
    event, msg_id, data = "message", None, []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":
            if data:
                yield SseMessage(event, msg_id, "\n".join(data))
            event, msg_id, data = "message", None, []
            continue
        if line.startswith(":"):  # comment / keep-alive
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "event":
            event = value
        elif field == "data":
            data.append(value)
        elif field == "id":
            msg_id = value
    if data:  # stream ended without a trailing blank line
        yield SseMessage(event, msg_id, "\n".join(data))


class WikimediaStream:
    def __init__(self, user_agent: str, url: str = STREAM_URL, session=None,
                 max_backoff: float = 60.0, read_timeout: float = 60.0):
        import requests

        self.url = url
        self.user_agent = user_agent
        self.session = session or requests.Session()
        self.max_backoff = max_backoff
        self.read_timeout = read_timeout
        self.last_event_id: str | None = None
        self.reconnects = 0

    def messages(self) -> Iterator[SseMessage]:
        backoff = 1.0
        while True:
            headers = {"User-Agent": self.user_agent, "Accept": "text/event-stream"}
            if self.last_event_id:
                headers["Last-Event-ID"] = self.last_event_id
            try:
                with self.session.get(self.url, headers=headers, stream=True,
                                      timeout=(10, self.read_timeout)) as resp:
                    resp.raise_for_status()
                    backoff = 1.0
                    for msg in parse_sse(resp.iter_lines(decode_unicode=True)):
                        if msg.id:
                            self.last_event_id = msg.id
                        if msg.event == "message":
                            yield msg
            except GeneratorExit:
                return
            except Exception as exc:  # network error, 5xx, read timeout
                self.reconnects += 1
                log.warning("stream interrupted (%s), reconnecting in %.0fs", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)

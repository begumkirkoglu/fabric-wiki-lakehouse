"""Raw Wikimedia ``recentchange`` event -> flat, stable record (the bronze contract).

Two design rules:

1. **Flat and type-stable.** The Eventstream Lakehouse destination enforces a Delta
   schema; nested or type-switching fields fail the write. Every field below is always
   present with the same type (missing values become ``None`` of that type).
2. **No personal data.** Anonymous Wikipedia edits are attributed to an IP address and
   edit comments are free text. We keep neither: only a ``user_type`` category
   (bot / anonymous / registered) and the comment length.
"""
from __future__ import annotations

import ipaddress
from datetime import datetime, timezone

SCHEMA_VERSION = 1
KEPT_TYPES = {"edit", "new"}

FIELDS = (
    "event_id", "schema_version", "event_time", "ingested_at", "rc_id", "wiki",
    "server_name", "change_type", "namespace", "page_title", "page_url", "user_type",
    "is_minor", "is_patrolled", "length_old", "length_new", "byte_delta",
    "revision_old", "revision_new", "comment_length",
)


def _is_ip(user: str) -> bool:
    try:
        ipaddress.ip_address(user)
        return True
    except ValueError:
        return False


def user_type(user: str | None, bot: bool | None) -> str:
    if bot:
        return "bot"
    if user and _is_ip(user):
        return "anonymous"
    return "registered"


def _int(value) -> int | None:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def to_record(raw: dict, wikis: set[str] | None = None, now: datetime | None = None) -> dict | None:
    """Return the flat record, or None if the event is out of scope."""
    if raw.get("type") not in KEPT_TYPES:
        return None
    wiki = raw.get("wiki")
    if wikis and wiki not in wikis:
        return None
    meta = raw.get("meta") or {}
    if not meta.get("id") or not wiki or not raw.get("title"):
        return None

    ts = raw.get("timestamp")
    event_time = (datetime.fromtimestamp(ts, tz=timezone.utc) if isinstance(ts, (int, float))
                  else datetime.fromisoformat(meta["dt"].replace("Z", "+00:00")))
    length = raw.get("length") or {}
    revision = raw.get("revision") or {}
    old_len, new_len = _int(length.get("old")), _int(length.get("new"))

    record = {
        "event_id": meta["id"],
        "schema_version": SCHEMA_VERSION,
        "event_time": event_time.astimezone(timezone.utc).isoformat(),
        "ingested_at": (now or datetime.now(timezone.utc)).isoformat(),
        "rc_id": _int(raw.get("id")),
        "wiki": wiki,
        "server_name": raw.get("server_name"),
        "change_type": raw["type"],
        "namespace": _int(raw.get("namespace")),
        "page_title": raw["title"],
        "page_url": meta.get("uri"),
        "user_type": user_type(raw.get("user"), raw.get("bot")),
        "is_minor": bool(raw.get("minor", False)),
        "is_patrolled": bool(raw.get("patrolled", False)),
        "length_old": old_len,
        "length_new": new_len,
        "byte_delta": (new_len or 0) - (old_len or 0) if new_len is not None else None,
        "revision_old": _int(revision.get("old")),
        "revision_new": _int(revision.get("new")),
        "comment_length": len(raw.get("comment") or ""),
    }
    assert tuple(record) == FIELDS  # contract guard
    return record

import json
from datetime import datetime, timezone

from conftest import FIXTURES
from wiki_producer.main import run
from wiki_producer.sinks import EventstreamSink, JsonlSink
from wiki_producer.sse import WikimediaStream, parse_sse
from wiki_producer.transform import FIELDS, to_record, user_type

SAMPLE = (FIXTURES / "recentchange_sample.sse").read_text().splitlines(keepends=True)


def test_parse_sse_skips_comments_and_keeps_ids():
    msgs = list(parse_sse(SAMPLE))
    assert len(msgs) == 41  # 40 events + 1 malformed
    assert msgs[0].id.startswith('[{"topic"')
    assert all(m.event == "message" for m in msgs)


def test_record_contract_and_privacy():
    raws = [m for m in parse_sse(SAMPLE) if m.id != "bad"]
    records = [r for r in (to_record(m.json(), now=datetime(2026, 10, 5, tzinfo=timezone.utc)) for m in raws) if r]
    assert records and all(tuple(r) == FIELDS for r in records)
    assert {r["change_type"] for r in records} <= {"edit", "new"}
    text = json.dumps(records)
    for secret in ("203.0.113.7", "2001:db8::42", "ExampleEditor", "copyedit"):
        assert secret not in text  # no IPs, user names or comments leave the producer
    new_pages = [r for r in records if r["change_type"] == "new"]
    assert all(r["length_old"] is None and r["byte_delta"] == r["length_new"] for r in new_pages)
    # every field has one stable JSON type across records (Lakehouse destination needs this)
    for f in FIELDS:
        assert len({type(r[f]).__name__ for r in records if r[f] is not None}) <= 1, f


def test_user_type():
    assert user_type("203.0.113.7", False) == "anonymous"
    assert user_type("2001:db8::42", False) == "anonymous"
    assert user_type("~2025-01234-56", False) == "anonymous"  # temporary account (logged out)
    assert user_type("SomeBot", True) == "bot"
    assert user_type("Alice", False) == "registered"


def test_wiki_filter():
    raw = next(m for m in parse_sse(SAMPLE) if m.id != "bad" and m.json()["type"] == "edit").json()
    assert to_record(raw, wikis={"nowiki"}) is None
    assert to_record(raw, wikis={raw["wiki"]}) is not None


class FakeResp:
    def __init__(self, lines, fail_after=None):
        self.lines, self.fail_after = lines, fail_after

    def __enter__(self): return self
    def __exit__(self, *a): return False
    def raise_for_status(self): pass

    def iter_lines(self, decode_unicode=True):
        for i, line in enumerate(self.lines):
            if self.fail_after is not None and i == self.fail_after:
                raise ConnectionError("dropped")
            yield line.rstrip("\n")


class FakeSession:
    """First connection drops mid-stream; the reconnect must send Last-Event-ID."""

    def __init__(self):
        self.headers_seen = []

    def get(self, url, headers=None, stream=True, timeout=None):
        self.headers_seen.append(dict(headers))
        if len(self.headers_seen) == 1:
            return FakeResp(SAMPLE, fail_after=60)
        return FakeResp(SAMPLE)


def test_run_end_to_end_with_reconnect(tmp_path, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    session = FakeSession()
    stream = WikimediaStream("test-agent/1.0 (test@example.com)", session=session)
    out = tmp_path / "out.jsonl"
    stats = run(stream, JsonlSink(str(out)), wikis=None, max_events=30, duration=0)
    assert stats["kept"] == 30 and stats["reconnects"] == 1 and stats["bad"] >= 0
    assert "Last-Event-ID" not in session.headers_seen[0]
    assert session.headers_seen[1]["Last-Event-ID"].startswith('[{"topic"')
    assert session.headers_seen[0]["User-Agent"].startswith("test-agent")
    assert len(out.read_text().splitlines()) == 30


class FakeBatch(list):
    def __init__(self, key, limit=3):
        super().__init__(); self.key, self.limit = key, limit

    def add(self, ev):
        if len(self) >= self.limit:
            raise ValueError("full")
        self.append(ev)


class FakeEH:
    def __init__(self): self.sent, self.calls = [], 0
    def create_batch(self, partition_key=None): return FakeBatch(partition_key)
    def send_batch(self, b):
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("throttled")  # one transient failure, retried
        self.sent.append((b.key, len(b)))
    def close(self): pass


def test_eventstream_sink_batches_per_wiki_and_retries(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    eh = FakeEH()
    sink = EventstreamSink("unused", max_batch_wait=999, client=eh)
    for i in range(7):
        sink.send({"wiki": "enwiki" if i % 2 else "trwiki", "n": i})
    sink.close()
    assert sink.sent == 7 and sink.failed == 0
    assert {k for k, _ in eh.sent} == {"enwiki", "trwiki"}
    assert all(n <= 3 for _, n in eh.sent)

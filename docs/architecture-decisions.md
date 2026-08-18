# Architecture Decisions — Wikimedia Kafka Streaming Pipeline

## ADR 1 — Single-node KRaft, combined broker+controller role
**Decision:** Run one Kafka broker with `KAFKA_PROCESS_ROLES: broker,controller`, no ZooKeeper.

**Reasoning:** KRaft's own design goal was to require only one system to run instead of two — a single combined-role node is exactly the pattern intended for small/dev setups. Deliberately scoped for a two-weekend project.

**Trade-off, stated honestly:** replication factor 1 means zero fault tolerance — losing this
container's volume loses the data. The controller quorum, being size 1, has no fault tolerance of
its own either. The next meaningful step isn't 2 nodes (a 2-node Raft quorum tolerates zero
failures and costs double — worse than 1) — it's 3, the minimum where KRaft's consensus mechanism
gains any real fault tolerance.

## ADR 2 — `confluent-kafka` over `kafka-python`
**Decision:** Use `confluent-kafka-python` for both producer and consumer.

**Reasoning:** Wraps `librdkafka` (C), actively maintained, performance and reliability comparable to the Java client. `kafka-python` is explicitly flagged by its own maintainers as not receiving
Kafka protocol updates. `confluent-kafka` is also the client most production Kafka stacks and job
postings actually reference.

## ADR 3 — `requests-sse` over `sseclient-py`
**Decision:** Use `requests-sse` to consume the Wikimedia EventStreams endpoint.

**Reasoning:** Wikimedia's own `pywikibot` library switched its EventStreams client from `sseclient` to `requests-sse` as of v10.0. `requests-sse`'s own documentation uses this exact recentchange endpoint as its usage example, with typed exceptions (`InvalidStatusCodeError`,
`InvalidContentTypeError`) built in.

## ADR 4 — Explicit language derivation, not a raw field read
**Decision:** Derive `language` from the `wiki` field via a sister-project suffix list, with an
explicit non-language bucket — rather than assuming `wiki` maps directly to a language code.

**Reasoning:** The recentchange stream has no `language` field. `wiki` values like `zhwikisource`
(Chinese Wikisource) and `wikidatawiki` (not a language wiki at all) proved this within the first
three live events pulled during development — see `data-notes.md`, item 2.

## ADR 5 — `other` kept as a first-class category, not filtered
**Decision:** Non-language wikis (Commons, Wikidata, etc.) are aggregated under
`language = "other"` in the same table as real language codes, not excluded from the pipeline.

**Reasoning:** Filtering belongs downstream of ingestion, not upstream of it — the producer's job is to move real, legitimate data into Kafka faithfully; the topic should stay a complete record so any future consumer (e.g., a bot-vs-human tracker) isn't foreclosed by an upstream decision it never had a say in. Same principle as Project 1's raw zone staying untouched. A "language Wikipedias only" view is a `WHERE language != 'other'` at query time — reversible, costs nothing.

## ADR 6 — Hand-rolled tumbling window, not Faust or Kafka Streams
**Decision:** Implement the 5-minute window aggregation manually in the consumer (in-memory dict,
flushed on wall-clock boundary crossing), rather than adopting a stream-processing framework.

**Reasoning:** Kafka Streams' windowed aggregation is JVM-only, with no official Python port. Faust, the closest Python equivalent, is a fork of a project its original maintainer discontinued — an unnecessary dependency risk for a two-weekend build. Hand-rolling the window is also more
transparent: every line of the aggregation logic is explainable, not delegated to a framework's
internals.

## ADR 7 — SQLite as the local sink
**Decision:** Write aggregated windows to SQLite via Python's stdlib `sqlite3`, not a second
Docker service.

**Reasoning:** No extra container, no new dependency, keeps `docker-compose.yml` focused on the
technology actually being demonstrated (Kafka). Trivially queryable for interview demonstration
purposes.

## ADR 8 — `duration_seconds` tracked per window, not just documented
**Decision:** Record actual observed wall-clock duration per window (`observation_start` to flush
time) alongside every row, rather than only noting the limitation in prose.

**Reasoning:** The first and last window of any consumer run cover less than the full window length — the consumer only starts counting from whenever it happens to start, and stops whenever it's interrupted, but both were originally labeled with the window's full theoretical boundaries.
`duration_seconds` makes the distinction checkable (`WHERE duration_seconds >= <window_seconds>`)
rather than a caveat someone has to remember.

**Known gap:** this only catches edge-of-run truncation, not a mid-window broker/session
interruption — see `data-notes.md`, item 5.

## ADR 9 — Respecting EventStreams as a small-scale public service
**Decision:** Set an identifying `User-Agent` header on all requests to `stream.wikimedia.org`;
did not attempt to increase throughput or parallelize connections to the stream.

**Reasoning:** Wikimedia's own documentation describes EventStreams as intended for small-scale external tool developers, and enforces a User-Agent policy specifically to trace misbehaving clients. Building responsibly against someone else's public infrastructure is part of the actual
engineering, not incidental to it.
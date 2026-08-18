# Data Notes — Wikimedia Kafka Streaming Pipeline

## 1. Broker healthcheck confirms real readiness, not just container start
`docker compose ps` after `docker compose up -d`:

```
NAME      IMAGE                COMMAND                  SERVICE   CREATED         STATUS                   PORTS
kafka     apache/kafka:4.2.0   "/__cacert_entrypoin…"   kafka     3 minutes ago   Up 3 minutes (healthy)   0.0.0.0:9092-9093->9092-9093/tcp, [::]:9092-9093->9092-9093/tcp
```

`(healthy)` reflects the compose healthcheck actually invoking `kafka-broker-api-versions.sh`
against the broker — confirmation it's accepting client connections, not just that the process
started.

## 2. Live data disproved the "plain language wiki" assumption immediately
First three real events pulled from the stream during producer development:

```
event 0: wiki = "zhwikisource"
event 1: wiki = "zhwikisource"
event 2: wiki = "wikidatawiki"
```

Naive derivation (`wiki.removesuffix("wiki")`) fails on all three — `zhwikisource` isn't a plain
language wiki, it's Chinese Wikisource, a sister project. `wikidatawiki` isn't tied to a language
at all. `derive_language()` handles both: a suffix list covering every sister project
(`wiktionary`, `wikibooks`, `wikinews`, `wikiquote`, `wikisource`, `wikiversity`, `wikivoyage`,
`wiki`), plus an explicit non-language bucket for wikis like `wikidatawiki` and `commonswiki`.

## 3. "other" dominates raw edit volume — confirmed, not assumed
The first five real messages read back from the topic were all `"language": "other"` — every one
`commonswiki` or `wikidatawiki`. Cross-checked against Wikimedia's own engineering documentation:
Wikidata's edit volume is known to flood other wikis' recent-changes feeds, sometimes to double
the actual edit count of the wikis it propagates into.

Confirmed again at full-window scale — actual consumer output, two consecutive flushes from the
same run:

```
Flushed 2026-08-15T14:30:00+00:00 -> 2026-08-15T14:35:00+00:00: {'other': 1377, 'en': 582, 'bn': 17, 'tr': 79, 'eu': 3, 'mg': 16, 'es': 23, 'he': 7, 'ckb': 2, 'te': 1, 'ha': 2, 'th': 10, 'pt': 30, 'ja': 36, 'ce': 130, 'it': 39, 'fr': 41, 'ur': 41, 'ru': 35, 'vi': 17, 'ar': 12, 'pl': 42, 'hr': 2, 'nl': 6, 'uk': 19, 'sv': 11, 'hi': 8, 'co': 2, 'zh': 411, 'sr': 62, 'de': 51, 'kn': 1, 'hy': 24, 'hu': 3, 'ko': 15, 'ca': 6, 'fa': 24, 'bs': 1, 'id': 30, 'arz': 1, 'el': 3, 'et': 5, 'ig': 1, 'sd': 2, 'da': 1, 'ro': 4, 'as': 2, 'be_x_old': 1, 'ka': 1, 'fi': 3, 'sw': 2, 'ta': 1, 'cy': 4, 'su': 3, 'nn': 7, 'af': 2, 'so': 12, 'rue': 1, 'lv': 1, 'tt': 1, 'ms': 2, 'sl': 1, 'no': 2, 'bbc': 1} (72.9s observed [PARTIAL])
Flushed 2026-08-15T14:35:00+00:00 -> 2026-08-15T14:40:00+00:00: {'et': 11, 'it': 81, 'en': 1270, 'de': 102, 'other': 3597, 'ar': 30, 'zh': 938, 'fi': 6, 'sr': 137, 'ur': 10, 'ja': 77, 'pnb': 2, 'uk': 46, 'ce': 270, 'tr': 214, 'az': 8, 'es': 36, 'ko': 25, 'ca': 27, 'fr': 203, 'sw': 8, 'bn': 42, 'pl': 137, 'ru': 90, 'id': 18, 'fa': 29, 'hy': 53, 'cy': 6, 'pt': 36, 'nl': 33, 'no': 11, 'uz': 6, 'hu': 2, 'lb': 1, 'ne': 4, 'te': 6, 'vi': 45, 'su': 4, 'ha': 16, 'he': 52, 'af': 5, 'sv': 18, 'bg': 2, 'simple': 1, 'arz': 2, 'kab': 1, 'ckb': 17, 'be': 1, 'el': 15, 'co': 5, 'kk': 1, 'ro': 4, 'as': 7, 'th': 16, 'mg': 12, 'hr': 1, 'so': 24, 'ta': 4, 'lij': 1, 'la': 2, 'yo': 12, 'lv': 2, 'mn': 7, 'eo': 2, 'test': 1, 'ms': 1, 'nds': 2, 'cs': 4, 'hi': 2, 'ka': 1, 'ku': 2, 'be_x_old': 2, 'mr': 1, 'ie': 15, 'sl': 1, 'bs': 2, 'vo': 4} (300.0s observed)
```

`other: 3597` against `en: 1270` and `zh: 938`, the next largest categories, in the second
(full, un-partial) window. Decision: keep `other` as its own row rather than filtering it out —
see ADR #5.

## 4. Producer survived a live broker outage unassisted
Mid-run, the producer logged a genuine connectivity loss to the broker:

```
%5|...|REQTMOUT|rdkafka#producer-1|...: Timed out ApiVersionRequest in flight (after 10066ms, timeout #0)
%6|...|FAIL|rdkafka#producer-1|...: Disconnected: connection closed by peer: POLLHUP (after 10034ms in state APIVERSION_QUERY)
%3|...|FAIL|rdkafka#producer-1|...: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2312ms in state CONNECT)
```

Followed by a run of `Delivery failed: KafkaError{code=_MSG_TIMED_OUT,...}` for messages queued
during the outage, then automatic recovery — `Produced N events so far...` resumed with no
restart needed. No code change required; this is `confluent-kafka`'s built-in retry behavior
working as intended.

## 5. The same outage forced a consumer group rebalance — and exposed a real blind spot
Consumer log from the same incident:

```
%4|...|SESSTMOUT|rdkafka#consumer-1|...: Consumer group session timed out (in join-state steady)
after 45267 ms without a successful response from the group coordinator (broker -1, last error
was Local: Broker transport failure): revoking assignment and rejoining group
```

`45267ms` lines up almost exactly with `confluent-kafka`'s default `session.timeout.ms` of
`45000` — the outage lasted long enough for the group coordinator to give up on this consumer
and force a rejoin.

**Known limitation:** the window flushed immediately after this event was stored with
`duration_seconds: 300.0` — not flagged partial — because `duration_seconds` only measures
wall-clock time between a window's start and its flush, not whether the consumer was actually
connected the whole time. A mid-window rebalance like this one produces a silent undercount that
the current schema cannot detect. Not fixed in this build; documented as a known gap. A fix
would hook `confluent-kafka`'s `on_revoke`/`on_assign` callbacks to track actual connected time
per window.

## 6. Partial vs. full windows, side by side — the evidence behind `duration_seconds`
Query run against the raw table, no filter:

```
python -c "import sqlite3; [print(r) for r in sqlite3.connect('edit_velocity.db').execute('SELECT window_start, language, edit_count, duration_seconds FROM edit_velocity ORDER BY window_start, language')]"
```

Two real rows from that output, same language, same run:

```
('2026-08-15T00:14:30+00:00', 'zh', 27, 11.4)
('2026-08-15T00:12:00+00:00', 'zh', 160, 30.0)
```

Wildly different counts, purely because of how much wall-clock time each window actually
covered — `11.4` is the `Ctrl+C` mid-window; `30.0` ran its full course. This is the concrete
case for why `duration_seconds` exists — without it, both rows look equally trustworthy.

Filtered query, once the `duration_seconds` column existed to filter on:

```
python -c "import sqlite3; [print(r) for r in sqlite3.connect('edit_velocity.db').execute(\"SELECT window_start, language, edit_count, duration_seconds FROM edit_velocity WHERE duration_seconds >= 300 ORDER BY window_start, language\")]"
```

Every row returned came back at exactly `300.0` — the filter doing precisely what it was built
to do, keeping only windows that ran their full length:

```
('2026-08-15T14:35:00+00:00', 'af', 5, 300.0)
('2026-08-15T14:35:00+00:00', 'ar', 30, 300.0)
('2026-08-15T14:35:00+00:00', 'ce', 270, 300.0)
('2026-08-15T14:35:00+00:00', 'en', 1270, 300.0)
('2026-08-15T14:35:00+00:00', 'other', 3597, 300.0)
('2026-08-15T14:35:00+00:00', 'zh', 938, 300.0)
```
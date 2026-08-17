import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer

KAFKA_TOPIC = "wikimedia-recentchange"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP = "wiki-language-aggregator"
WINDOW_SECONDS = 300  # temporarily 30 for testing — switch back to 300 once verified
DB_PATH = "edit_velocity.db"


def init_db(path):
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS edit_velocity (
            window_start     TEXT NOT NULL,
            window_end       TEXT NOT NULL,
            language         TEXT NOT NULL,
            edit_count       INTEGER NOT NULL,
            duration_seconds REAL NOT NULL,
            PRIMARY KEY (window_start, language)
        )
    """)
    conn.commit()
    return conn


def window_start_for(ts: float, window_seconds: float = WINDOW_SECONDS) -> float:
    return ts - (ts % window_seconds)

def is_partial(duration_seconds: float, window_seconds: float = WINDOW_SECONDS) -> bool:
    return duration_seconds < window_seconds

def flush_window(conn, window_start_epoch, counts, observation_start, observation_end):
    window_end_epoch = window_start_epoch + WINDOW_SECONDS
    start_iso = datetime.fromtimestamp(window_start_epoch, tz=timezone.utc).isoformat()
    end_iso = datetime.fromtimestamp(window_end_epoch, tz=timezone.utc).isoformat()
    duration_seconds = round(observation_end - observation_start, 1)

    rows = [
        (start_iso, end_iso, lang, count, duration_seconds)
        for lang, count in counts.items()
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO edit_velocity VALUES (?, ?, ?, ?, ?)", rows
    )
    conn.commit()
    flag = " [PARTIAL]" if is_partial(duration_seconds, WINDOW_SECONDS) else ""
    print(f"Flushed {start_iso} -> {end_iso}: {dict(counts)} ({duration_seconds}s observed{flag})")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([KAFKA_TOPIC])
    conn = init_db(DB_PATH)

    current_window_start = None
    observation_start = None
    counts = defaultdict(int)

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()
            this_window_start = window_start_for(now)

            if current_window_start is None:
                current_window_start = this_window_start
                observation_start = now  # counting truly begins here, not at the theoretical boundary

            if this_window_start != current_window_start:
                if counts:
                    flush_window(conn, current_window_start, counts, observation_start, now)
                counts = defaultdict(int)
                current_window_start = this_window_start
                observation_start = now

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value())
            except ValueError:
                continue

            counts[payload.get("language", "other")] += 1

    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        if counts:
            flush_window(conn, current_window_start, counts, observation_start, time.time())
        consumer.close()
        conn.close()
        print("Consumer closed cleanly.")


if __name__ == "__main__":
    main()
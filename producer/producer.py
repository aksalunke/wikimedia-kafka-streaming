import json
import os
import signal
from confluent_kafka import Producer
from requests_sse import EventSource

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {
    "User-Agent": "wikimedia-kafka-streaming/1.0 (https://github.com/aksalunke/wikimedia-kafka-streaming)"
}
KAFKA_TOPIC = "wikimedia-recentchange"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Wikis that aren't tied to a single language at all
NON_LANGUAGE_WIKIS = {
    "commonswiki", "wikidatawiki", "metawiki", "mediawikiwiki",
    "specieswiki", "wikitech", "incubatorwiki", "foundationwiki",
}

# Sister-project suffixes, checked before the generic "wiki" suffix
PROJECT_SUFFIXES = [
    "wiktionary", "wikibooks", "wikinews", "wikiquote",
    "wikisource", "wikiversity", "wikivoyage", "wiki",
]


def derive_language(wiki: str) -> str:
    if wiki in NON_LANGUAGE_WIKIS:
        return "other"
    for suffix in PROJECT_SUFFIXES:
        if wiki.endswith(suffix):
            return wiki[: -len(suffix)] or "other"
    return "other"

def is_canary_event(change: dict) -> bool:
    return change.get("meta", {}).get("domain") == "canary"

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def handle_sigterm(signum, frame):
    raise KeyboardInterrupt()        

def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    sent = 0
    try:
        with EventSource(STREAM_URL, headers=HEADERS, timeout=30) as event_source:
            for event in event_source:
                if event.type != "message" or not event.data:
                    continue
                try:
                    change = json.loads(event.data)
                except ValueError:
                    continue

                if is_canary_event(change):
                    continue

                wiki = change.get("wiki", "")
                language = derive_language(wiki)

                payload = {
                    "language": language,
                    "wiki": wiki,
                    "type": change.get("type"),
                    "bot": change.get("bot"),
                    "user": change.get("user"),
                    "title": change.get("title"),
                    "timestamp": change.get("timestamp"),
                }

                producer.produce(
                    KAFKA_TOPIC,
                    key=language.encode("utf-8"),
                    value=json.dumps(payload).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                sent += 1
                if sent % 50 == 0:
                    print(f"Produced {sent} events so far...")
    finally:
        producer.flush()
        print(f"Stopped. Total produced: {sent}")


if __name__ == "__main__": 
    signal.signal(signal.SIGTERM, handle_sigterm)   
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted by user.")

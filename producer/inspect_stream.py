from requests_sse import EventSource

HEADERS = {
    "User-Agent": "wikimedia-kafka-streaming/1.0 (https://github.com/aksalunke/wikimedia-kafka-streaming)"
}

with EventSource(
    "https://stream.wikimedia.org/v2/stream/recentchange",
    headers=HEADERS,
    timeout=30,
) as event_source:
    for i, event in enumerate(event_source):
        print(f"--- event {i} ---")
        print(event)
        print([attr for attr in dir(event) if not attr.startswith("_")])
        if i >= 2:
            break
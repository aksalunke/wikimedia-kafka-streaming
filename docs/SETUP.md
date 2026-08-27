# Setup Runbook — Wikimedia Kafka Streaming Pipeline

Every phase of this build, in order, with the real commands used and the real problems hit along
the way — not a cleaned-up happy path. Written to actually be followed from an empty folder to
the finished, three-container pipeline.

**Prerequisites assumed already installed:** Python 3.13, Docker Desktop (with WSL2), VS Code,
GitHub Desktop — same toolchain as the AWS and TfL projects, nothing new to set up.

---

## Phase 1 — Repo scaffold and the broker

```powershell
cd C:\Users\aksha\Documents\GitHub
New-Item wikimedia-kafka-streaming -ItemType Directory
cd wikimedia-kafka-streaming
New-Item producer, consumer, tests, docs -ItemType Directory
code .
```

Set up as a new repository in GitHub Desktop — Python `.gitignore` template, MIT license,
initialize with a README (replaced properly in Phase 7).

`docker-compose.yml`, broker only at this stage:

```yaml
services:
  kafka:
    image: apache/kafka:4.2.0
    container_name: kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

volumes:
  kafka-data:
```

Bring it up, create the topic, and prove a message round-trips before writing any real code:

```powershell
docker compose up -d
docker compose ps
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic wikimedia-recentchange --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
"edit-test-1`nedit-test-2`nedit-test-3" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --topic wikimedia-recentchange --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic wikimedia-recentchange --from-beginning --max-messages 3 --bootstrap-server localhost:9092
```

---

## Phase 2 — Producer

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install requests-sse confluent-kafka
```

If PowerShell blocks the activation script: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned`, then retry.

Looked at real live events before writing any parsing logic (`producer/inspect_stream.py`):

```python
from requests_sse import EventSource

with EventSource("https://stream.wikimedia.org/v2/stream/recentchange", timeout=30) as event_source:
    for i, event in enumerate(event_source):
        print(f"--- event {i} ---")
        print(event)
        print([attr for attr in dir(event) if not attr.startswith("_")])
        if i >= 2:
            break
```

First run failed with a `403` — Wikimedia requires an identifying `User-Agent`; the default
`requests` one gets blocked outright. Fixed by passing `headers={"User-Agent": "..."}` into
`EventSource`. That header, and the `KAFKA_BOOTSTRAP_SERVERS`/topic constants, became the shared
setup at the top of `producer/producer.py`, built around what those three real events actually
showed: `wiki` values like `zhwikisource` and `wikidatawiki`, neither a plain language code.

Run it, then confirm real messages actually landed in Kafka:

```powershell
python producer/producer.py
```
```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic wikimedia-recentchange --from-beginning --max-messages 5 --bootstrap-server localhost:9092
```

---

## Phase 3 — Consumer

`consumer/consumer.py` — five-minute tumbling window, hand-built in memory since Kafka Streams
has no Python equivalent, flushed to SQLite with a `duration_seconds` column tracking real
observed time per window (added after noticing the first and last window of any run were
labeled as full-length but weren't).

For a fast first test, temporarily set `WINDOW_SECONDS = 30` at the top of the file instead of
`300`, so you're not waiting minutes for the first flush. Two terminals:

```powershell
python consumer/consumer.py
```
```powershell
python producer/producer.py
```

Confirm real rows landed, including a mix of partial and full windows:

```powershell
python -c "import sqlite3; [print(r) for r in sqlite3.connect('edit_velocity.db').execute('SELECT window_start, language, edit_count, duration_seconds FROM edit_velocity ORDER BY window_start, language')]"
```

Switch `WINDOW_SECONDS` back to `300` once confirmed working.

---

## Phase 4 — pytest

Two refactors made the pure logic testable in isolation — extracting `is_canary_event()` out of
`producer.py`'s `main()`, and `is_partial()` out of `consumer.py`'s `flush_window()`, plus giving
`window_start_for()` an optional `window_seconds` parameter.

`pytest.ini`:
```ini
[pytest]
pythonpath = .
```

`requirements-dev.txt`:
```
pytest
```

`tests/test_producer.py` and `tests/test_consumer.py` — 18 tests total, covering
`derive_language`, `is_canary_event`, `window_start_for`, and `is_partial`, including the real
observed edge cases (`zhwikisource`, `wikidatawiki`, an `11.4`-second partial window).

```powershell
pip install -r requirements-dev.txt
pytest -v
```

---

## Phase 5 — Dockerize producer and consumer

Split dependencies by service rather than one shared file, since `consumer.py` never imports
`requests_sse`:

`producer/requirements.txt`:
```
confluent-kafka==2.15.0
requests-sse==0.5.3
```
`consumer/requirements.txt`:
```
confluent-kafka==2.15.0
```

Both `producer/Dockerfile` and `consumer/Dockerfile` follow the same shape:
```dockerfile
FROM python:3.13-slim
WORKDIR /app
COPY <service>/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY <service>/<service>.py <service>/<service>.py
CMD ["python", "<service>/<service>.py"]
```

`.dockerignore` at the repo root:
```
.venv/
.git/
__pycache__/
*.pyc
*.db
.pytest_cache/
tests/
docs/
kafka-data/
```

Code changes to both `producer.py` and `consumer.py` — `KAFKA_BOOTSTRAP_SERVERS` (and, for the
consumer, `DB_PATH`) read from environment variables with the original values as defaults, plus a
`SIGTERM` handler that raises `KeyboardInterrupt`, since `docker compose down` sends `SIGTERM`
rather than the `SIGINT` that `Ctrl+C` sends, and Python doesn't turn `SIGTERM` into an exception
by default — without this, the existing graceful-flush logic would silently stop working the
moment either script ran inside a container.

`docker-compose.yml` gained a second broker listener — `INTERNAL` on `kafka:29092` — since
containers can't reach the broker via `localhost` the way host-run scripts could; that word means
something different inside every container. `PLAINTEXT`/`localhost:9092` stayed untouched for
host-side tools. Also added `KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'` (see below) and two new
services, `producer` and `consumer`, each gated on `kafka`'s healthcheck via `depends_on`.

**Two real problems hit bringing this up:**

1. A Dockerfile saved as `DockerFile` (wrong capitalization) built fine on Windows — NTFS is
   case-insensitive — and failed the instant Docker Desktop handed the build to WSL2's
   case-sensitive Linux filesystem. Same issue hit a `requirements` file missing its `.txt`
   extension. Fixed by renaming both, in both `producer/` and `consumer/`.
2. After the listener change, `kafka` came up unhealthy — logs showed a repeating heartbeat
   timeout against its own controller role. KRaft persists broker identity, including listener
   config, to disk on first startup and doesn't rewrite it on later restarts, so the old
   single-listener identity conflicted with the new two-listener config. Fixed by removing the
   volume entirely and letting the broker register fresh:
   ```powershell
   docker compose down
   docker volume rm wikimedia-kafka-streaming_kafka-data
   docker compose up -d --build
   ```

With a fresh volume, the topic no longer existed — and the first `--describe` after the producer
auto-created it showed `PartitionCount: 1`, not 3, confirming Kafka's default auto-create
behavior had silently reformatted it. Recreated explicitly, matching Phase 1:

```powershell
docker compose up -d kafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic wikimedia-recentchange --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker compose up -d
docker compose ps
```

`docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic wikimedia-recentchange --bootstrap-server localhost:9092` should now show `PartitionCount: 3`, and all three containers `Up`/`healthy`.

---

## Phase 6 — CI

```powershell
mkdir .github
mkdir .github\workflows
New-Item .github\workflows\ci.yml -ItemType File
```

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Check out repository
        uses: actions/checkout@v7

      - name: Set up Python
        uses: actions/setup-python@v7
        with:
          python-version: "3.13"

      - name: Install dependencies
        run: |
          pip install -r producer/requirements.txt
          pip install -r consumer/requirements.txt
          pip install -r requirements-dev.txt

      - name: Run pytest
        run: pytest -v
```

Installs from both service-specific requirements files, not just one — importing `producer.py`
or `consumer.py` at all requires their real dependencies, even though the tests themselves never
touch a live broker.

---

## Phase 7 — README and documentation

Final `README.md` — architecture diagram, real findings pulled from `docs/data-notes.md`, both
run paths (Docker Compose and local dev), and a "what I'd do differently in production" section.
`docs/architecture-decisions.md` and `docs/data-notes.md` were written incrementally throughout
every phase above, not reconstructed afterward — each entry added at the point the real decision
or bug actually happened.

---

## Verifying the whole thing from scratch

```powershell
docker compose down
docker volume rm wikimedia-kafka-streaming_kafka-data
docker compose up -d kafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic wikimedia-recentchange --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker compose up -d --build
docker compose ps
pytest -v
```

Three healthy/running containers, an 18-test pytest pass, and a topic showing `PartitionCount: 3`
— the actual, reproducible finish line.
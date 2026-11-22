"""Shared fixtures.

Everything runs in-process: no Docker, Kafka, Snapchain node, OpenAI key or
model download. Postgres is a real server (embedded via ``pgserver``, or the one
in ``OZI_TEST_POSTGRES_URL``); Qdrant is the real client in ``:memory:`` mode.
"""

import os
import sys
import tempfile
import types
from urllib.parse import unquote, urlparse

import psycopg2
import pytest

from tests import fakes

# The semantic consumer and the API load the e5 model when they are imported.
# Swap in the fake before anything imports them, so torch is never needed.
sys.modules["sentence_transformers"] = types.SimpleNamespace(
    SentenceTransformer=fakes.FakeSentenceTransformer
)

# langdetect is random unless seeded.
from langdetect import DetectorFactory  # noqa: E402

DetectorFactory.seed = 0

# --------------------------------------------------------------------------- #
# Postgres
# --------------------------------------------------------------------------- #


def _start_embedded_postgres():
    try:
        import pgserver
    except ImportError:
        pytest.skip("pgserver is not installed and OZI_TEST_POSTGRES_URL is not set")
    # Short path: the socket lives in this directory and macOS caps socket paths
    # at 104 characters, which pytest's tmp_path can exceed.
    server = pgserver.get_server(tempfile.mkdtemp(prefix="ozi-pg-"), cleanup_mode="delete")
    server.psql("CREATE DATABASE ozi_test;")
    info = server.get_postmaster_info()
    return server, dict(
        host=str(info.socket_dir),
        port=str(info.port),
        user="postgres",
        password="",
        dbname="ozi_test",
    )


def _postgres_from_url(url):
    u = urlparse(url)
    return dict(
        host=u.hostname or "localhost",
        port=str(u.port or 5432),
        user=unquote(u.username or ""),
        password=unquote(u.password or ""),
        dbname=u.path.lstrip("/") or "ozi_test",
    )


@pytest.fixture(scope="session")
def pg_params():
    """Connection settings for the test database, also exported as the
    POSTGRES_* variables the consumers and the API read."""
    url = os.getenv("OZI_TEST_POSTGRES_URL")
    server = None
    if url:
        # Every test drops and re-creates the public schema of this database.
        params = _postgres_from_url(url)
    else:
        server, params = _start_embedded_postgres()

    env = {
        "POSTGRES_HOST": params["host"],
        "POSTGRES_PORT": params["port"],
        "POSTGRES_USER": params["user"],
        "POSTGRES_PASSWORD": params["password"],
        "POSTGRES_DB": params["dbname"],
        "OPENAI_API_KEY": "test-key-not-used",
    }
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    yield params
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    if server is not None:
        server.cleanup()


@pytest.fixture
def db(pg_params):
    """A connection to an empty database: the public schema is re-created
    before each test, and the services create their own tables."""
    conn = psycopg2.connect(**pg_params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
    yield conn
    conn.close()


def query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# --------------------------------------------------------------------------- #
# Kafka, Qdrant and the services
# --------------------------------------------------------------------------- #


@pytest.fixture
def kafka(monkeypatch):
    """One in-memory broker, wired into the poller and both consumers."""
    import consumer as analytics_consumer  # analytics/consumer.py
    import poller
    from consumers import consumer as semantic_consumer

    broker = fakes.FakeKafka()
    monkeypatch.setattr(poller, "KafkaProducer", broker.producer)
    monkeypatch.setattr(analytics_consumer, "KafkaConsumer", broker.consumer)
    monkeypatch.setattr(semantic_consumer, "KafkaConsumer", broker.consumer)
    return broker


@pytest.fixture
def qdrant(monkeypatch):
    from qdrant_client import QdrantClient

    from consumers import consumer as semantic_consumer

    client = QdrantClient(":memory:")
    monkeypatch.setattr(semantic_consumer, "QdrantClient", lambda *a, **kw: client)
    return client


def run_poller(events):
    """Run the real ``SnapchainGRPCPoller.run()`` over ``events``."""
    import poller

    p = poller.SnapchainGRPCPoller()
    p.stub = fakes.FakeHubStub(events)
    p.connect = lambda: None  # keep the fake stub if the poller reconnects
    with pytest.raises(fakes.StreamEnded):
        p.run()
    return p


class Pipeline:
    """Snapchain events → poller → Kafka → analytics consumer (Postgres) and
    semantic consumer (Qdrant), each running its real ``consume()`` loop."""

    def __init__(self, kafka, db, qdrant):
        self.kafka, self.db, self.qdrant = kafka, db, qdrant

    def publish(self, events):
        return run_poller(events)

    def consume(self):
        import consumer as analytics_consumer
        from consumers import consumer as semantic_consumer
        from consumers.config import ANALYTICS_TOPIC, EMBEDDER_TOPIC

        # Same topics as analytics/consumer.py's __main__.
        analytics_consumer.KafkaAnalyticsConsumer(
            topics=[ANALYTICS_TOPIC, EMBEDDER_TOPIC]
        ).consume()
        semantic_consumer.KafkaEmbedConsumer(
            ["localhost:6333"], topics=[EMBEDDER_TOPIC]
        ).consume()

    def run(self, events):
        self.publish(events)
        self.consume()

    def query(self, sql, params=None):
        return query(self.db, sql, params)


@pytest.fixture
def pipeline(kafka, db, qdrant, monkeypatch):
    from consumers import consumer as semantic_consumer

    # The semantic consumer writes only when its buffer reaches BATCH_SIZE (100)
    # and has no flush when the stream ends (issue #10), so a 36-cast sample would never be
    # written. Writing every message keeps the test about routing and storage.
    monkeypatch.setattr(semantic_consumer, "BATCH_SIZE", 1)
    return Pipeline(kafka, db, qdrant)

"""In-memory stand-ins for the services the pipeline talks to.

Only the boundaries are faked: Snapchain's gRPC stream, Kafka, the embedding
model, OpenAI and Redis. The poller, both consumers, Postgres, Qdrant and the
FastAPI app are the real code (Postgres is an embedded server, Qdrant runs in
its in-process ``:memory:`` mode).
"""

import hashlib
import re
from collections import namedtuple

import numpy as np

# --------------------------------------------------------------------------- #
# Snapchain
# --------------------------------------------------------------------------- #


class StreamEnded(Exception):
    """Raised by FakeHubStub once its events are used up.

    ``SnapchainGRPCPoller.poll`` re-subscribes forever after a stream ends, so
    the fake raises this on the second ``Subscribe`` to stop ``run()``.
    """


class FakeHubStub:
    """Replaces ``rpc_pb2_grpc.HubServiceStub``: streams a fixed list of events."""

    def __init__(self, events):
        self.events = list(events)
        self.requests = []

    def Subscribe(self, request, metadata=None):
        self.requests.append(request)
        if len(self.requests) > 1:
            raise StreamEnded()
        return iter(self.events)


# --------------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------------- #

KafkaRecord = namedtuple("KafkaRecord", "topic value")


class FakeKafka:
    """A single in-memory broker shared by the producer and consumers.

    Messages are kept in one log in publish order, so every consumer sees the
    order the poller produced, across topics, like a single-partition topic.
    Values are serialized and deserialized with the callers' own functions.
    """

    def __init__(self):
        self.log = []  # [(topic, bytes)]
        self.keys = []  # the message key of each log entry, serialized

    def messages(self, topic):
        return [value for t, value in self.log if t == topic]

    # Factories with the same signatures as kafka-python's classes.
    def producer(
        self, bootstrap_servers=None, value_serializer=None, key_serializer=None, **_
    ):
        return _FakeProducer(
            self, value_serializer or (lambda v: v), key_serializer or (lambda k: k)
        )

    def consumer(self, *topics, value_deserializer=None, **_):
        return _FakeConsumer(self, topics, value_deserializer or (lambda v: v))


class _FakeProducer:
    def __init__(self, broker, serializer, key_serializer):
        self.broker = broker
        self.serializer = serializer
        self.key_serializer = key_serializer

    def send(self, topic, value=None, key=None, **_):
        self.broker.log.append((topic, self.serializer(value)))
        self.broker.keys.append(self.key_serializer(key))

    def flush(self, *_, **__):
        pass


class _FakeConsumer:
    """Iterates over what is in the log when iteration starts, then stops."""

    def __init__(self, broker, topics, deserializer):
        self.broker = broker
        self.topics = set(topics)
        self.deserializer = deserializer

    def __iter__(self):
        for topic, raw in list(self.broker.log):
            if topic in self.topics:
                yield KafkaRecord(topic, self.deserializer(raw))


# --------------------------------------------------------------------------- #
# Embedding model
# --------------------------------------------------------------------------- #


class FakeSentenceTransformer:
    """Deterministic bag-of-words embedder with the e5 model's 768 dimensions.

    Texts that share words get a high cosine similarity, so semantic search
    returns sensible hits without downloading torch or the real model.
    """

    DIM = 768

    def __init__(self, *_, **__):
        pass

    def _embed(self, text):
        vec = np.zeros(self.DIM, dtype=np.float32)
        for word in re.findall(r"\w+", (text or "").lower()):
            digest = hashlib.md5(word.encode()).digest()
            vec[int.from_bytes(digest[:4], "little") % self.DIM] += 1.0
        if not vec.any():
            vec[0] = 1.0
        return vec / np.linalg.norm(vec)

    def encode(self, sentences, **_):
        if isinstance(sentences, str):
            return self._embed(sentences)
        return np.stack([self._embed(s) for s in sentences])


# --------------------------------------------------------------------------- #
# OpenAI
# --------------------------------------------------------------------------- #


class FakeLLM:
    """Replaces the API's ``llm_client``.

    It builds the prompts the API is expected to send with the real prompt
    builders and answers each one; any other prompt is treated as the SQL
    summary request. Every call is recorded in ``calls`` as (kind, prompt).
    """

    def __init__(self):
        self.calls = []
        self.script = None

    def expect(self, request, classification, sql=None, summary="fake summary"):
        self.script = dict(
            request=request, classification=classification, sql=sql, summary=summary
        )

    async def ask(self, prompt):
        from api.utils.llm_prompts import classify_prompt, sql_generation_prompt

        s = self.script
        req = s["request"]
        if prompt == classify_prompt(req["query"]):
            self.calls.append(("classify", prompt))
            return s["classification"]
        if prompt == sql_generation_prompt(
            req["query"], fid=req["fid"], mode=req["search_mode"]
        ):
            self.calls.append(("sql", prompt))
            return s["sql"]
        self.calls.append(("sql_summary", prompt))
        return s["summary"]

    async def summarize(self, user_query, documents):
        self.calls.append(("semantic_summary", (user_query, list(documents))))
        return self.script["summary"]


# --------------------------------------------------------------------------- #
# Redis
# --------------------------------------------------------------------------- #


class FakeRedis:
    """The subset of ``redis.asyncio.Redis`` the API uses."""

    def __init__(self):
        self.store = {}
        self.ttls = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = value
        self.ttls[key] = ex

    async def close(self):
        pass

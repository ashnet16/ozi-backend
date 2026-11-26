import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone

import emoji
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointIdsList, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

from consumers.base import Consumer
from consumers.config import (
    BATCH_SIZE,
    CAST_ADD_MSG,
    CAST_REMOVE_MSG,
    EMBEDDER_TOPIC,
    FLUSH_INTERVAL_MS,
)
from consumers.utils import farcaster_ts_to_datetime, to_hex

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

URL_REGEX = re.compile(r"http\S+|www\.\S+")

def normalize_cast(text):
    if not text:
        return ""
    text = emoji.demojize(text, delimiters=(":", ":"))
    text = re.sub(URL_REGEX, "", text)
    return text.strip().lower()

def point_id(message_hash_hex):
    """Qdrant point id of a cast. Derived from the hash, so a replayed cast
    overwrites its own point and a cast remove can find it without a lookup."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, message_hash_hex))


def removed_cast_hashes(messages):
    """Hex hashes of the casts that the CAST_REMOVE messages in ``messages`` delete."""
    removed = set()
    for msg in messages:
        if msg.get("message_type") != CAST_REMOVE_MSG:
            continue
        target = to_hex(msg.get("cast_remove", {}).get("target_hash"))
        if target:
            removed.add(target)
        removed.update(to_hex(m.get("hash")) for m in msg.get("removed", []))
    removed.discard("")
    return removed


def set_defaults(record):
    safe_record = {}
    for k, v in record.items():
        if v is None:
            safe_record[k] = ""
        elif isinstance(v, (str, int, float, bool)):
            safe_record[k] = v
        elif isinstance(v, (list, dict)):
            safe_record[k] = json.dumps(v)
        else:
            safe_record[k] = str(v)
    return safe_record

class KafkaEmbedConsumer(Consumer):
    def __init__(self, embedder_hosts, topics, group_id="ozi-embed-consumer-group"):
        super().__init__(end_point=embedder_hosts[0], topics=topics, consumer_group=group_id)

        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=["kafka:29092"],
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            # Ends iteration after this long without a message, so the buffer gets written.
            consumer_timeout_ms=FLUSH_INTERVAL_MS,
        )

        self.embed_model = SentenceTransformer("intfloat/multilingual-e5-base")
        self.qdrant = QdrantClient(host="qdrant", port=6333)
        self.collection_name = "farcaster_casts"
        self.ensure_collection_exists()

    def ensure_collection_exists(self):
        try:
            existing = {c.name for c in self.qdrant.get_collections().collections}
            if self.collection_name in existing:
                logger.info("Collection %s already exists.", self.collection_name)
                return
            self.qdrant.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=768, distance=Distance.COSINE),
            )
            logger.info("Collection %s created.", self.collection_name)
        except Exception as e:
            logger.warning("Failed to ensure Qdrant collection: %s", str(e))

    def process(self, messages):
        """Records to embed for the cast adds in ``messages``, leaving out any cast
        that a CAST_REMOVE in the same messages deletes, before or after it."""
        records = []
        consumer_ts = datetime.now(timezone.utc).isoformat()
        removed = removed_cast_hashes(messages)

        for msg in messages:
            if msg.get("message_type") != CAST_ADD_MSG:
                continue

            cast = msg.get("cast", {})
            text = cast.get("text")
            language = cast.get("language", "")
            fid = msg.get("fid")
            timestamp = farcaster_ts_to_datetime(msg.get("timestamp")).isoformat()
            message_hash = msg.get("message_hash")
            message_hash_hex = to_hex(message_hash)

            raw_data = (
                msg.get("raw", {})
                .get("merge_message_body", {})
                .get("message", {})
                .get("data", {})
            )

            cast_body = raw_data.get("cast_add_body", {})
            parent_cast = cast_body.get("parent_cast_id", {})
            parent_url = cast_body.get("parent_url", cast.get("parent_url"))
            parent_hash_b64 = parent_cast.get("hash") or cast.get("parent_hash")
            parent_hash_hex = to_hex(parent_hash_b64)

            is_comment = bool(parent_cast or parent_url or parent_hash_b64)

            if not (text and fid and message_hash):
                continue
            if message_hash_hex in removed:
                continue

            record = {
                "fid": fid,
                "timestamp": timestamp,
                "idx_timestamp": consumer_ts,
                "text": normalize_cast(text),
                "language": language,
                "mentions": cast.get("mentions", []),
                "embeds": cast.get("embeds", []),
                "message_hash": message_hash,
                "message_hash_hex": message_hash_hex,
                "consumer_ts": consumer_ts,
                "is_comment": is_comment,
                "vector_id": point_id(message_hash_hex),
                "parent_fid": parent_cast.get("fid", "") if parent_cast else "",
                "parent_hash": parent_hash_b64 or "",
                "parent_hash_hex": parent_hash_hex,
            }

            records.append(record)

        return records

    def write_to_qdrant(self, records):
        texts = [r["text"] for r in records]
        metadatas = [set_defaults(r) for r in records]
        ids = [r["vector_id"] for r in records]

        vectors = self.embed_model.encode(
            texts, batch_size=BATCH_SIZE, show_progress_bar=False
        ).tolist()

        points = [
            PointStruct(id=ids[i], vector=vectors[i], payload=metadatas[i])
            for i in range(len(records))
        ]

        self.qdrant.upsert(collection_name=self.collection_name, points=points)
        logger.info("Successfully wrote %d embeddings to Qdrant.", len(points))

    def delete_from_qdrant(self, message_hashes):
        """Delete the points of removed casts. A cast that has no point (never
        embedded, or not written yet) is skipped without an error."""
        # Only existing ids: the in-process Qdrant raises on an unknown one.
        existing = [
            p.id
            for p in self.qdrant.retrieve(
                collection_name=self.collection_name,
                ids=[point_id(h) for h in message_hashes],
                with_payload=False,
                with_vectors=False,
            )
        ]
        if existing:
            self.qdrant.delete(
                collection_name=self.collection_name,
                points_selector=PointIdsList(points=existing),
            )
        logger.info(
            "Deleted %d of %d removed casts from Qdrant; the rest had no point.",
            len(existing),
            len(message_hashes),
        )

    def flush(self, buffer):
        """Embed and write the buffered casts, and delete the points of removed
        ones. Returns what is left: the same buffer if a Qdrant call failed (both
        are idempotent, so it is retried next flush), else []."""
        if not buffer:
            return buffer
        records = self.process(buffer)
        removed = removed_cast_hashes(buffer)
        if not (records or removed):
            return []  # nothing in it to embed or delete
        try:
            if records:
                self.write_to_qdrant(records)
            if removed:
                self.delete_from_qdrant(sorted(removed))
            return []
        except Exception as e:
            logger.error("Retrying Qdrant write next round: %s", str(e))
            return buffer

    def consume(self, stop_when_idle=False):
        """Read the topics forever. The buffer is written when it reaches BATCH_SIZE,
        when FLUSH_INTERVAL_MS has passed since the last write, and when no message
        has arrived for FLUSH_INTERVAL_MS. With ``stop_when_idle``, return after
        that first quiet spell instead of waiting for more."""
        logger.info("Started Kafka Embedder Consumer on topics: %s", self.topics)
        interval = FLUSH_INTERVAL_MS / 1000
        buffer = []
        last_flush = time.monotonic()
        while True:
            for message in self.consumer:
                try:
                    buffer.append(message.value)
                    if len(buffer) >= BATCH_SIZE or time.monotonic() - last_flush >= interval:
                        buffer = self.flush(buffer)
                        last_flush = time.monotonic()
                except KafkaError as e:
                    logger.error("Kafka error: %s", str(e))
                except Exception as e:
                    logger.error("Unexpected error: %s", str(e))
            # The topic has been quiet for FLUSH_INTERVAL_MS.
            try:
                buffer = self.flush(buffer)
            except Exception as e:
                logger.error("Unexpected error: %s", str(e))
            last_flush = time.monotonic()
            if stop_when_idle:
                return


if __name__ == "__main__":
    embedder_hosts = ["localhost:6333"]
    consumer = KafkaEmbedConsumer(embedder_hosts, topics=[EMBEDDER_TOPIC])
    consumer.consume()

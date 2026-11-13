import base64
import json
import logging
import re
import uuid
from datetime import datetime, timezone

import emoji
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

from consumers.base import Consumer
from consumers.config import BATCH_SIZE, CAST_ADD_MSG, EMBEDDER_TOPIC

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

URL_REGEX = re.compile(r"http\S+|www\.\S+")

def normalize_cast(text):
    if not text:
        return ""
    text = emoji.demojize(text, delimiters=(":", ":"))
    text = re.sub(URL_REGEX, "", text)
    return text.strip().lower()

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

def safe_unix_timestamp(ts):
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except Exception:
        logger.warning(f"Failed to parse timestamp: {ts}")
        return datetime.now(tz=timezone.utc).isoformat()

def b64_to_hex(b64_str):
    try:
        return base64.b64decode(b64_str + "==").hex()
    except Exception:
        return ""

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
        )

        self.embed_model = SentenceTransformer("intfloat/multilingual-e5-base")
        self.qdrant = QdrantClient(host="qdrant", port=6333)
        self.collection_name = "farcaster_casts"
        self.ensure_collection_exists()

    def ensure_collection_exists(self):
        try:
            self.qdrant.recreate_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=768, distance=Distance.COSINE),
            )
            logger.info("Collection %s created or reset.", self.collection_name)
        except Exception as e:
            logger.warning("Failed to create or reset Qdrant collection: %s", str(e))

    def process(self, messages):
        records = []
        consumer_ts = datetime.now(timezone.utc).isoformat()

        for msg in messages:
            if msg.get("message_type") != CAST_ADD_MSG:
                continue

            cast = msg.get("cast", {})
            text = cast.get("text")
            language = cast.get("language", "")
            fid = msg.get("fid")
            timestamp = safe_unix_timestamp(msg.get("timestamp"))
            message_hash = msg.get("message_hash")
            message_hash_hex = b64_to_hex(message_hash)

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
            parent_hash_hex = b64_to_hex(parent_hash_b64) if parent_hash_b64 else ""

            is_comment = bool(parent_cast or parent_url or parent_hash_b64)

            if not (text and fid and message_hash):
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
                "vector_id": str(uuid.uuid4()),
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

    def consume(self):
        logger.info("Started Kafka Embedder Consumer on topics: %s", self.topics)
        buffer = []

        for message in self.consumer:
            try:
                buffer.append(message.value)

                if len(buffer) >= BATCH_SIZE:
                    records = self.process(buffer)
                    if records:
                        try:
                            self.write_to_qdrant(records)
                            buffer = []  # Only clear on success
                        except Exception as e:
                            logger.error("Retrying Qdrant write next round: %s", str(e))
                            # Optionally log to DLQ or file here

            except KafkaError as e:
                logger.error("Kafka error: %s", str(e))
            except Exception as e:
                logger.error("Unexpected error: %s", str(e))


if __name__ == "__main__":
    embedder_hosts = ["localhost:6333"]
    consumer = KafkaEmbedConsumer(embedder_hosts, topics=[EMBEDDER_TOPIC])
    consumer.consume()

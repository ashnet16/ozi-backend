import base64
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg2 import OperationalError
from table_queries import (
    create_casts_table,
    create_comments_table,
    create_reactions_table,
)

from consumers.base import Consumer
from consumers.config import ANALYTICS_TOPIC, BATCH_SIZE, EMBEDDER_TOPIC

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

FARCASTER_EPOCH = datetime(2021, 10, 1, tzinfo=timezone.utc)


def to_pg_ts(farcaster_ts: int) -> datetime:
    try:
        return FARCASTER_EPOCH + timedelta(seconds=int(farcaster_ts))
    except Exception:
        return datetime.now(timezone.utc)


def b64_to_hex(b64_str):
    try:
        return base64.b64decode(b64_str + "==").hex()
    except Exception:
        return ""


class KafkaAnalyticsConsumer(Consumer):
    def __init__(
        self, topics=[ANALYTICS_TOPIC], group_id="ozi-analytics-consumer-group"
    ):
        super().__init__(
            end_point="kafka:29092", topics=topics, consumer_group=group_id
        )
        self.topics = topics
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=[self.end_point],
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )

        self.cast_buffer, self.comment_buffer, self.reaction_buffer = [], [], []
        self.connect_to_db()
        self.ensure_tables_exist()

    def connect_to_db(self):
        while True:
            try:
                self.conn = psycopg2.connect(
                    dbname=os.getenv("POSTGRES_DB", "ozi"),
                    user=os.environ["POSTGRES_USER"],
                    password=os.environ["POSTGRES_PASSWORD"],
                    host=os.getenv("POSTGRES_HOST", "postgres"),
                    port=os.getenv("POSTGRES_PORT", "5432"),
                )
                self.conn.autocommit = True
                logger.info("Connected to PostgreSQL.")
                break
            except OperationalError as e:
                logger.error("PostgreSQL connection failed, retrying in 5s: %s", e)
                time.sleep(5)

    def ensure_tables_exist(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_casts_table())
                cur.execute(create_comments_table())
                cur.execute(create_reactions_table())
            logger.info("PostgreSQL tables ready.")
        except Exception as e:
            logger.error("Failed to ensure tables exist: %s", e)

    def process(self, msg):
        logger.info(f"[ANALYTICS] Received message: {json.dumps(msg)[:200]}...")
        try:
            event_type = msg.get("message_subtype")
            if event_type == "CAST_ADD":
                parent_cast = (
                    msg.get("raw", {})
                    .get("merge_message_body", {})
                    .get("message", {})
                    .get("data", {})
                    .get("cast_add_body", {})
                    .get("parent_cast_id")
                )
                if parent_cast:
                    self.comment_buffer.append(self.prepare_comment_record(msg))
                else:
                    self.cast_buffer.append(self.prepare_cast_record(msg))
            elif event_type == "REACTION_ADD":
                self.reaction_buffer.append(self.prepare_reaction_record(msg))
            else:
                logger.warning(f"Unknown event_type: {event_type}")
        except Exception as e:
            logger.error(f"Failed to process message: {str(e)}")

    def prepare_cast_record(self, msg):
        msg_raw = msg["raw"]
        msg_body = msg_raw["merge_message_body"]["message"]["data"]
        cast_info = msg.get("cast", {})
        ts = to_pg_ts(msg_body.get("timestamp"))
        return (
            int(msg_raw["id"]),
            msg.get("message_subtype", ""),
            int(msg["fid"]),
            ts,
            cast_info.get("text", ""),
            cast_info.get("language", ""),
            json.dumps(cast_info.get("embeds", [])),
            json.dumps(cast_info.get("mentions", [])),
            msg.get("message_hash", ""),
            b64_to_hex(msg.get("message_hash", "")),
            datetime.now(timezone.utc),
        )

    def prepare_comment_record(self, msg):
        msg_raw = msg["raw"]
        msg_body = msg_raw["merge_message_body"]["message"]["data"]
        cast_add_body = msg_body.get("cast_add_body", {})
        ts = to_pg_ts(msg_body.get("timestamp"))
        parent_hash_b64 = cast_add_body.get("parent_cast_id", {}).get("hash", "")
        return (
            int(msg_raw["id"]),
            msg.get("message_subtype", ""),
            int(msg["fid"]),
            ts,
            int(cast_add_body.get("parent_cast_id", {}).get("fid", 0)),
            parent_hash_b64,
            b64_to_hex(parent_hash_b64),
            cast_add_body.get("text", ""),
            msg.get("cast", {}).get("language", ""),
            json.dumps(msg.get("cast", {}).get("embeds", [])),
            msg.get("message_hash", ""),
            b64_to_hex(msg.get("message_hash", "")),
            datetime.now(timezone.utc),
        )

    def prepare_reaction_record(self, msg):
        msg_raw = msg["raw"]
        msg_body = msg_raw["merge_message_body"]["message"]["data"]
        ts = to_pg_ts(msg_body.get("timestamp"))
        target_hash_b64 = msg["reaction"].get("target_hash", "")
        return (
            int(msg_raw["id"]),
            msg.get("message_subtype", ""),
            int(msg["fid"]),
            ts,
            msg["reaction"].get("type", ""),
            int(msg["reaction"].get("target_fid", 0)),
            target_hash_b64,
            b64_to_hex(target_hash_b64),
            msg.get("message_hash", ""),
            b64_to_hex(msg.get("message_hash", "")),
            datetime.now(timezone.utc),
        )

    def bulk_insert_casts(self):
        if not self.cast_buffer:
            return
        try:
            with self.conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO casts (id, msg_type, fid, msg_timestamp, text, lang, embeds, mentions, msg_hash, msg_hash_hex, idx_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """,
                    self.cast_buffer,
                )
            logger.info(f"Bulk inserted {len(self.cast_buffer)} casts.")
            self.cast_buffer = []
        except Exception as e:
            logger.error(f"Failed to insert casts (will retry): {str(e)}")

    def bulk_insert_comments(self):
        if not self.comment_buffer:
            return
        try:
            with self.conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO comments (id, msg_type, fid, msg_timestamp, parent_fid, parent_hash, parent_hash_hex, text, lang, embeds, msg_hash, msg_hash_hex, idx_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """,
                    self.comment_buffer,
                )
            logger.info(f"Bulk inserted {len(self.comment_buffer)} comments.")
            self.comment_buffer = []
        except Exception as e:
            logger.error(f"Failed to insert comments (will retry): {str(e)}")

    def bulk_insert_reactions(self):
        if not self.reaction_buffer:
            return
        try:
            with self.conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO reactions (id, msg_type, fid, msg_timestamp, reaction_type, target_fid, target_hash, target_hash_hex, msg_hash, msg_hash_hex, idx_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """,
                    self.reaction_buffer,
                )
            logger.info(f"Bulk inserted {len(self.reaction_buffer)} reactions.")
            self.reaction_buffer = []
        except Exception as e:
            logger.error(f"Failed to insert reactions (will retry): {str(e)}")

    def flush_buffers(self):
        self.bulk_insert_casts()
        self.bulk_insert_comments()
        self.bulk_insert_reactions()

    def consume(self):
        logger.info("Starting Kafka analytics consumer for topics: %s", self.topics)
        for message in self.consumer:
            try:
                self.process(message.value)
                if (
                    len(self.cast_buffer) >= BATCH_SIZE
                    or len(self.comment_buffer) >= BATCH_SIZE
                    or len(self.reaction_buffer) >= BATCH_SIZE
                ):
                    self.flush_buffers()
            except OperationalError:
                logger.warning("Lost PostgreSQL connection, reconnecting...")
                self.connect_to_db()
            except KafkaError as e:
                logger.error("Kafka error: %s", str(e))
            except Exception as e:
                logger.error("Unexpected error: %s", str(e))
        self.flush_buffers()


if __name__ == "__main__":
    consumer = KafkaAnalyticsConsumer(topics=[ANALYTICS_TOPIC, EMBEDDER_TOPIC])
    consumer.consume()
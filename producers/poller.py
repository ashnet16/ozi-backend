import json
import logging
import os
import time

import grpc
from base import EventTransformer, KafkaTopic, Poller
from google.protobuf.json_format import MessageToDict
from handlers import HANDLERS, Topic
from kafka import KafkaProducer
from proto import hub_event_pb2, message_pb2, request_response_pb2, rpc_pb2_grpc

SNAPCHAIN_SUBSCRIBE_ENDPOINT = os.getenv(
    "SNAPCHAIN_ENDPOINT", "host.docker.internal:3383"
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class KafkaJSONTopic(KafkaTopic):
    def __init__(self, topic_name, bootstrap_servers="kafka:29092", topic_type=None):
        super().__init__(topic_name, topic_type)
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: None if k is None else str(k).encode("utf-8"),
            linger_ms=100,
            batch_size=16384,
        )

    def produce(self, event, topic_name=None, key=None):
        target_topic = topic_name or self.topic_name
        try:
            self.producer.send(target_topic, key=key, value=event)
            self.producer.flush()
            logger.info("Sent event to Kafka topic: %s", target_topic)
        except Exception as e:
            logger.error("Failed to send to Kafka: %s", e)


def _env_flag(name):
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def open_channel(end_point, use_tls=False):
    """Open a gRPC channel to a Snapchain node.

    The endpoint is `host:port`. An `https://` prefix (or SNAPCHAIN_USE_TLS)
    selects TLS, for hosted nodes; a node on your own machine is plaintext.
    """
    if end_point.startswith("https://"):
        end_point, use_tls = end_point[len("https://") :], True
    elif end_point.startswith("http://"):
        end_point = end_point[len("http://") :]
    end_point = end_point.rstrip("/")

    if use_tls:
        return grpc.secure_channel(end_point, grpc.ssl_channel_credentials())
    return grpc.insecure_channel(end_point)


class SnapchainGRPCPoller(Poller):
    def __init__(self, end_point=SNAPCHAIN_SUBSCRIBE_ENDPOINT):
        super().__init__(end_point, "snapchain_grpc")
        self.end_point = end_point
        # Hosted nodes usually want an API key, sent as a request header.
        api_key = os.getenv("SNAPCHAIN_API_KEY", "").strip()
        header = os.getenv("SNAPCHAIN_API_KEY_HEADER", "x-api-key").strip().lower()
        self.metadata = [(header, api_key)] if api_key else None
        self.channel = None
        self.connect()
        self.topics = {
            topic: KafkaJSONTopic(topic.value, topic_type=topic.name.lower())
            for topic in Topic
        }

    def connect(self):
        """(Re)open the channel. A channel that has failed for a long time backs
        off and can keep reporting a stale error after the node is reachable."""
        if self.channel is not None:
            self.channel.close()
        self.channel = open_channel(self.end_point, _env_flag("SNAPCHAIN_USE_TLS"))
        self.stub = rpc_pb2_grpc.HubServiceStub(self.channel)

    def poll(self):
        request = request_response_pb2.SubscribeRequest(
            event_types=[hub_event_pb2.HUB_EVENT_TYPE_MERGE_MESSAGE]
        )
        while True:
            try:
                for event in self.stub.Subscribe(request, metadata=self.metadata):
                    yield event
            except grpc.RpcError as e:
                logger.warning("Stream error: %s. Reconnecting in 3s...", e)
                time.sleep(3)
                self.connect()

    def publish(self, event, topic):
        # Keyed on the author, so each fid's messages share a partition and are
        # consumed in the order Snapchain merged them: a remove after its add.
        self.topics[topic].produce(event, key=event.get("fid"))

    def run(self):
        logger.info("Starting Snapchain gRPC Poller...")
        for event in self.poll():
            try:
                transformed_event, topic = FarcasterMessageTransformer(event).transform()
                print(transformed_event)
                self.publish(transformed_event, topic)
            except Exception as e:
                logger.error("Failed to transform/publish event: %s", str(e))


def message_subtype(message_type_enum):
    """``MESSAGE_TYPE_CAST_ADD`` -> ``CAST_ADD``."""
    return message_pb2.MessageType.Name(message_type_enum).replace("MESSAGE_TYPE_", "")


def removed_messages(event):
    """The messages this merge displaced, from the event's ``deleted_messages``:
    the cast a CAST_REMOVE deletes, the like a REACTION_REMOVE undoes, or the
    remove a re-added like replaces. Empty when nothing was displaced."""
    return [
        {
            "hash": m.hash.hex(),
            "message_type": message_subtype(m.data.type),
            "fid": m.data.fid,
        }
        for m in event.merge_message_body.deleted_messages
    ]


def dead_letter(envelope, reason):
    envelope["dlq_reason"] = reason
    return envelope, Topic.DLQ


class FarcasterMessageTransformer(EventTransformer):
    """Builds the envelope every message shares and hands the type-specific part
    to the handler registered for the message type (see ``handlers``)."""

    def __init__(self, event, handlers=HANDLERS):
        super().__init__(event)
        self.handlers = handlers

    def transform(self):
        """Returns ``(envelope, topic)``."""
        event_type = hub_event_pb2.HubEventType.Name(self.event.type)
        envelope = {"event_type": event_type, "schema_version": "v1"}
        logger.debug("Transforming event: %s", event_type)

        try:
            if not (
                self.event.type == hub_event_pb2.HUB_EVENT_TYPE_MERGE_MESSAGE
                and self.event.HasField("merge_message_body")
            ):
                envelope["raw"] = self._raw()
                return dead_letter(
                    envelope, f"Unhandled HubEventType or missing field: {event_type}"
                )

            msg = self.event.merge_message_body.message
            data = msg.data
            msg_type = message_pb2.MessageType.Name(data.type)
            envelope.update(
                {
                    "message_type": msg_type,
                    "message_subtype": message_subtype(data.type),
                    "fid": data.fid,
                    "timestamp": data.timestamp,
                    "message_hash": msg.hash.hex(),
                    "raw": self._raw(),
                }
            )
        except Exception as e:
            return self._failed(envelope, e)

        handler = self.handlers.get(data.type)
        if handler is None:
            envelope["data"] = MessageToDict(data, preserving_proto_field_name=True)
            return dead_letter(envelope, f"Unhandled MESSAGE_TYPE: {msg_type}")

        try:
            reason = handler.validate(data)
            if reason:
                return dead_letter(envelope, reason)
            envelope[handler.payload_key] = handler.payload(data)
            envelope["removed"] = removed_messages(self.event)
            return envelope, handler.topic
        except Exception as e:
            return self._failed(envelope, e, f"{type(handler).__name__} failed: ")

    def _raw(self):
        return MessageToDict(self.event, preserving_proto_field_name=True)

    def _failed(self, envelope, error, context=""):
        logger.exception("Failed to transform Farcaster/Snapchain event")
        envelope["error"] = f"Transformation failed: {error}"
        try:
            envelope["raw"] = self._raw()
        except Exception:
            pass
        return dead_letter(envelope, f"Transformation error: {context}{error}")


if __name__ == "__main__":
    SnapchainGRPCPoller().run()

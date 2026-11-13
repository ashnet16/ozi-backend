import json
import logging
import time

import grpc
from base import EventTransformer, KafkaTopic, Poller
from google.protobuf.json_format import MessageToDict
from kafka import KafkaProducer
from langdetect import detect
from proto import hub_event_pb2, message_pb2, request_response_pb2, rpc_pb2_grpc

from producers.config import (
    ANALYTICS_TOPIC,
    CAST_ADD_MSG,
    DLQ_TOPIC,
    EMBEDDER_TOPIC,
)

SNAPCHAIN_SUBSCRIBE_ENDPOINT = "ec2-13-58-183-70.us-east-2.compute.amazonaws.com:3383"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class KafkaJSONTopic(KafkaTopic):
    def __init__(self, topic_name, bootstrap_servers="kafka:29092", topic_type=None):
        super().__init__(topic_name, topic_type)
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=100,
            batch_size=16384,
        )

    def produce(self, event, topic_name=None):
        target_topic = topic_name or self.topic_name
        try:
            self.producer.send(target_topic, value=event)
            self.producer.flush()
            logger.info("Sent event to Kafka topic: %s", target_topic)
        except Exception as e:
            logger.error("Failed to send to Kafka: %s", e)


class SnapchainGRPCPoller(Poller):
    def __init__(self, end_point=SNAPCHAIN_SUBSCRIBE_ENDPOINT):
        super().__init__(end_point, "snapchain_grpc")
        self.channel = grpc.insecure_channel(end_point)
        self.stub = rpc_pb2_grpc.HubServiceStub(self.channel)
        self.topic_main = KafkaJSONTopic(EMBEDDER_TOPIC, topic_type=CAST_ADD_MSG)
        self.topic_other = KafkaJSONTopic(ANALYTICS_TOPIC, topic_type="other")
        self.topic_dlq = KafkaJSONTopic(DLQ_TOPIC, topic_type="dlq")

    def poll(self):
        request = request_response_pb2.SubscribeRequest(
            event_types=[hub_event_pb2.HUB_EVENT_TYPE_MERGE_MESSAGE]
        )
        while True:
            try:
                for event in self.stub.Subscribe(request):
                    yield event
            except grpc.RpcError as e:
                logger.warning("Stream error: %s. Reconnecting in 3s...", e)
                time.sleep(3)

    def publish(self, event):
        topic = self._select_topic(event)
        topic.produce(event)

    def _select_topic(self, event):
        if "dlq_reason" in event:
            return self.topic_dlq
        if event.get("message_type") == CAST_ADD_MSG:
            return self.topic_main
        return self.topic_other

    def run(self):
        logger.info("Starting Snapchain gRPC Poller...")
        for event in self.poll():
            try:
                transformed_event = FarcasterMessageTransformer(event).transform()
                print(transformed_event)
                self.publish(transformed_event)
            except Exception as e:
                logger.error("Failed to transform/publish event: %s", str(e))


class FarcasterMessageTransformer(EventTransformer):
    def transform(self):
        event_type = hub_event_pb2.HubEventType.Name(self.event.type)
        output = {"event_type": event_type, "schema_version": "v1"}
        logger.debug("Transforming event: %s", event_type)

        try:
            if (
                self.event.type == hub_event_pb2.HUB_EVENT_TYPE_MERGE_MESSAGE
                and self.event.HasField("merge_message_body")
            ):
                msg = self.event.merge_message_body.message
                data = msg.data
                msg_type_enum = data.type
                msg_type = message_pb2.MessageType.Name(msg_type_enum)

                output.update(
                    {
                        "message_type": msg_type,
                        "message_subtype": msg_type.replace("MESSAGE_TYPE_", ""),
                        "fid": data.fid,
                        "timestamp": data.timestamp,
                        "message_hash": msg.hash.hex(),
                        "raw": MessageToDict(
                            self.event, preserving_proto_field_name=True
                        ),
                    }
                )

                if msg_type_enum == message_pb2.MessageType.MESSAGE_TYPE_CAST_ADD:
                    text = data.cast_add_body.text
                    output["cast"] = {
                        "text": text,
                        "language": (
                            (lambda t: detect(t) if t and len(t) > 3 else None)(text)
                            if text
                            else None
                        ),
                        "mentions": list(data.cast_add_body.mentions),
                        "embeds": [
                            e.url
                            for e in data.cast_add_body.embeds
                            if e.HasField("url")
                        ],
                    }

                elif msg_type_enum == message_pb2.MessageType.MESSAGE_TYPE_REACTION_ADD:
                    body = data.reaction_body
                    output["reaction"] = {
                        "type": message_pb2.ReactionType.Name(body.type),
                        "target_fid": body.target_cast_id.fid,
                        "target_hash": body.target_cast_id.hash.hex(),
                    }

                elif msg_type_enum == message_pb2.MessageType.MESSAGE_TYPE_LINK_ADD:
                    body = data.link_body
                    output["link"] = {
                        "type": body.type,
                        "target_fid": body.target_fid,
                    }

                elif (
                    msg_type_enum == message_pb2.MessageType.MESSAGE_TYPE_USER_DATA_ADD
                ):
                    body = data.user_data_body
                    output["user_data"] = {
                        "type": hub_event_pb2.UserDataType.Name(body.type),
                        "value": body.value,
                    }

                else:
                    output["data"] = MessageToDict(
                        data, preserving_proto_field_name=True
                    )
                    output["dlq_reason"] = f"Unhandled MESSAGE_TYPE: {msg_type}"

            else:
                output.update(
                    {
                        "raw": MessageToDict(
                            self.event, preserving_proto_field_name=True
                        ),
                        "dlq_reason": f"Unhandled HubEventType or missing field: {event_type}",
                    }
                )

        except Exception as e:
            logger.exception("Failed to transform Farcaster/Snapchain event")
            output.update(
                {
                    "error": f"Transformation failed: {str(e)}",
                    "raw": MessageToDict(self.event, preserving_proto_field_name=True),
                    "dlq_reason": f"Transformation error: {str(e)}",
                }
            )

        return output


if __name__ == "__main__":
    SnapchainGRPCPoller().run()

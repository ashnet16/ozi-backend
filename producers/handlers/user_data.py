from proto import message_pb2

from .base import MessageHandler, Topic, register


@register
class UserDataAddHandler(MessageHandler):
    message_type = message_pb2.MESSAGE_TYPE_USER_DATA_ADD
    topic = Topic.OTHER
    payload_key = "user_data"

    def payload(self, data):
        body = data.user_data_body
        return {
            "type": message_pb2.UserDataType.Name(body.type),
            "value": body.value,
        }

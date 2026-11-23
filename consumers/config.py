# Messages
MSG_TYPE_PREFIX = "MESSAGE_TYPE"
CAST_ADD_MSG = f"{MSG_TYPE_PREFIX}_CAST_ADD"
EMBEDDER_TOPIC = "farcaster.events.add"
BATCH_SIZE = 100
# The consumers also write a partial batch once this long has passed since their
# last write, or when no message has arrived for this long.
FLUSH_INTERVAL_MS = 2000
ANALYTICS_TOPIC = "farcaster.events.other"

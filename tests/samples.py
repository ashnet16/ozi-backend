"""Snapchain events recorded in ``analytics/message_sample.json``.

The file holds 190 ``HubEvent``s in protobuf JSON form (camelCase keys, int64 as
strings), one after another rather than as a JSON array. They are parsed into
real ``hub_event_pb2.HubEvent`` messages so the poller sees what it would get
from a node.

The ``expected_*`` helpers read the raw JSON directly, not through any service
code, so tests compare the pipeline against an independent reading of the input.
"""

import base64
import json
from functools import lru_cache
from pathlib import Path

from google.protobuf import json_format

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_FILE = ROOT / "analytics" / "message_sample.json"

# This cast's text is only a URL. langdetect raises on it, so the poller sends the
# whole cast to the DLQ and it is never stored or embedded. See
# test_routing.py::test_cast_without_detectable_language_is_not_dead_lettered.
LANGDETECT_DROPPED_CAST = "3177ff450560e451f8613e52449343c4069db73d"


@lru_cache(maxsize=None)
def raw_events():
    text = SAMPLE_FILE.read_text()
    decoder = json.JSONDecoder()
    events, pos = [], 0
    while True:
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos >= len(text):
            return tuple(events)
        obj, pos = decoder.raw_decode(text, pos)
        events.append(obj)


def to_proto(event_dict):
    from proto import hub_event_pb2

    return json_format.ParseDict(event_dict, hub_event_pb2.HubEvent())


def hub_events():
    return [to_proto(e) for e in raw_events()]


def event_by_id(event_id):
    return next(e for e in raw_events() if e["id"] == str(event_id))


def message_data(event):
    return event.get("mergeMessageBody", {}).get("message", {}).get("data", {})


def b64_to_hex(value):
    return base64.b64decode(value).hex()


def message_hash_hex(event):
    return b64_to_hex(event["mergeMessageBody"]["message"]["hash"])


def events_of_type(message_type):
    return [e for e in raw_events() if message_data(e).get("type") == message_type]


def expected_cast_hashes():
    """Hashes of top-level casts (no parent cast) in the sample."""
    return {
        message_hash_hex(e)
        for e in events_of_type("MESSAGE_TYPE_CAST_ADD")
        if "parentCastId" not in message_data(e).get("castAddBody", {})
    }


def expected_comment_hashes():
    """Hashes of replies (casts with a parent cast) in the sample."""
    return {
        message_hash_hex(e)
        for e in events_of_type("MESSAGE_TYPE_CAST_ADD")
        if "parentCastId" in message_data(e).get("castAddBody", {})
    }


def expected_reaction_hashes(reaction_type=None):
    return {
        message_hash_hex(e)
        for e in events_of_type("MESSAGE_TYPE_REACTION_ADD")
        if reaction_type is None
        or message_data(e)["reactionBody"]["type"] == reaction_type
    }


def make_event(message_type, fid=1, timestamp=185845800, body=None, event_id=1):
    """Build a synthetic MERGE_MESSAGE ``HubEvent`` for a type the sample lacks."""
    data = {
        "type": message_type,
        "fid": str(fid),
        "timestamp": timestamp,
        "network": "FARCASTER_NETWORK_MAINNET",
    }
    data.update(body or {})
    return to_proto(
        {
            "type": "HUB_EVENT_TYPE_MERGE_MESSAGE",
            "id": str(event_id),
            "mergeMessageBody": {
                "message": {
                    "data": data,
                    "hash": base64.b64encode(bytes(range(20))).decode(),
                }
            },
        }
    )

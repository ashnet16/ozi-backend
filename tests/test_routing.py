"""Poller: Snapchain event → transformed message → Kafka topic.

Runs the real ``SnapchainGRPCPoller.run()`` over the recorded sample with a
fake gRPC stream and an in-memory Kafka. No database is needed.
"""

import json

import pytest

from producers.config import ANALYTICS_TOPIC, DLQ_TOPIC, EMBEDDER_TOPIC
from tests import samples
from tests.conftest import run_poller


def published(kafka, topic):
    return [json.loads(v) for v in kafka.messages(topic)]


@pytest.fixture
def sample_run(kafka):
    poller = run_poller(samples.hub_events())
    return poller, kafka


def test_poller_subscribes_to_merge_message_events(sample_run):
    from proto import hub_event_pb2

    poller, _ = sample_run
    request = poller.stub.requests[0]
    assert list(request.event_types) == [hub_event_pb2.HUB_EVENT_TYPE_MERGE_MESSAGE]


def test_every_event_is_published_exactly_once(sample_run):
    _, kafka = sample_run
    assert len(kafka.log) == len(samples.raw_events())
    assert {t for t, _ in kafka.log} <= {EMBEDDER_TOPIC, ANALYTICS_TOPIC, DLQ_TOPIC}


def test_cast_adds_go_to_the_embedder_topic(sample_run):
    # Cast removes share the topic, so each stays behind the cast it removes.
    _, kafka = sample_run
    msgs = published(kafka, EMBEDDER_TOPIC)

    assert {m["message_type"] for m in msgs} == {
        "MESSAGE_TYPE_CAST_ADD",
        "MESSAGE_TYPE_CAST_REMOVE",
    }
    expected = (
        samples.expected_cast_hashes() | samples.expected_comment_hashes()
    ) - {samples.LANGDETECT_DROPPED_CAST}
    assert {
        m["message_hash"] for m in msgs if m["message_type"] == "MESSAGE_TYPE_CAST_ADD"
    } == expected


@pytest.mark.parametrize(
    "message_type",
    ["MESSAGE_TYPE_REACTION_ADD", "MESSAGE_TYPE_LINK_ADD", "MESSAGE_TYPE_USER_DATA_ADD"],
)
def test_other_handled_types_go_to_the_analytics_topic(sample_run, message_type):
    _, kafka = sample_run
    events = samples.events_of_type(message_type)
    assert events, f"sample has no {message_type}"

    on_topic = {
        m["message_hash"]
        for m in published(kafka, ANALYTICS_TOPIC)
        if m["message_type"] == message_type
    }
    assert on_topic == {samples.message_hash_hex(e) for e in events}
    assert not any(m.get("message_type") == message_type for m in published(kafka, DLQ_TOPIC))


def test_cast_payload_matches_the_snapchain_message(sample_run):
    _, kafka = sample_run
    by_hash = {m["message_hash"]: m for m in published(kafka, EMBEDDER_TOPIC)}

    for event in samples.events_of_type("MESSAGE_TYPE_CAST_ADD"):
        h = samples.message_hash_hex(event)
        if h == samples.LANGDETECT_DROPPED_CAST:
            continue
        data = samples.message_data(event)
        body = data.get("castAddBody", {})
        msg = by_hash[h]

        assert msg["fid"] == int(data["fid"])
        assert msg["timestamp"] == data["timestamp"]
        assert msg["message_subtype"] == "CAST_ADD"
        assert msg["cast"]["text"] == body.get("text", "")
        assert msg["cast"]["mentions"] == [int(f) for f in body.get("mentions", [])]
        assert msg["cast"]["embeds"] == [e["url"] for e in body.get("embeds", []) if "url" in e]
        # The consumers read parents and ids from the raw event.
        assert msg["raw"]["id"] == event["id"]


def test_comment_payload_example(sample_run):
    # Event 556357181562880 is the comment used by the cast_remove_comment
    # scenario in analytics/message_sample_expected.json.
    _, kafka = sample_run
    msg = next(
        m
        for m in published(kafka, EMBEDDER_TOPIC)
        if m["message_hash"] == "7fc0f8eaf8504258b8040f53c090724a84618709"
    )
    assert msg["fid"] == 805254
    assert msg["cast"]["text"].startswith("Agree completely.")
    assert msg["cast"]["language"] == "en"
    parent = msg["raw"]["merge_message_body"]["message"]["data"]["cast_add_body"]["parent_cast_id"]
    assert parent == {"fid": "806804", "hash": "Xm1BND/BMsX5b9z3Bk/fLKxpNRY="}


def test_reaction_payload_matches_the_snapchain_message(sample_run):
    _, kafka = sample_run
    by_hash = {
        m["message_hash"]: m
        for m in published(kafka, ANALYTICS_TOPIC)
        if m["message_type"] == "MESSAGE_TYPE_REACTION_ADD"
    }

    for event in samples.events_of_type("MESSAGE_TYPE_REACTION_ADD"):
        body = samples.message_data(event)["reactionBody"]
        target = body.get("targetCastId", {})
        msg = by_hash[samples.message_hash_hex(event)]

        assert msg["reaction"] == {
            "type": body["type"],
            "target_fid": int(target.get("fid", 0)),
            "target_hash": samples.b64_to_hex(target["hash"]) if target else "",
        }


def test_prune_events_go_to_the_dlq(sample_run):
    _, kafka = sample_run
    prunes = [e for e in samples.raw_events() if e["type"] == "HUB_EVENT_TYPE_PRUNE_MESSAGE"]
    dlq = [m for m in published(kafka, DLQ_TOPIC) if m["event_type"] == "HUB_EVENT_TYPE_PRUNE_MESSAGE"]

    assert len(dlq) == len(prunes) > 0
    assert all(m["dlq_reason"].startswith("Unhandled HubEventType") for m in dlq)


# Types that stay unsupported. Per the task, these keep going to the DLQ.
@pytest.mark.parametrize(
    "message_type",
    [
        "MESSAGE_TYPE_LINK_REMOVE",
        "MESSAGE_TYPE_VERIFICATION_ADD_ETH_ADDRESS",
        "MESSAGE_TYPE_VERIFICATION_REMOVE",
        "MESSAGE_TYPE_USERNAME_PROOF",
        "MESSAGE_TYPE_FRAME_ACTION",
    ],
)
def test_unhandled_message_types_go_to_the_dlq(kafka, message_type):
    run_poller([samples.make_event(message_type)])

    assert [t for t, _ in kafka.log] == [DLQ_TOPIC]
    (msg,) = published(kafka, DLQ_TOPIC)
    assert msg["message_type"] == message_type
    assert msg["dlq_reason"] == f"Unhandled MESSAGE_TYPE: {message_type}"
    assert "data" in msg


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Existing bug: langdetect raises on text without letters (here only a URL), "
        "and the poller dead-letters the whole cast instead of storing it with no "
        "language. https://github.com/ashnet16/ozi-backend/issues/7"
    ),
)
def test_cast_without_detectable_language_is_not_dead_lettered(sample_run):
    _, kafka = sample_run
    hashes = {m["message_hash"] for m in published(kafka, EMBEDDER_TOPIC)}
    assert samples.LANGDETECT_DROPPED_CAST in hashes


# --------------------------------------------------------------------------- #
# Removals
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "message_type, topic",
    [
        ("MESSAGE_TYPE_CAST_REMOVE", EMBEDDER_TOPIC),
        ("MESSAGE_TYPE_REACTION_REMOVE", ANALYTICS_TOPIC),
    ],
)
def test_removes_go_to_the_topic_of_what_they_remove(sample_run, message_type, topic):
    _, kafka = sample_run
    events = samples.events_of_type(message_type)
    assert events, f"sample has no {message_type}"

    on_topic = {m["message_hash"] for m in published(kafka, topic) if m["message_type"] == message_type}
    assert on_topic == {samples.message_hash_hex(e) for e in events}
    for other in {EMBEDDER_TOPIC, ANALYTICS_TOPIC, DLQ_TOPIC} - {topic}:
        assert not any(m.get("message_type") == message_type for m in published(kafka, other))


def test_cast_remove_payload(sample_run):
    _, kafka = sample_run
    by_hash = {m["message_hash"]: m for m in published(kafka, EMBEDDER_TOPIC)}

    for event in samples.events_of_type("MESSAGE_TYPE_CAST_REMOVE"):
        data = samples.message_data(event)
        msg = by_hash[samples.message_hash_hex(event)]
        assert msg["message_subtype"] == "CAST_REMOVE"
        assert msg["fid"] == int(data["fid"])
        assert msg["cast_remove"] == {
            "target_hash": samples.b64_to_hex(data["castRemoveBody"]["targetHash"])
        }

    # The cast_remove_comment scenario.
    (msg,) = [
        m for m in by_hash.values()
        if m.get("cast_remove", {}).get("target_hash") == "7fc0f8eaf8504258b8040f53c090724a84618709"
    ]
    assert msg["removed"] == [
        {"hash": "7fc0f8eaf8504258b8040f53c090724a84618709", "message_type": "CAST_ADD", "fid": 805254}
    ]


def test_reaction_remove_payload(sample_run):
    _, kafka = sample_run
    by_hash = {m["message_hash"]: m for m in published(kafka, ANALYTICS_TOPIC)}

    for event in samples.events_of_type("MESSAGE_TYPE_REACTION_REMOVE"):
        body = samples.message_data(event)["reactionBody"]
        msg = by_hash[samples.message_hash_hex(event)]
        assert msg["message_subtype"] == "REACTION_REMOVE"
        assert msg["reaction_remove"] == {
            "type": body["type"],
            "target_fid": int(body["targetCastId"]["fid"]),
            "target_hash": samples.b64_to_hex(body["targetCastId"]["hash"]),
        }
        assert "reaction" not in msg


def test_removed_lists_the_messages_each_merge_displaced(sample_run):
    # From the event's deleted_messages, for every handled message type: removes,
    # and also the re-added like, which displaced the remove before it.
    _, kafka = sample_run
    by_hash = {
        m["message_hash"]: m
        for topic in (EMBEDDER_TOPIC, ANALYTICS_TOPIC)
        for m in published(kafka, topic)
    }
    with_removed = 0
    for event in samples.raw_events():
        msg = by_hash.get(samples.message_hash_hex(event)) if "mergeMessageBody" in event else None
        if msg is None:
            continue
        expected = [
            {"hash": h, "message_type": t.replace("MESSAGE_TYPE_", ""), "fid": fid}
            for h, t, fid in samples.deleted_messages(event)
        ]
        assert msg["removed"] == expected
        with_removed += bool(expected)
    assert with_removed == 8


def test_messages_are_keyed_by_fid(sample_run):
    # Each fid's messages land in one partition, so a remove stays behind its add.
    _, kafka = sample_run
    for (topic, raw), key in zip(kafka.log, kafka.keys):
        msg = json.loads(raw)
        if "fid" in msg:
            assert key == str(msg["fid"]).encode()
        else:
            assert topic == DLQ_TOPIC and key is None


# --------------------------------------------------------------------------- #
# Handlers
# --------------------------------------------------------------------------- #


def test_each_handled_type_has_one_handler_with_its_topic():
    from handlers import HANDLERS, Topic
    from proto import message_pb2

    routes = {
        message_pb2.MessageType.Name(t): (type(h).__name__, h.topic, h.payload_key)
        for t, h in HANDLERS.items()
    }
    assert routes == {
        "MESSAGE_TYPE_CAST_ADD": ("CastAddHandler", Topic.ADD, "cast"),
        "MESSAGE_TYPE_CAST_REMOVE": ("CastRemoveHandler", Topic.ADD, "cast_remove"),
        "MESSAGE_TYPE_REACTION_ADD": ("ReactionAddHandler", Topic.OTHER, "reaction"),
        "MESSAGE_TYPE_REACTION_REMOVE": ("ReactionRemoveHandler", Topic.OTHER, "reaction_remove"),
        "MESSAGE_TYPE_LINK_ADD": ("LinkAddHandler", Topic.OTHER, "link"),
        "MESSAGE_TYPE_USER_DATA_ADD": ("UserDataAddHandler", Topic.OTHER, "user_data"),
    }


def test_registering_a_type_twice_is_refused():
    from handlers import MessageHandler, register
    from proto import message_pb2

    with pytest.raises(ValueError, match="already has a handler"):

        @register
        class AnotherCastAddHandler(MessageHandler):
            message_type = message_pb2.MESSAGE_TYPE_CAST_ADD
            payload_key = "cast"

            def payload(self, data):
                return {}


def test_a_new_handler_is_used_without_changing_the_transformer(kafka):
    from handlers import HANDLERS, MessageHandler, Topic, register
    from proto import message_pb2

    link_remove = message_pb2.MESSAGE_TYPE_LINK_REMOVE
    try:

        @register
        class LinkRemoveHandler(MessageHandler):
            message_type = link_remove
            topic = Topic.OTHER
            payload_key = "link_remove"

            def payload(self, data):
                return {"target_fid": data.link_body.target_fid}

        run_poller(
            [samples.make_event("MESSAGE_TYPE_LINK_REMOVE", body={"linkBody": {"type": "follow", "targetFid": "7"}})]
        )
    finally:
        HANDLERS.pop(link_remove, None)

    assert [t for t, _ in kafka.log] == [ANALYTICS_TOPIC]
    (msg,) = published(kafka, ANALYTICS_TOPIC)
    assert msg["link_remove"] == {"target_fid": 7}
    assert msg["removed"] == []


def test_a_message_its_handler_rejects_goes_to_the_dlq(kafka):
    run_poller([samples.make_event("MESSAGE_TYPE_CAST_REMOVE", body={"castRemoveBody": {}})])

    (msg,) = published(kafka, DLQ_TOPIC)
    assert msg["dlq_reason"] == "CAST_REMOVE has no target hash"
    assert msg["message_type"] == "MESSAGE_TYPE_CAST_REMOVE"


def test_a_handler_that_fails_sends_the_message_to_the_dlq(kafka, monkeypatch):
    from handlers import HANDLERS
    from proto import message_pb2

    handler = HANDLERS[message_pb2.MESSAGE_TYPE_REACTION_ADD]

    def broken(data):
        raise RuntimeError("boom")

    monkeypatch.setattr(handler, "payload", broken)
    run_poller([samples.to_proto(samples.events_of_type("MESSAGE_TYPE_REACTION_ADD")[0])])

    (msg,) = published(kafka, DLQ_TOPIC)
    assert msg["dlq_reason"] == "Transformation error: ReactionAddHandler failed: boom"
    assert "raw" in msg

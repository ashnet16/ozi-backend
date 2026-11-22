"""End to end ingestion: Snapchain events → poller → Kafka → analytics consumer
→ Postgres, and → semantic consumer → Qdrant.

Each test starts from an empty database and an empty Qdrant collection.
"""

from datetime import datetime, timedelta, timezone

import pytest

from tests import samples

COLLECTION = "farcaster_casts"
FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)


def hashes(pipeline, table, where="TRUE"):
    return {r[0] for r in pipeline.query(f"SELECT msg_hash_hex FROM {table} WHERE {where}")}


@pytest.fixture
def ingested(pipeline):
    pipeline.run(samples.hub_events())
    return pipeline


def test_top_level_casts_are_stored_in_casts(ingested):
    expected = samples.expected_cast_hashes() - {samples.LANGDETECT_DROPPED_CAST}
    assert expected
    assert hashes(ingested, "casts") == expected


def test_replies_are_stored_in_comments(ingested):
    expected = samples.expected_comment_hashes()
    assert expected
    assert hashes(ingested, "comments") == expected


def test_likes_and_recasts_are_stored_in_reactions(ingested):
    assert hashes(ingested, "reactions") == samples.expected_reaction_hashes()
    for reaction_type in ("REACTION_TYPE_LIKE", "REACTION_TYPE_RECAST"):
        expected = samples.expected_reaction_hashes(reaction_type)
        assert expected, f"sample has no {reaction_type}"
        assert hashes(ingested, "reactions", f"reaction_type = '{reaction_type}'") == expected


def test_comment_row(ingested):
    event = samples.event_by_id(556357181562880)
    data = samples.message_data(event)
    (row,) = ingested.query(
        "SELECT fid, msg_type, msg_timestamp, parent_fid, parent_hash_hex, text, lang"
        " FROM comments WHERE msg_hash_hex = %s",
        ("7fc0f8eaf8504258b8040f53c090724a84618709",),
    )
    assert row == (
        805254,
        "CAST_ADD",
        FARCASTER_EPOCH + timedelta(seconds=data["timestamp"]),
        806804,
        samples.b64_to_hex("Xm1BND/BMsX5b9z3Bk/fLKxpNRY="),
        data["castAddBody"]["text"],
        "en",
    )


def test_reaction_row(ingested):
    # The recast used by the reaction_remove_recast scenario.
    (row,) = ingested.query(
        "SELECT fid, msg_type, reaction_type, target_fid, target_hash_hex"
        " FROM reactions WHERE msg_hash_hex = %s",
        ("f2ac095d09c68164cdced30c353462e77bcb764f",),
    )
    assert row == (
        256034,
        "REACTION_ADD",
        "REACTION_TYPE_RECAST",
        466111,
        "c09ef76a3c0b83e58c6cd77e09ca8fb91337ba5b",
    )


def test_link_and_user_data_messages_are_not_stored(ingested):
    tables = {r[0] for r in ingested.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )}
    assert tables == {"casts", "comments", "reactions"}


def test_casts_with_text_are_embedded_in_qdrant(ingested):
    expected = {
        samples.message_hash_hex(e)
        for e in samples.events_of_type("MESSAGE_TYPE_CAST_ADD")
        if samples.message_data(e).get("castAddBody", {}).get("text")
    } - {samples.LANGDETECT_DROPPED_CAST}

    points, _ = ingested.qdrant.scroll(COLLECTION, limit=1000, with_payload=True)
    payloads = {p.payload["message_hash_hex"]: p.payload for p in points}

    assert set(payloads) == expected
    assert len(points) == len(expected)

    # Unlike the analytics consumer, which files channel casts (parentUrl, no
    # parent cast) under `casts`, the semantic consumer flags them as comments.
    has_parent = {
        samples.message_hash_hex(e)
        for e in samples.events_of_type("MESSAGE_TYPE_CAST_ADD")
        if {"parentCastId", "parentUrl"} & set(samples.message_data(e).get("castAddBody", {}))
    }
    assert has_parent - samples.expected_comment_hashes(), "sample has no channel cast"
    for h, payload in payloads.items():
        assert payload["is_comment"] == (h in has_parent)


def test_replaying_the_stream_does_not_duplicate_rows_or_points(ingested):
    counts = lambda: (  # noqa: E731
        [ingested.query(f"SELECT count(*) FROM {t}")[0][0] for t in ("casts", "comments", "reactions")],
        ingested.qdrant.count(COLLECTION).count,
    )
    before = counts()

    ingested.run(samples.hub_events())

    assert counts() == before


def test_messages_are_applied_in_stream_order(pipeline):
    # The analytics consumer reads the add and other topics together; every
    # message the poller produced reaches it once, in the order produced.
    import consumer as analytics_consumer
    from consumers.config import ANALYTICS_TOPIC, EMBEDDER_TOPIC

    pipeline.publish(samples.hub_events())
    seen = []
    c = analytics_consumer.KafkaAnalyticsConsumer(topics=[ANALYTICS_TOPIC, EMBEDDER_TOPIC])
    c.process = lambda msg: seen.append(msg["message_hash"])
    c.consume()

    produced = [
        samples.message_hash_hex(e)
        for e in samples.raw_events()
        if samples.message_data(e).get("type")
        in ("MESSAGE_TYPE_CAST_ADD", "MESSAGE_TYPE_REACTION_ADD", "MESSAGE_TYPE_LINK_ADD", "MESSAGE_TYPE_USER_DATA_ADD")
        and samples.message_hash_hex(e) != samples.LANGDETECT_DROPPED_CAST
    ]
    assert seen == produced

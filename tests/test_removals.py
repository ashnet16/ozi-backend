"""Cast deletes and reaction removes, end to end.

The scenarios are the ones in ``analytics/message_sample_expected.json``: the
last ten events of ``analytics/message_sample.json`` are synthetic removes (and
one re-add) of casts and reactions earlier in the sample.

Removes are recorded in ``removed_messages``; the raw tables keep every row,
and the ``api.live_*`` views leave out the removed ones.
"""

import logging

import psycopg2
import pytest

from tests import samples

COLLECTION = "farcaster_casts"

DELETED_COMMENT = "7fc0f8eaf8504258b8040f53c090724a84618709"
ORPHAN_CAST_REMOVE_TARGET = "4d45785ea2b9d99aed9e2eb1b730a7f662f6ba6f"
ORPHAN_REACTION_TARGET = "9c09a5114eaf88d648373b9e792bba6c8e2e20a6"

REMOVED_LIKE = "2a5a24ecf76fb65623d2a7478754e688ceaffd84"
REMOVED_RECAST = "f2ac095d09c68164cdced30c353462e77bcb764f"
LIKE_REMOVED_AT_SAME_TIMESTAMP = "fa47f2ef20a8dd66bae04a2d44fab521234f3fb1"
LIKE_REMOVED_THEN_READDED = "d0f6cc35670bea27fa4f2f2f00e8f07192185349"
READDED_LIKE = "3366cb6d9f340cf7097ce347638bd313dfe11639"
LIKE_WITH_REMOVE_FOR_A_RECAST = "1d3c8f01fbc2fe16ff79eeb1ba2fdbda0e33dd7a"

# Event ids of the remove scenarios' events.
COMMENT_ADD_EVENT, COMMENT_REMOVE_EVENT = 556357181562880, 556357271748611
LIKE_ADD_EVENT, LIKE_REMOVE_EVENT = 556357181562885, 556357271748613


def hashes(pipeline, relation, where="TRUE"):
    return {
        r[0] for r in pipeline.query(f"SELECT msg_hash_hex FROM {relation} WHERE {where}")
    }


def counts(pipeline):
    tables = ("casts", "comments", "reactions", "removed_messages")
    return (
        {t: pipeline.query(f"SELECT count(*) FROM {t}")[0][0] for t in tables},
        pipeline.qdrant.count(COLLECTION).count,
    )


def has_point(qdrant, message_hash_hex):
    from consumers.consumer import point_id

    return bool(qdrant.retrieve(COLLECTION, ids=[point_id(message_hash_hex)]))


def consumer_errors(caplog):
    """Errors logged by either consumer (the analytics consumer's module is
    imported as ``consumer``, the semantic one as ``consumers.consumer``)."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno >= logging.ERROR and r.name in ("consumer", "consumers.consumer")
    ]


def event_with_id(event_id):
    return next(e for e in samples.hub_events() if e.id == event_id)


@pytest.fixture
def ingested(pipeline, caplog):
    caplog.set_level(logging.INFO)
    pipeline.run(samples.hub_events())
    return pipeline


# --------------------------------------------------------------------------- #
# Postgres: the raw tables keep removed rows, the views leave them out
# --------------------------------------------------------------------------- #


def test_raw_tables_keep_removed_casts_comments_and_reactions(ingested):
    assert DELETED_COMMENT in hashes(ingested, "comments")
    assert {
        REMOVED_LIKE,
        REMOVED_RECAST,
        LIKE_REMOVED_AT_SAME_TIMESTAMP,
        LIKE_REMOVED_THEN_READDED,
    } <= hashes(ingested, "reactions")
    # Every add in the sample is still in its table, removed or not.
    assert hashes(ingested, "comments") == samples.expected_comment_hashes()
    assert hashes(ingested, "reactions") == samples.expected_reaction_hashes()


def test_deleted_comment_is_not_in_the_views(ingested):
    assert DELETED_COMMENT not in hashes(ingested, "api.live_comments")
    assert DELETED_COMMENT not in hashes(ingested, "api.live_casts")
    assert hashes(ingested, "api.live_comments") == (
        samples.expected_comment_hashes() - {DELETED_COMMENT}
    )


def test_live_casts_leave_out_removed_casts(ingested):
    assert hashes(ingested, "api.live_casts") == (
        hashes(ingested, "casts") - samples.removed_hashes()
    )


def test_live_reactions_count_only_reactions_that_were_not_removed(ingested):
    live = hashes(ingested, "api.live_reactions")

    for removed in (REMOVED_LIKE, REMOVED_RECAST, LIKE_REMOVED_AT_SAME_TIMESTAMP):
        assert removed not in live
    # Removed, then liked again: only the re-add counts.
    assert LIKE_REMOVED_THEN_READDED not in live
    assert READDED_LIKE in live
    readd = samples.removal_scenarios()["reaction_remove_then_readd"]
    ((likes,),) = ingested.query(
        "SELECT count(*) FROM api.live_reactions"
        " WHERE fid = 858865 AND target_hash_hex = %s AND reaction_type = 'REACTION_TYPE_LIKE'",
        (readd["target_hash_hex"],),
    )
    assert likes == 1
    # Its remove is for a recast of the same cast, which doesn't exist.
    assert LIKE_WITH_REMOVE_FOR_A_RECAST in live

    assert live == samples.expected_reaction_hashes() - samples.removed_hashes()


def test_removed_messages_records_each_scenario(ingested):
    rows = {
        r[0]: r[1:]
        for r in ingested.query(
            "SELECT msg_hash_hex, message_type, fid, reason, removed_by_hash_hex"
            " FROM removed_messages"
        )
    }
    remove_hash = {
        e.id: e.merge_message_body.message.hash.hex() for e in samples.hub_events()
    }

    assert rows[DELETED_COMMENT] == (
        "CAST_ADD", 805254, "cast_remove", remove_hash[COMMENT_REMOVE_EVENT]
    )
    assert rows[REMOVED_LIKE] == (
        "REACTION_ADD", 879085, "reaction_remove", remove_hash[LIKE_REMOVE_EVENT]
    )
    assert rows[REMOVED_RECAST][2] == "reaction_remove"
    assert rows[LIKE_REMOVED_AT_SAME_TIMESTAMP][2] == "reaction_remove"
    assert rows[LIKE_REMOVED_THEN_READDED][2] == "reaction_remove"
    # The re-add displaced the remove, which Snapchain reports as deleted too.
    unlike = remove_hash[556357271748616]
    assert rows[unlike] == ("REACTION_REMOVE", 858865, "replaced", READDED_LIKE)
    # Nothing displaced the like whose remove is for a recast.
    assert LIKE_WITH_REMOVE_FOR_A_RECAST not in rows

    # Every removed message in the sample has a row, and nothing else does
    # except the reaction removes that displaced nothing (keyed on their own hash).
    reaction_removes_without_target = {
        remove_hash[int(e["id"])]
        for e in samples.events_of_type("MESSAGE_TYPE_REACTION_REMOVE")
        if not samples.deleted_messages(e)
    }
    assert set(rows) == samples.removed_hashes() | reaction_removes_without_target


def test_removed_at_is_the_time_of_the_removing_message(ingested):
    from consumers.utils import farcaster_ts_to_datetime

    remove = samples.message_data(samples.event_by_id(COMMENT_REMOVE_EVENT))
    ((removed_at,),) = ingested.query(
        "SELECT removed_at FROM removed_messages WHERE msg_hash_hex = %s",
        (DELETED_COMMENT,),
    )
    assert removed_at == farcaster_ts_to_datetime(remove["timestamp"])


# --------------------------------------------------------------------------- #
# Qdrant
# --------------------------------------------------------------------------- #


def test_deleted_comment_has_no_point_in_qdrant(ingested):
    assert not has_point(ingested.qdrant, DELETED_COMMENT)
    points, _ = ingested.qdrant.scroll(COLLECTION, limit=1000, with_payload=True)
    assert DELETED_COMMENT not in {p.payload["message_hash_hex"] for p in points}
    assert not any(p.payload["text"].startswith("agree completely.") for p in points)


def test_cast_remove_deletes_a_point_that_was_already_written(pipeline):
    # The comment is embedded in one run and deleted in the next.
    add, remove = event_with_id(COMMENT_ADD_EVENT), event_with_id(COMMENT_REMOVE_EVENT)
    from consumers import consumer as semantic_consumer
    from consumers.config import EMBEDDER_TOPIC

    embedder = semantic_consumer.KafkaEmbedConsumer(["localhost:6333"], topics=[EMBEDDER_TOPIC])
    pipeline.publish([add])
    assert embedder.flush(pipeline_messages(pipeline, EMBEDDER_TOPIC)) == []
    assert has_point(pipeline.qdrant, DELETED_COMMENT)

    pipeline.kafka.log.clear()
    pipeline.publish([remove])
    assert embedder.flush(pipeline_messages(pipeline, EMBEDDER_TOPIC)) == []
    assert not has_point(pipeline.qdrant, DELETED_COMMENT)


def pipeline_messages(pipeline, topic):
    import json

    return [json.loads(v) for v in pipeline.kafka.messages(topic)]


# --------------------------------------------------------------------------- #
# Removes with no matching add, and removes that arrive before their add
# --------------------------------------------------------------------------- #


def test_removes_with_no_matching_add_are_recorded_without_errors(ingested, caplog):
    assert consumer_errors(caplog) == []

    # A cast remove whose cast Ozi never received.
    assert ORPHAN_CAST_REMOVE_TARGET not in hashes(ingested, "casts")
    assert ingested.query(
        "SELECT message_type, fid, reason FROM removed_messages WHERE msg_hash_hex = %s",
        (ORPHAN_CAST_REMOVE_TARGET,),
    ) == [("CAST_ADD", 407871, "cast_remove")]

    # A reaction remove for a like that never existed. Snapchain displaced
    # nothing, so the liked message's hash is unknown and the row is keyed on
    # the remove's own hash.
    (event,) = [
        e
        for e in samples.events_of_type("MESSAGE_TYPE_REACTION_REMOVE")
        if samples.b64_to_hex(samples.message_data(e)["reactionBody"]["targetCastId"]["hash"])
        == ORPHAN_REACTION_TARGET
    ]
    remove_hash = samples.message_hash_hex(event)
    assert ingested.query(
        "SELECT msg_hash_hex, message_type, fid, reason FROM removed_messages"
        " WHERE removed_by_hash_hex = %s",
        (remove_hash,),
    ) == [(remove_hash, "REACTION_ADD", 256034, "reaction_remove")]
    assert not ingested.query(
        "SELECT 1 FROM reactions WHERE target_hash_hex = %s", (ORPHAN_REACTION_TARGET,)
    )


def test_removes_streamed_before_their_adds_still_hide_them(pipeline, caplog):
    # The comment's and the like's removes come first in the stream.
    caplog.set_level(logging.INFO)
    early = {COMMENT_REMOVE_EVENT, LIKE_REMOVE_EVENT}
    events = samples.hub_events()
    pipeline.run(
        [e for e in events if e.id in early] + [e for e in events if e.id not in early]
    )

    assert consumer_errors(caplog) == []
    assert DELETED_COMMENT in hashes(pipeline, "comments")
    assert DELETED_COMMENT not in hashes(pipeline, "api.live_comments")
    assert REMOVED_LIKE in hashes(pipeline, "reactions")
    assert REMOVED_LIKE not in hashes(pipeline, "api.live_reactions")
    # Same batch, so the embedder drops the comment before writing it.
    assert not has_point(pipeline.qdrant, DELETED_COMMENT)


def test_removes_applied_before_their_adds_arrive_still_hide_them(pipeline, caplog):
    # Each consumer handles the removes in one flush and the adds in a later one.
    import json

    import consumer as analytics_consumer
    from consumers import consumer as semantic_consumer
    from consumers.config import ANALYTICS_TOPIC, EMBEDDER_TOPIC

    caplog.set_level(logging.INFO)
    analytics = analytics_consumer.KafkaAnalyticsConsumer(topics=[ANALYTICS_TOPIC, EMBEDDER_TOPIC])
    embedder = semantic_consumer.KafkaEmbedConsumer(["localhost:6333"], topics=[EMBEDDER_TOPIC])

    def deliver(event_ids):
        pipeline.kafka.log.clear()
        pipeline.publish([event_with_id(i) for i in event_ids])
        for topic, raw in pipeline.kafka.log:
            analytics.process(json.loads(raw))
        analytics.flush_buffers()
        assert embedder.flush(pipeline_messages(pipeline, EMBEDDER_TOPIC)) == []

    deliver([COMMENT_REMOVE_EVENT, LIKE_REMOVE_EVENT])
    assert {DELETED_COMMENT, REMOVED_LIKE} <= hashes(pipeline, "removed_messages")

    deliver([COMMENT_ADD_EVENT, LIKE_ADD_EVENT])

    assert consumer_errors(caplog) == []
    assert DELETED_COMMENT in hashes(pipeline, "comments")
    assert DELETED_COMMENT not in hashes(pipeline, "api.live_comments")
    assert REMOVED_LIKE in hashes(pipeline, "reactions")
    assert REMOVED_LIKE not in hashes(pipeline, "api.live_reactions")
    # Qdrant: the late add is written (the embedder keeps no record of past
    # removes), and no error is raised. See the readme's known limitations.


# --------------------------------------------------------------------------- #
# Replays
# --------------------------------------------------------------------------- #


def test_resending_the_stream_with_removals_creates_no_duplicates(ingested):
    before = counts(ingested)
    assert before[0]["removed_messages"] > 0

    ingested.run(samples.hub_events())

    assert counts(ingested) == before


def test_the_same_removal_twice_has_no_further_effect(pipeline):
    remove = event_with_id(COMMENT_REMOVE_EVENT)
    pipeline.run(samples.hub_events())
    before = (
        pipeline.query("SELECT * FROM removed_messages ORDER BY msg_hash_hex"),
        hashes(pipeline, "api.live_comments"),
    )

    pipeline.kafka.log.clear()
    pipeline.run([remove])

    after = (
        pipeline.query("SELECT * FROM removed_messages ORDER BY msg_hash_hex"),
        hashes(pipeline, "api.live_comments"),
    )
    assert after == before


# --------------------------------------------------------------------------- #
# The read-only role the API runs generated SQL as
# --------------------------------------------------------------------------- #


@pytest.fixture
def query_role(ingested, pg_params):
    import os

    conn = psycopg2.connect(
        **{**pg_params, "user": os.environ["OZI_QUERY_USER"], "password": os.environ["OZI_QUERY_PASSWORD"]}
    )
    conn.autocommit = True
    yield conn
    conn.close()


def test_query_role_reads_the_live_views_by_their_bare_names(query_role, ingested):
    for view in ("live_casts", "live_comments", "live_reactions"):
        with query_role.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {view}")
            assert cur.fetchone()[0] == len(hashes(ingested, f"api.{view}")) > 0


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM public.casts",
        "SELECT count(*) FROM public.comments",
        "SELECT count(*) FROM public.reactions",
        "SELECT count(*) FROM public.removed_messages",
    ],
)
def test_query_role_cannot_read_the_raw_tables(query_role, sql):
    with query_role.cursor() as cur, pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cur.execute(sql)


def test_query_role_cannot_write(query_role):
    with query_role.cursor() as cur, pytest.raises(psycopg2.Error):
        cur.execute(
            "INSERT INTO public.removed_messages (msg_hash_hex, reason) VALUES ('x', 'y')"
        )
    with query_role.cursor() as cur, pytest.raises(psycopg2.Error):
        cur.execute("CREATE TABLE api.scratch (x int)")


def test_views_pick_up_columns_added_to_a_table(pipeline):
    import consumer as analytics_consumer
    from consumers.config import ANALYTICS_TOPIC, EMBEDDER_TOPIC

    pipeline.run(samples.hub_events())
    with pipeline.db.cursor() as cur:
        cur.execute("ALTER TABLE casts ADD COLUMN extra TEXT")

    # The consumer re-creates the views on start.
    analytics_consumer.KafkaAnalyticsConsumer(topics=[ANALYTICS_TOPIC, EMBEDDER_TOPIC])

    columns = {
        r[0]
        for r in pipeline.query(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = 'api' AND table_name = 'live_casts'"
        )
    }
    assert "extra" in columns



def test_only_cast_and_reaction_removals_are_recorded(pipeline):
    # A profile update displaces the previous one; that is not a removal Ozi tracks.
    import base64

    from proto import hub_event_pb2

    event = samples.make_event(
        "MESSAGE_TYPE_USER_DATA_ADD",
        body={"userDataBody": {"type": "USER_DATA_TYPE_BIO", "value": "new bio"}},
    )
    displaced = hub_event_pb2.HubEvent()
    displaced.CopyFrom(event)
    old = event.merge_message_body.deleted_messages.add()
    old.CopyFrom(displaced.merge_message_body.message)
    old.hash = base64.b64decode("q83vASNFZ4mrze8BI0VniavN7wE=")

    pipeline.run([event])

    assert pipeline.query("SELECT count(*) FROM removed_messages") == [(0,)]

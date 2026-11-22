"""Search API over data that went through the whole pipeline.

The FastAPI app runs for real, including its startup hook (asyncpg pool, API
tables) and its psycopg2 queries, against the same Postgres and Qdrant the
consumers wrote to. Only OpenAI and Redis are faked.
"""

from collections import Counter
from types import SimpleNamespace

import pytest

from tests import fakes, samples

FID = "805254"  # author of the "Agree completely..." comment in the sample
COMMENT_HASH = "7fc0f8eaf8504258b8040f53c090724a84618709"


@pytest.fixture
def api(pipeline, monkeypatch):
    from fastapi.testclient import TestClient

    pipeline.run(samples.hub_events())

    # Imported here: api.main connects to Postgres at import time.
    from api import main
    from api.services import search

    monkeypatch.setattr(search, "client", pipeline.qdrant)
    llm = fakes.FakeLLM()
    monkeypatch.setattr(main, "llm_client", llm)

    with TestClient(main.app) as client:
        redis = fakes.FakeRedis()
        monkeypatch.setattr(main, "redis", redis)  # after startup replaced it
        assert client.post("/auth", json={"fid": FID}).status_code == 200

        def search_for(classification, sql=None, **request):
            request = {"fid": FID, "search_mode": "deep", **request}
            llm.expect(request, classification, sql=sql)
            return client.post("/search", json=request)

        yield SimpleNamespace(
            client=client, llm=llm, redis=redis, pipeline=pipeline, search=search_for
        )


def llm_call_kinds(api):
    return [kind for kind, _ in api.llm.calls]


def test_health(api):
    assert api.client.get("/health").json() == {"status": "ok"}


def test_analytic_search_runs_the_generated_sql(api):
    sql = "SELECT msg_hash_hex, text FROM comments WHERE text ILIKE '%patience is indeed%'"
    resp = api.search("analytic", sql=sql, query="who talks about patience")

    assert resp.status_code == 200
    body = resp.json()
    assert body["classification"] == ["analytic"]
    assert body["sql"] == sql
    assert [row[0] for row in body["sample_results"]] == [COMMENT_HASH]
    assert body["sql_summary"] == "fake summary"
    assert body["full_results_link"].startswith("/query?sql=SELECT")
    assert "semantic_results" not in body
    assert llm_call_kinds(api) == ["classify", "sql", "sql_summary"]


def test_analytic_search_counts_likes_and_recasts(api):
    sql = (
        "SELECT reaction_type, COUNT(*) FROM reactions"
        " GROUP BY reaction_type ORDER BY reaction_type"
    )
    body = api.search("analytic", sql=sql, query="how many likes and recasts").json()

    expected = Counter(
        samples.message_data(e)["reactionBody"]["type"]
        for e in samples.events_of_type("MESSAGE_TYPE_REACTION_ADD")
    )
    assert {t: n for t, n in body["sample_results"]} == dict(expected)


def test_full_results_link_returns_every_row(api):
    sql = "SELECT msg_hash_hex FROM casts ORDER BY msg_hash_hex"
    link = api.search("analytic", sql=sql, query="all casts").json()["full_results_link"]

    resp = api.client.get(link, params={"fid": FID})

    assert resp.status_code == 200
    expected = sorted(samples.expected_cast_hashes() - {samples.LANGDETECT_DROPPED_CAST})
    assert [r["msg_hash_hex"] for r in resp.json()["results"]] == expected


def test_semantic_search_finds_the_matching_cast(api):
    resp = api.search(
        "semantic",
        query="vet the team, ask questions and ensure the project aligns with your values",
        min_score=0.3,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["classification"] == ["semantic"]
    assert body["semantic_results"][0].startswith("agree completely. the human aspect")
    assert body["summary"] == "fake summary"
    assert "sql" not in body
    assert llm_call_kinds(api) == ["classify", "semantic_summary"]
    _, (query, docs) = api.llm.calls[-1]
    assert docs == body["semantic_results"]


def test_semantic_search_drops_hits_below_min_score(api):
    body = api.search("semantic", query="zzzz qqqq unrelated words", min_score=0.9).json()

    assert body["semantic_results"] == []
    assert body["summary"] == "No relevant casts found to summarize."
    assert llm_call_kinds(api) == ["classify"]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Existing bug: general mode filters Qdrant on fid with the request's string "
        "fid, but payloads store fid as an integer, so nothing ever matches. "
        "https://github.com/ashnet16/ozi-backend/issues/8"
    ),
)
def test_general_mode_semantic_search_returns_the_users_own_casts(api):
    body = api.search(
        "semantic",
        search_mode="general",
        query="vet the team, ask questions and ensure the project aligns with your values",
        min_score=0.3,
    ).json()

    assert body["semantic_results"]
    assert body["semantic_results"][0].startswith("agree completely.")


def test_semantic_and_analytic_together(api):
    sql = "SELECT COUNT(*) FROM casts"
    body = api.search(
        "semantic, analytic",
        sql=sql,
        query="vet the team, ask questions and ensure the project aligns with your values",
        min_score=0.3,
    ).json()

    assert body["classification"] == ["semantic", "analytic"]
    assert body["semantic_results"]
    assert body["sample_results"] == [
        [len(samples.expected_cast_hashes() - {samples.LANGDETECT_DROPPED_CAST})]
    ]


def test_search_is_logged_and_uses_quota(api):
    before = api.pipeline.query("SELECT deep_quota FROM users WHERE fid = %s", (FID,))[0][0]

    api.search("analytic", sql="SELECT COUNT(*) FROM reactions", query="reaction count")

    after = api.pipeline.query("SELECT deep_quota FROM users WHERE fid = %s", (FID,))[0][0]
    assert after == before - 1
    logs = api.pipeline.query("SELECT fid, query, classification, sql_query, search_mode FROM query_logs")
    assert logs == [(FID, "reaction count", ["analytic"], "SELECT COUNT(*) FROM reactions", "deep")]


def test_repeated_search_is_served_from_the_cache(api):
    request = dict(query="how many casts", search_mode="general")
    first = api.search("analytic", sql="SELECT COUNT(*) FROM casts", **request)
    calls = len(api.llm.calls)

    second = api.search("analytic", sql="SELECT COUNT(*) FROM casts", **request)

    assert second.json() == first.json()
    assert len(api.llm.calls) == calls
    key = f"query_response:{FID}:general:how many casts"
    assert set(api.redis.store) == {key}
    assert api.redis.ttls[key] == 3600


def test_invalid_search_mode_is_rejected(api):
    resp = api.search("analytic", search_mode="network", query="anything")

    assert resp.status_code == 400
    assert api.llm.calls == []


def test_unknown_user_is_rejected(api):
    resp = api.search("analytic", fid="999999999", query="anything")

    assert resp.status_code == 403
    assert api.llm.calls == []


def test_generated_sql_that_writes_is_refused(api):
    before = api.pipeline.query("SELECT COUNT(*) FROM casts")[0][0]

    with pytest.raises(ValueError, match="Unsafe SQL"):
        api.search("analytic", sql="DELETE FROM casts", query="remove everything")

    assert api.pipeline.query("SELECT COUNT(*) FROM casts")[0][0] == before > 0

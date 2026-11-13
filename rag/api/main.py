import json
import os
import time
from urllib.parse import quote
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2
import asyncpg
from pydantic import BaseModel
from api.services.search import search_vector
from api.utils.llm_prompts import classify_prompt, sql_generation_prompt, summarization_prompt
from api.utils.sql_formatter import clean_llm_sql, clean_sql
from api.utils.sql_validator import validate_safe_sql
from llm.chatgpt.connection import ChatGPTConnectionInfo
from llm.factory import LLMFactory
import logging
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

class QueryRequest(BaseModel):
    query: str
    n_results: int = 1500
    min_score: float = 0.5
    fid: str
    search_mode: str

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis = None
pg_pool = None

@app.on_event("startup")
async def startup():
    global redis, pg_pool
    redis = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379"), decode_responses=True)

    pg_pool = await asyncpg.create_pool(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.getenv("POSTGRES_DB", "ozi"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        min_size=1,
        max_size=5,
    )

    await ensure_tables()

@app.on_event("shutdown")
async def shutdown():
    if redis:
        await redis.close()

    if pg_pool:
        await pg_pool.close()


connection_info = ChatGPTConnectionInfo(api_key=os.getenv("OPENAI_API_KEY"))
llm_client = LLMFactory.create(provider="chatgpt", connection_info=connection_info)

pg_conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB", "ozi"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
)
pg_conn.autocommit = True
cur = pg_conn.cursor()

@app.get("/health")
def health_check():
    try:
        cur.execute("SELECT 1;")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "details": str(e)}

async def ensure_tables():
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS query_logs (
                id SERIAL PRIMARY KEY,
                fid TEXT,
                query TEXT,
                classification TEXT[],
                sql_query TEXT,
                response_time_ms INT,
                response JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                fid TEXT PRIMARY KEY,
                signer_uuid TEXT,
                general_quota INT DEFAULT 20,
                deep_quota INT DEFAULT 10,
                network_quota INT DEFAULT 5,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS logins (
                id SERIAL PRIMARY KEY,
                fid TEXT NOT NULL,
                login_time TIMESTAMPTZ DEFAULT NOW(),
                FOREIGN KEY (fid) REFERENCES users(fid)
            );
        """)
    logger.info("All necessary tables ensured.")


def enforce_quota(fid, quota_type):
    if not fid:
        return False, "Missing FID"
    cur.execute(f"SELECT {quota_type}_quota FROM users WHERE fid = %s;", (fid,))
    row = cur.fetchone()
    if not row:
        return False, "User not found or not authenticated."
    if row[0] <= 0:
        return False, f"{quota_type.capitalize()} quota exceeded."
    cur.execute(f"UPDATE users SET {quota_type}_quota = {quota_type}_quota - 1 WHERE fid = %s;", (fid,))
    return True, None


def log_query(fid, query, classification, sql_code, response, start_time, search_mode=None):
    response_time_ms = int((time.time() - start_time) * 1000)
    cur.execute(
        "INSERT INTO query_logs (fid, query, classification, sql_query, response_time_ms, response, search_mode) VALUES (%s, %s, %s, %s, %s, %s, %s);",
        (
            fid or "anonymous",
            query,
            classification,
            sql_code,
            response_time_ms,
            json.dumps(response),
            search_mode,
        ),
    )

@app.post("/search")
async def search_router(request: QueryRequest):
    cache_key = f"query_response:{request.fid}:{request.search_mode}:{request.query}"
    cached = await redis.get(cache_key)
    if cached:
        return JSONResponse(content=json.loads(cached))

    start = time.time()
    fid = request.fid
    search_mode = request.search_mode
    if search_mode not in ("general", "deep"):
        return JSONResponse(status_code=400, content={"error": "Invalid search mode"})
    quota_type = "general" if search_mode == "general" else "deep"
    allowed, error = enforce_quota(fid, quota_type)
    if not allowed:
        return JSONResponse(status_code=403, content={"error": error})

    classification = (await llm_client.ask(classify_prompt(request.query))).strip().lower()
    intents = [c.strip() for c in classification.split(",")]
    response = {"query": request.query, "classification": intents}
    sql_code_for_log = None
    top_texts = []

    if "semantic" in intents:
        vector_hits = await search_vector(request)
        async for line in vector_hits.body_iterator:
            hit = json.loads(line)
            if hit.get("payload", {}).get("text"):
                top_texts.append(hit["payload"]["text"])
        summary = await llm_client.summarize(request.query, top_texts[:request.n_results]) if top_texts else "No relevant casts found to summarize."
        response["summary"] = summary
        response["semantic_results"] = top_texts

    if "analytic" in intents:
        sql_prompt = sql_generation_prompt(
            request.query,
            fid=request.fid,
            mode=request.search_mode
        )
        sql_code = clean_sql(clean_llm_sql(await llm_client.ask(sql_prompt)))
        validate_safe_sql(sql_code)
        sql_code_for_log = sql_code
        cur.execute(sql_code)
        short_results = cur.fetchall()[:request.n_results]
        flat_rows = [", ".join(map(str, row)) for row in short_results]
        summary = await llm_client.ask(summarization_prompt(request.query, flat_rows))
        sql_encoded = quote(sql_code)
        response["sql"] = sql_code
        response["sample_results"] = short_results
        response["sql_summary"] = summary.strip()
        response["full_results_link"] = f"/query?sql={sql_encoded}"

    log_query(fid, request.query, intents, sql_code_for_log, response, start, request.search_mode)

    await redis.set(cache_key, json.dumps(response), ex=3600)
    return response


@app.post("/auth")
async def auth_user(request: Request):
    try:
        body = await request.json()
        fid = body.get("fid")
        signer_uuid = body.get("signer_uuid")

        if not fid or not signer_uuid:
            return JSONResponse(status_code=400, content={"error": "Missing fid or signer_uuid"})

        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(
                    "SELECT 1 FROM users WHERE fid = $1 AND signer_uuid = $2;",
                    fid, signer_uuid
                )

                if not exists:
                    await conn.execute(
                        "INSERT INTO users (fid, signer_uuid) VALUES ($1, $2);",
                        fid, signer_uuid
                    )

                await conn.execute("INSERT INTO logins (fid) VALUES ($1);", fid)

                row = await conn.fetchrow("""
                    SELECT general_quota, deep_quota, network_quota
                    FROM users
                    WHERE fid = $1
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, fid)
                general_quota = row["general_quota"]
                deep_quota = row["deep_quota"]
                network_quota = row["network_quota"]

                login_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM logins WHERE fid = $1;", fid)

                total_query_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM query_logs WHERE fid = $1;", fid)

                counts = dict(await conn.fetch("""
                    SELECT search_mode, COUNT(*) FROM query_logs
                    WHERE fid = $1
                    GROUP BY search_mode;
                """, fid))
                general_count = counts.get("general", 0)
                deep_count = counts.get("deep", 0)
                network_count = counts.get("network", 0)

        return {
            "fid": fid,
            "signer_uuid": signer_uuid,
            "general_quota": general_quota,
            "deep_quota": deep_quota,
            "login_attempts": login_count,
            "search_attempts_total": total_query_count,
            "search_attempts_general": general_count,
            "search_attempts_deep": deep_count,
            "search_attempts_network": network_count,
            "general_quota_exceeded": general_quota <= general_count,
            "deep_quota_exceeded": deep_quota <= deep_count,
            "network_quota_exceeded": network_quota <= network_count,
            "general_quota_remaining": general_quota - general_count,
            "deep_quota_remaining": deep_quota - deep_count,
            "network_quota_remaining": network_quota - network_count,
            "status": "authenticated"
        }

    except Exception as e:
        logger.exception(f"/auth error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/query")
async def query_full(sql: str, fid: str = None):
    start = time.time()
    allowed, error = enforce_quota(fid, "general")
    if not allowed:
        return JSONResponse(status_code=403, content={"error": error})
    sql_code = clean_sql(clean_llm_sql(sql.strip()))
    validate_safe_sql(sql_code)
    cur.execute(sql_code)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    results = [dict(zip(columns, row)) for row in rows]
    log_query(fid, sql_code, [], sql_code, json.loads(json.dumps({"results": results}, default=str)), start)
    return {"results": results}

@app.get("/queries")
def list_queries(limit: int = 100):
    cur.execute("SELECT id, query, classification, sql_query, created_at FROM query_logs ORDER BY created_at DESC LIMIT %s;", (limit,))
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return {"recent_queries": [dict(zip(columns, row)) for row in rows]}

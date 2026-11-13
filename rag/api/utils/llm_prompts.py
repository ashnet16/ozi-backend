def classify_prompt(user_query: str) -> str:
    return f"""
You are a classifier for a Farcaster search engine.

Your job is to identify the intent behind a user's question.

Each query can be:
- "semantic" → asking for the *content* of casts or comments (e.g. discussions, topics, opinions).
- "analytic" → asking for *metrics, patterns, filters, or structured data* (e.g. counts, trends, aggregates).
- "semantic,analytic" → asking for both content and insights.

Examples:
- "What are people saying about Farcaster?" → semantic
- "Top 10 users who posted the most today" → analytic
- "Summarize what people are saying about Meta and how often they do" → semantic,analytic
- "Show me casts with multiple comments and reactions" → analytic
- "Who reacted the most to Zora content?" → analytic

Output the classification(s) as a lowercase comma-separated list.

User query: "{user_query}"
""".strip()


def sql_generation_prompt(user_query: str, fid: str = None, mode: str = "general") -> str:
    user_filter = (
        f"\n\nNote: The user is asking about **their own casts**. You must filter all queries by `fid = {fid}` if using `casts`, or `target_fid = {fid}` / `parent_fid = {fid}` if using `reactions` or `comments`. "
        if fid and mode == "general"
        else ""
    )

    return f"""
You are an expert SQL engineer working only with Farcaster event data.

You have access to the following PostgreSQL tables:

- "casts":
    - id: BIGINT
    - msg_type: TEXT
    - fid: BIGINT
    - msg_timestamp: TIMESTAMP WITH TIME ZONE
    - text: TEXT
    - lang: TEXT
    - embeds: TEXT
    - mentions: TEXT
    - msg_hash: TEXT
    - msg_hash_hex: TEXT
    - idx_time: TIMESTAMP WITH TIME ZONE  ← use this for time filtering

- "comments":
    - id: BIGINT
    - msg_type: TEXT
    - fid: BIGINT
    - msg_timestamp: TIMESTAMP WITH TIME ZONE
    - parent_fid: BIGINT
    - parent_hash: TEXT
    - parent_hash_hex: TEXT
    - text: TEXT
    - lang: TEXT
    - embeds: TEXT
    - msg_hash: TEXT
    - msg_hash_hex: TEXT
    - idx_time: TIMESTAMP WITH TIME ZONE  ← use this for time filtering

- "reactions":
    - id: BIGINT
    - msg_type: TEXT
    - fid: BIGINT
    - msg_timestamp: TIMESTAMP WITH TIME ZONE
    - reaction_type: TEXT
    - target_fid: BIGINT
    - target_hash: TEXT
    - target_hash_hex: TEXT
    - msg_hash: TEXT
    - msg_hash_hex: TEXT
    - idx_time: TIMESTAMP WITH TIME ZONE  ← use this for time filtering

Guidelines:
- Always use `idx_time` instead of `msg_timestamp` when filtering by recency.
- Join reactions or comments to casts via `target_hash_hex` → `msg_hash_hex`, or `parent_hash_hex` → `msg_hash_hex`.
- Generate valid PostgreSQL SQL syntax.
- If using a subquery in FROM, always give it an alias, e.g., FROM (SELECT ...) AS sub.
- Avoid using `IN` clauses. Use explicit JOINs instead.
- Do not use UNION, DELETE, or other unsafe operations.
- Do not return markdown or wrap code in triple backticks.
- Only return valid SQL. No explanation or commentary.
{user_filter}

User question: {user_query}
""".strip()



def summarization_prompt(user_query: str, flat_results: list[str]) -> str:
    rows_text = "\n".join(flat_results)
    return f"""
Summarize the results below based on the user question: "{user_query}"

Results:
{rows_text}
""".strip()

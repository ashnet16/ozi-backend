from typing import List
import emoji

def format_as_markdown(
    query: str,
    classification: List[str],
    semantic_summary: str = "",
    top_casts: List[str] = [],
    sql: str = "",
    results: List = [],
    sql_summary: str = "",
    full_results_link: str = "",
) -> str:
    parts = [
        f"**Query:** {query.strip()}",
        f"**Classification:** {', '.join(classification).strip()}",
        "---"
    ]

    if semantic_summary:
        parts.append("### Semantic Summary")
        parts.append(semantic_summary.strip())

    if top_casts:
        parts.append("---\n\n### Top Casts")
        for cast in top_casts:
            for line in cast.strip().splitlines():
                line = emoji.emojize(line.strip(), language='alias')
                if line:
                    parts.append(f"- _{line}_")

    if sql:
        parts.append("---\n\n### SQL Query")
        parts.append(f"```sql\n{sql.strip()}\n```")

    if results:
        parts.append("### Results")
        for row in results:
            parts.append(f"- {row}")

    if sql_summary:
        parts.append("### Summary")
        parts.append(sql_summary.strip())

    if full_results_link:
        parts.append(f"[View full query results]({full_results_link})")

    return "\n\n".join(parts)

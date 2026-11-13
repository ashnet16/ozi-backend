import re


def clean_sql(sql: str) -> str:
    return re.sub(r"^```sql\s*|```$", "", sql.strip(), flags=re.IGNORECASE)


def clean_llm_sql(response: str) -> str:
    return re.sub(r"^```sql|```$", "", response.strip(), flags=re.IGNORECASE).strip()

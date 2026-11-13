import re

DANGEROUS_KEYWORDS = [
    "DROP",
    "DELETE",
    "INSERT",
    "UPDATE",
    "ALTER",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
]

ALLOWED_FUNCTIONS = {
    "COUNT", "ROUND", "SUM", "AVG", "MIN", "MAX", "ILIKE", "LOWER", "UPPER", "LENGTH", "TRIM", "SUBSTRING",
    "COALESCE", "NULLIF", "CASE", "CAST", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE","LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
    "NOW", "DATE_PART", "EXTRACT", "TO_CHAR", "CURRENT_DATE", "CURRENT_TIMESTAMP", "AGE",
}


def validate_safe_sql(sql_query: str) -> None:
    print(f"Validating SQL: {sql_query}")
    sql_clean = sql_query.strip()

    if not re.match(r"^\s*(SELECT|WITH)\b", sql_clean, flags=re.IGNORECASE):
        raise ValueError("Unsafe SQL: query must start with SELECT or WITH.")

    # Block dangerous keywords
    for keyword in DANGEROUS_KEYWORDS:
        pattern = r"\b" + keyword + r"\b"
        if re.search(pattern, sql_clean, flags=re.IGNORECASE):
            raise ValueError(f"Unsafe SQL: contains dangerous keyword '{keyword}'")

    used_functions = {
        match.group(1)
        for match in re.finditer(r"\b([A-Z_][A-Z_0-9]*)\s*\(", sql_clean.upper())
    }

    disallowed = used_functions - ALLOWED_FUNCTIONS
    if disallowed:
        raise ValueError(f"Unsafe SQL: uses disallowed functions: {', '.join(sorted(disallowed))}")

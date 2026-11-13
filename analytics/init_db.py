import os

import duckdb
from table_queries import (
    create_casts_table,
    create_comments_table,
    create_reactions_table,
)

os.makedirs("duckdb_data", exist_ok=True)

conn = duckdb.connect("duckdb_data/ozi.duckdb")
conn.execute(create_casts_table())
conn.execute(create_comments_table())
conn.execute(create_reactions_table())

print("Database initialized successfully.")

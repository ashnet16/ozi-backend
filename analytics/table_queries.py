# Rows are keyed on the message hash. `id` is the Snapchain event id, which is
# only unique within a shard, so it cannot be the key.

TABLES = ("casts", "comments", "reactions")

# Days between the Farcaster epoch (2021-01-01) and the 2021-10-01 date that
# older versions used by mistake when converting timestamps.
_OLD_EPOCH_ERROR_DAYS = 273


def primary_key_column():
    """Query returning the primary-key column of the table passed as a parameter."""
    return """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """


def migrate_to_hash_key(table):
    """One-off repair of a table created when `id` was the primary key.

    Older versions also base64-decoded hashes that were already hex and used the
    wrong epoch, so this rewrites the hex columns and shifts the timestamps before
    moving the key. Run it only while the key is still `id`.
    """
    assert table in TABLES
    hex_repairs = [("msg_hash_hex", "msg_hash")]
    if table == "reactions":
        hex_repairs.append(("target_hash_hex", "target_hash"))
    statements = [
        f"UPDATE {table} SET {dst} = lower({src}) WHERE {src} ~* '^[0-9a-f]{{40}}$';"
        for dst, src in hex_repairs
    ]
    statements += [
        f"UPDATE {table} SET msg_timestamp = msg_timestamp"
        f" - INTERVAL '{_OLD_EPOCH_ERROR_DAYS} days';",
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_pkey;",
        f"ALTER TABLE {table} ADD PRIMARY KEY (msg_hash_hex);",
    ]
    return "BEGIN;\n" + "\n".join(statements) + "\nCOMMIT;"


def create_casts_table():
    return """
    CREATE TABLE IF NOT EXISTS casts (
        id BIGINT,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        text TEXT,
        lang TEXT,
        embeds TEXT,
        mentions TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT PRIMARY KEY,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_casts_msg_hash_hex ON casts (msg_hash_hex);
    """


def create_comments_table():
    return """
    CREATE TABLE IF NOT EXISTS comments (
        id BIGINT,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        parent_fid BIGINT,
        parent_hash TEXT,
        parent_hash_hex TEXT,
        text TEXT,
        lang TEXT,
        embeds TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT PRIMARY KEY,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_comments_parent_hash_hex ON comments (parent_hash_hex);
    CREATE INDEX IF NOT EXISTS idx_comments_msg_hash_hex ON comments (msg_hash_hex);
    """


def create_reactions_table():
    return """
    CREATE TABLE IF NOT EXISTS reactions (
        id BIGINT,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        reaction_type TEXT,
        target_fid BIGINT,
        target_hash TEXT,
        target_hash_hex TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT PRIMARY KEY,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_reactions_target_hash_hex ON reactions (target_hash_hex);
    CREATE INDEX IF NOT EXISTS idx_reactions_msg_hash_hex ON reactions (msg_hash_hex);
    """


# Removals are recorded, not applied to the tables above: a removed cast or
# reaction keeps its row, and the api.live_* views leave it out.
REMOVED_MESSAGES = "removed_messages"

# The schema the API's generated SQL reads from, as the QUERY_ROLE role.
API_SCHEMA = "api"
LIVE_VIEWS = {f"live_{table}": table for table in TABLES}


def create_removed_messages_table():
    """One row per removed message, keyed on its hash, so recording the same
    removal again does nothing. A row may come before the message it removes,
    or without it: the views hide the message whenever it arrives.

    ``reason`` is ``cast_remove``, ``reaction_remove``, or ``replaced`` for a
    message displaced by a later add (e.g. the remove a re-added like undoes).
    A REACTION_REMOVE names its reaction by fid, type and target, not by hash;
    when Snapchain reports no reaction it displaced, the removed hash is unknown
    and the row is keyed on the remove's own hash, so it hides nothing.
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {REMOVED_MESSAGES} (
        msg_hash_hex TEXT PRIMARY KEY,
        message_type TEXT,
        fid BIGINT,
        reason TEXT NOT NULL,
        removed_by_hash_hex TEXT,
        removed_at TIMESTAMP WITH TIME ZONE,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """


def create_api_views():
    """The live_* views: each table without its removed rows.

    ``t.*`` is expanded when a view is created, so this runs on every start,
    after any table change, to pick up new columns.
    """
    views = "\n".join(
        f"""
    CREATE OR REPLACE VIEW {API_SCHEMA}.{view} AS
    SELECT t.*
    FROM public.{table} t
    WHERE NOT EXISTS (
        SELECT 1 FROM public.{REMOVED_MESSAGES} r WHERE r.msg_hash_hex = t.msg_hash_hex
    );"""
        for view, table in LIVE_VIEWS.items()
    )
    return f"CREATE SCHEMA IF NOT EXISTS {API_SCHEMA};\n{views}"


def create_query_role(role, password=None):
    """Statements that create or update the read-only role the API runs
    generated SQL as. It can read the views in the api schema and nothing else:
    the views read the tables with their owner's rights, not the role's."""
    from psycopg2 import sql

    name = sql.Identifier(role)
    schema = sql.Identifier(API_SCHEMA)
    statements = [
        sql.SQL(
            "DO $$ BEGIN CREATE ROLE {} LOGIN;"
            " EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
        ).format(name),
        sql.SQL("ALTER ROLE {} SET search_path = {}").format(name, schema),
        sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(name),
        sql.SQL("REVOKE ALL ON SCHEMA public FROM {}").format(name),
        sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {}").format(name),
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, name),
        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(schema, name),
    ]
    if password:
        statements.insert(
            1,
            sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                name, sql.Literal(password)
            ),
        )
    return statements

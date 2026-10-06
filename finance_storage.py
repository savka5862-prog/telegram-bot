"""Transactional storage for the SQLite workspace and PostgreSQL publishing."""
import fcntl
import hashlib
import os
import re
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from pathlib import Path

DEFAULT_SQLITE_FILE = Path(__file__).resolve().with_name("bot_database.db")
_SQL_TOKENS = re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|\?")


def postgres_query(query):
    """Translate SQL placeholders, never bound values or quoted literals."""
    return _SQL_TOKENS.sub(
        lambda match: "%s" if match.group() == "?" else match.group(),
        query.replace("%", "%%"),
    )


def uses_postgres(db_file=None):
    # Explicit temporary files keep offline unit tests independent of the live DB.
    if db_file and Path(db_file).resolve() != DEFAULT_SQLITE_FILE:
        return False
    backend = os.getenv("BOT_DB_BACKEND", "postgres")
    if backend not in ("postgres", "sqlite"):
        raise RuntimeError("BOT_DB_BACKEND must be postgres or sqlite.")
    if os.getenv("APP_ENV") == "production" and backend != "postgres":
        raise RuntimeError("Published apps require persistent PostgreSQL storage.")
    return backend == "postgres"


class PostgresConnection:
    is_postgres = True

    def __init__(self, connection):
        self.connection = connection

    def execute(self, query, params=None):
        sql = postgres_query(query) if params is not None else query
        return self.connection.execute(sql, params)


@contextmanager
def connection(db_file=None):
    if not uses_postgres(db_file):
        with closing(sqlite3.connect(db_file or DEFAULT_SQLITE_FILE)) as conn, conn:
            yield conn
        return

    import psycopg

    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("Persistent DATABASE_URL is required; SQLite fallback is disabled.")
    
    with psycopg.connect(url, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS records (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    chat_id BIGINT,
                    amount REAL,
                    category TEXT,
                    comment TEXT,
                    details TEXT,
                    deal_number TEXT,
                    accounting_period TEXT,
                    date TIMESTAMP
                );
                
                ALTER TABLE records ADD COLUMN IF NOT EXISTS chat_id BIGINT;
                ALTER TABLE records ADD COLUMN IF NOT EXISTS details TEXT;
                ALTER TABLE records ADD COLUMN IF NOT EXISTS deal_number TEXT;
                ALTER TABLE records ADD COLUMN IF NOT EXISTS accounting_period TEXT;
            """)
        conn.commit()
        yield PostgresConnection(conn)


@contextmanager
def polling_lock(db_file=None):
    """One Telegram poller per database, including overlapping process starts."""
    if not uses_postgres(db_file):
        # Do not hold a database write lock or modify accounting records.
        database_path = str(Path(db_file or DEFAULT_SQLITE_FILE).resolve())
        lock_id = hashlib.sha256(database_path.encode()).hexdigest()[:24]
        lock_path = Path(tempfile.gettempdir()) / f"finance-bot-{lock_id}.lock"
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError(
                    "Another Finance Bot process already owns Telegram polling."
                ) from error
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
        return

    import psycopg

    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
        acquired = conn.execute("SELECT pg_try_advisory_lock(718941203)").fetchone()[0]
        if not acquired:
            raise RuntimeError("Another Finance Bot process already owns Telegram polling.")
        try:
            yield
        finally:
            conn.execute("SELECT pg_advisory_unlock(718941203)")

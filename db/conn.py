"""
Database connection factory for deved SQLite DB.
Use get_engine() for pandas/sqlalchemy; get_conn() for raw sqlite3.
"""
from pathlib import Path
import os
from typing import Optional

# Default DB path: project root / deved.db
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = os.environ.get("DEVED_DB_PATH", str(_PROJECT_ROOT / "deved.db"))


def get_engine(db_path: Optional[str] = None):
    from sqlalchemy import create_engine
    path = db_path or DEFAULT_DB_PATH
    return create_engine(f"sqlite:///{path}", future=True)


def get_conn(db_path: Optional[str] = None):
    import sqlite3
    path = db_path or DEFAULT_DB_PATH
    return sqlite3.connect(path)


def ensure_schema(engine=None):
    """Create tables if they do not exist (run schema.sql)."""
    from sqlalchemy import text
    if engine is None:
        engine = get_engine()
    schema_path = Path(__file__).resolve().parent / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        for raw in sql.split(";"):
            # Drop full-line comments so chunk may start with -- from previous comment
            stmt = "\n".join(
                line for line in raw.strip().split("\n")
                if line.strip() and not line.strip().startswith("--")
            ).strip()
            if stmt and "CREATE TABLE" in stmt:
                conn.execute(text(stmt))

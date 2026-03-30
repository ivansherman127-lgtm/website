"""
Optional: log successful Google Sheets syncs to sheets_sync_log for audit.
Call log_sync(sheet_id, worksheet_name, row_count) after a successful push.
"""
from datetime import datetime
from typing import Optional

from .conn import get_engine


def log_sync(
    sheet_id: str,
    worksheet_name: str,
    row_count: Optional[int] = None,
    db_path: Optional[str] = None,
) -> None:
    """Insert one row into sheets_sync_log."""
    engine = get_engine(db_path)
    synced_at = datetime.utcnow().isoformat() + "Z"
    with engine.connect() as conn:
        from sqlalchemy import text
        conn.execute(
            text(
                "INSERT INTO sheets_sync_log (sheet_id, worksheet_name, synced_at, row_count) "
                "VALUES (:sheet_id, :worksheet_name, :synced_at, :row_count)"
            ),
            dict(
                sheet_id=sheet_id,
                worksheet_name=worksheet_name,
                synced_at=synced_at,
                row_count=row_count,
            ),
        )
        conn.commit()

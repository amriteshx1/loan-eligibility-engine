from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from .config import get_settings


@contextmanager
def connection() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(get_settings().database_url, row_factory=dict_row) as conn:
        yield conn


def database_is_ready() -> bool:
    try:
        with connection() as conn:
            conn.execute("SELECT 1")
        return True
    except psycopg.Error:
        return False


def create_batch(
    batch_id: UUID,
    object_key: str,
    original_filename: str,
) -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO ingestion_batches (id, object_key, original_filename)
            VALUES (%s, %s, %s)
            """,
            (batch_id, object_key, original_filename),
        )


def get_batch_by_object_key(object_key: str) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute(
            """
            SELECT id, status, workflow_triggered_at
            FROM ingestion_batches
            WHERE object_key = %s
            """,
            (object_key,),
        ).fetchone()
    return dict(row) if row else None


def get_batch(batch_id: UUID) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute(
            """
            SELECT id, original_filename, status, row_count, error_message,
                   workflow_triggered_at, workflow_trigger_error,
                   created_at, updated_at
            FROM ingestion_batches
            WHERE id = %s
            """,
            (batch_id,),
        ).fetchone()
    return dict(row) if row else None


def claim_pending_batch(object_key: str) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute(
            """
            UPDATE ingestion_batches
            SET status = 'processing',
                error_message = NULL,
                updated_at = NOW()
            WHERE object_key = %s
              AND status = 'pending'
            RETURNING id, object_key
            """,
            (object_key,),
        ).fetchone()
    return dict(row) if row else None


def mark_batch_completed(batch_id: UUID, row_count: int) -> None:
    with connection() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET status = 'completed',
                row_count = %s,
                error_message = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (row_count, batch_id),
        )


def mark_batch_failed(batch_id: UUID, error_message: str) -> None:
    with connection() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET status = 'failed',
                row_count = 0,
                error_message = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (error_message[:2000], batch_id),
        )


def mark_workflow_triggered(batch_id: UUID) -> None:
    with connection() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET workflow_triggered_at = NOW(),
                workflow_trigger_error = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (batch_id,),
        )


def mark_workflow_trigger_failed(batch_id: UUID, error_message: str) -> None:
    with connection() as conn:
        conn.execute(
            """
            UPDATE ingestion_batches
            SET workflow_trigger_error = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (error_message[:2000], batch_id),
        )


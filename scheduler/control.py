"""
Scheduler Component Control (PostgreSQL)
=======================================
Shared utilities for:
- Component registry (what should be running)
- Enable/disable flags (feature toggles)
- Heartbeats (running / not running for dashboard)
- Error event logging
- PostgreSQL advisory locks (singleton execution across processes)

This module is intentionally independent of APScheduler and of scheduler/status.py
so it can be used by both per-component services and the website API.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import traceback as tb_mod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.database import get_postgres, get_postgres_dedicated_connection

logger = logging.getLogger("scheduler.control")


@dataclass(frozen=True)
class ComponentDef:
    component_id: str
    kind: str  # job|service|stream
    group_name: str  # master|master2|shared
    description: str
    expected_interval_ms: Optional[int] = None
    default_enabled: bool = True


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stable_int64_from_str(value: str) -> int:
    """
    Convert a string into a stable signed int64 (for advisory locks).
    """
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    n = int.from_bytes(digest, byteorder="big", signed=False)
    # Map to signed int64 range
    if n >= 2**63:
        n -= 2**64
    return n


def ensure_components_registered(component_defs: Iterable[ComponentDef]) -> None:
    """
    Upsert component registry + ensure a settings row exists.
    Safe to call repeatedly.
    """
    defs = list(component_defs)
    if not defs:
        return

    with get_postgres() as conn:
        with conn.cursor() as cursor:
            for c in defs:
                cursor.execute(
                    """
                    INSERT INTO scheduler_components
                    (component_id, kind, group_name, description, expected_interval_ms, default_enabled, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (component_id) DO UPDATE SET
                        kind = EXCLUDED.kind,
                        group_name = EXCLUDED.group_name,
                        description = EXCLUDED.description,
                        expected_interval_ms = EXCLUDED.expected_interval_ms,
                        default_enabled = EXCLUDED.default_enabled,
                        updated_at = NOW()
                    """,
                    [
                        c.component_id,
                        c.kind,
                        c.group_name,
                        c.description,
                        c.expected_interval_ms,
                        c.default_enabled,
                    ],
                )

                # Ensure a settings row exists (defaults to default_enabled)
                cursor.execute(
                    """
                    INSERT INTO scheduler_component_settings (component_id, enabled, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (component_id) DO NOTHING
                    """,
                    [c.component_id, c.default_enabled],
                )


def set_component_enabled(component_id: str, enabled: bool, updated_by: Optional[str] = None, note: Optional[str] = None) -> bool:
    """
    Persist enable/disable toggle for a component.
    Returns True if the component exists.
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM scheduler_components WHERE component_id = %s",
                [component_id],
            )
            exists = cursor.fetchone() is not None
            if not exists:
                return False

            cursor.execute(
                """
                INSERT INTO scheduler_component_settings (component_id, enabled, updated_at, updated_by, note)
                VALUES (%s, %s, NOW(), %s, %s)
                ON CONFLICT (component_id) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    updated_at = NOW(),
                    updated_by = EXCLUDED.updated_by,
                    note = EXCLUDED.note
                """,
                [component_id, enabled, updated_by, note],
            )
            return True


def get_component_enabled(component_id: str) -> Optional[bool]:
    """
    Returns enabled flag, or None if component does not exist.
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(s.enabled, c.default_enabled) AS enabled
                FROM scheduler_components c
                LEFT JOIN scheduler_component_settings s ON s.component_id = c.component_id
                WHERE c.component_id = %s
                """,
                [component_id],
            )
            row = cursor.fetchone()
            return row["enabled"] if row else None


def upsert_heartbeat(
    component_id: str,
    instance_id: str,
    status: str,
    host: Optional[str] = None,
    pid: Optional[int] = None,
    started_at: Optional[datetime] = None,
    last_error_at: Optional[datetime] = None,
    last_error_message: Optional[str] = None,
    heartbeat_at: Optional[datetime] = None,
) -> None:
    """
    Upsert a heartbeat row for (component_id, instance_id).
    """
    hb = heartbeat_at or _utcnow()
    host = host or socket.gethostname()
    pid = pid if pid is not None else os.getpid()

    if last_error_message and len(last_error_message) > 500:
        last_error_message = last_error_message[:497] + "..."

    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO scheduler_component_heartbeats
                (component_id, instance_id, host, pid, started_at, last_heartbeat_at, status, last_error_at, last_error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (component_id, instance_id) DO UPDATE SET
                    host = EXCLUDED.host,
                    pid = EXCLUDED.pid,
                    started_at = COALESCE(scheduler_component_heartbeats.started_at, EXCLUDED.started_at),
                    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
                    status = EXCLUDED.status,
                    last_error_at = EXCLUDED.last_error_at,
                    last_error_message = EXCLUDED.last_error_message
                """,
                [
                    component_id,
                    instance_id,
                    host,
                    pid,
                    started_at,
                    hb,
                    status,
                    last_error_at,
                    last_error_message,
                ],
            )


def record_error_event(
    component_id: str,
    message: str,
    instance_id: Optional[str] = None,
    host: Optional[str] = None,
    pid: Optional[int] = None,
    traceback_text: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Persist a structured error event for later review in the dashboard.
    """
    host = host or socket.gethostname()
    pid = pid if pid is not None else os.getpid()
    msg = message or "Unknown error"
    if len(msg) > 500:
        msg = msg[:497] + "..."

    tb_text = traceback_text
    if tb_text and len(tb_text) > 100_000:
        tb_text = tb_text[:100_000] + "\n... (truncated)"

    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO scheduler_error_events
                (component_id, occurred_at, host, pid, instance_id, message, traceback, context)
                VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s::jsonb)
                """,
                [
                    component_id,
                    host,
                    pid,
                    instance_id,
                    msg,
                    tb_text,
                    json.dumps(context or {}),
                ],
            )


def acquire_component_lock(component_id: str, application_name: Optional[str] = None) -> Optional[Any]:
    """
    Acquire a session-level advisory lock for a component_id and return the dedicated connection.
    The caller must close() the returned connection to release the lock.
    
    Returns:
        Dedicated psycopg2 connection if lock acquired, else None.
    """
    lock_key = stable_int64_from_str(component_id)
    app_name = application_name or f"ftg_component:{component_id}"
    conn = None
    try:
        conn = get_postgres_dedicated_connection(application_name=app_name)
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s) AS locked", [lock_key])
            row = cursor.fetchone()
            locked = bool(row["locked"]) if row else False
        if locked:
            return conn
        conn.close()
        return None
    except Exception as e:
        logger.error(f"Failed to acquire advisory lock for {component_id}: {e}")
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return None


def safe_capture_traceback(exc: BaseException) -> str:
    return "".join(tb_mod.format_exception(type(exc), exc, exc.__traceback__))


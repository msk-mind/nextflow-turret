"""SQLite-backed store for persisting workflow run state and launch records.

Uses stdlib ``sqlite3`` only — no ORM dependency.

Schema
------
``runs`` table stores one row per workflow, with JSON-serialised columns for
the structured fields (task_counts, processes, resources, failures).  Rows
are upserted on every state change so the DB is always up-to-date.

``launches`` table stores one row per pipeline launch submitted through the
Turret launcher.

Connection handling
-------------------
A single ``sqlite3.Connection`` (``check_same_thread=False``) is kept open for
the lifetime of the store.  This is required for ``":memory:"`` databases,
where every new ``connect()`` call would produce a fresh empty database.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Optional


_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS runs (
    workflow_id  TEXT PRIMARY KEY,
    batch_id     TEXT NOT NULL,
    run_name     TEXT NOT NULL,
    complete     INTEGER NOT NULL DEFAULT 0,
    task_counts  TEXT NOT NULL DEFAULT '{}',
    processes    TEXT NOT NULL DEFAULT '[]',
    resources    TEXT NOT NULL DEFAULT '{}',
    failures     TEXT NOT NULL DEFAULT '[]',
    started_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
)
"""

_CREATE_LAUNCHES = """
CREATE TABLE IF NOT EXISTS launches (
    launch_id    TEXT PRIMARY KEY,
    pipeline     TEXT NOT NULL,
    revision     TEXT,
    params       TEXT NOT NULL DEFAULT '{}',
    profile      TEXT,
    work_dir     TEXT,
    run_name     TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    pid          INTEGER,
    exit_code    INTEGER,
    log_path     TEXT NOT NULL,
    submitted_at REAL NOT NULL,
    started_at   REAL,
    finished_at  REAL
)
"""

_UPSERT_RUN = """
INSERT INTO runs
    (workflow_id, batch_id, run_name, complete,
     task_counts, processes, resources, failures,
     started_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workflow_id) DO UPDATE SET
    complete    = excluded.complete,
    task_counts = excluded.task_counts,
    processes   = excluded.processes,
    resources   = excluded.resources,
    failures    = excluded.failures,
    updated_at  = excluded.updated_at
"""

_UPSERT_LAUNCH = """
INSERT INTO launches
    (launch_id, pipeline, revision, params, profile, work_dir,
     run_name, status, pid, exit_code, log_path,
     submitted_at, started_at, finished_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(launch_id) DO UPDATE SET
    status      = excluded.status,
    pid         = excluded.pid,
    exit_code   = excluded.exit_code,
    started_at  = excluded.started_at,
    finished_at = excluded.finished_at
"""


class RunStore:
    """Thin wrapper around a SQLite database for persisting workflow runs.

    Parameters
    ----------
    db_path:
        Path to the SQLite file, or ``":memory:"`` for an ephemeral in-process
        database (useful for testing).
    """

    def __init__(self, db_path: str | Path = "turret.db") -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute(_CREATE_RUNS)
            self._conn.execute(_CREATE_LAUNCHES)
            self._conn.commit()

    # ------------------------------------------------------------------
    # Runs — write
    # ------------------------------------------------------------------

    def upsert(self, state: dict) -> None:
        """Insert or update a run from a :meth:`WorkflowState.as_dict` snapshot."""
        with self._lock:
            self._conn.execute(_UPSERT_RUN, (
                state["workflow_id"],
                state["batch_id"],
                state["run_name"],
                int(state["complete"]),
                json.dumps(state["task_counts"]),
                json.dumps(state["processes"]),
                json.dumps(state["resources"]),
                json.dumps(state["failures"]),
                state["started_at"],
                state["updated_at"],
            ))
            self._conn.commit()

    # ------------------------------------------------------------------
    # Runs — read
    # ------------------------------------------------------------------

    def load_all(self) -> list[dict]:
        """Return all persisted runs, oldest first."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY started_at"
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get(self, workflow_id: str) -> Optional[dict]:
        """Return a single run by workflow_id, or ``None``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_id = ?", (workflow_id,)
            ).fetchone()
        return self._row_to_dict(row) if row else None

    # ------------------------------------------------------------------
    # Launches — write
    # ------------------------------------------------------------------

    def upsert_launch(self, launch: dict) -> None:
        """Insert or update a launch record from a :meth:`LaunchRecord.as_dict` snapshot."""
        with self._lock:
            self._conn.execute(_UPSERT_LAUNCH, (
                launch["launch_id"],
                launch["pipeline"],
                launch["revision"],
                json.dumps(launch["params"]),
                launch["profile"],
                launch["work_dir"],
                launch["run_name"],
                launch["status"],
                launch["pid"],
                launch["exit_code"],
                launch["log_path"],
                launch["submitted_at"],
                launch["started_at"],
                launch["finished_at"],
            ))
            self._conn.commit()

    # ------------------------------------------------------------------
    # Launches — read
    # ------------------------------------------------------------------

    def load_all_launches(self) -> list[dict]:
        """Return all persisted launches, most-recent first."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM launches ORDER BY submitted_at DESC"
            ).fetchall()
        return [self._launch_row_to_dict(r) for r in rows]

    def get_launch(self, launch_id: str) -> Optional[dict]:
        """Return a single launch by launch_id, or ``None``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM launches WHERE launch_id = ?", (launch_id,)
            ).fetchone()
        return self._launch_row_to_dict(row) if row else None

    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["complete"]    = bool(d["complete"])
        d["task_counts"] = json.loads(d["task_counts"])
        d["processes"]   = json.loads(d["processes"])
        d["resources"]   = json.loads(d["resources"])
        d["failures"]    = json.loads(d["failures"])
        return d

    @staticmethod
    def _launch_row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["params"] = json.loads(d["params"])
        return d

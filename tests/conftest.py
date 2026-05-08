"""Shared fixtures and helpers for the integration and E2E test suites."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from nextflow_turret.server.app import create_app
from nextflow_turret.state import workflow_id_for_batch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path):
    """Path to a fresh, isolated SQLite database file."""
    return str(tmp_path / "test.db")


@pytest.fixture()
def log_dir(tmp_path):
    """Temporary directory for per-launch log files."""
    d = tmp_path / "logs"
    d.mkdir()
    return str(d)


@pytest.fixture()
def mem_client(log_dir):
    """TestClient backed by an in-memory DB (fast; no cross-test persistence)."""
    return TestClient(create_app(db_path=":memory:", log_dir=log_dir))


@pytest.fixture()
def file_client(db_path, log_dir):
    """TestClient backed by a real file DB (for restart-recovery tests)."""
    return TestClient(create_app(db_path=db_path, log_dir=log_dir))


# ---------------------------------------------------------------------------
# Scenario helper
# ---------------------------------------------------------------------------

def nf_trace_lifecycle(
    client: TestClient,
    batch_id: str,
    n_tasks: int = 9,
    n_failed: int = 0,
) -> str:
    """Simulate a complete Nextflow ``-with-tower`` trace sequence.

    Calls in order:
    1. GET  /user-info
    2. POST /trace/create
    3. PUT  /trace/{wid}/begin
    4. PUT  /trace/{wid}/progress  ×3  (incremental)
    5. PUT  /trace/{wid}/complete

    Returns the *workflow_id*.
    """
    run_name = f"dispatcher_{batch_id}"
    wid = workflow_id_for_batch(batch_id)

    assert client.get("/user-info").status_code == 200

    r = client.post("/trace/create", json={"runName": run_name})
    assert r.status_code == 200, r.text
    assert r.json()["workflowId"] == wid

    client.put(f"/trace/{wid}/begin", json={"workflow": {"runName": run_name}})

    per_step = (n_tasks - n_failed) // 3
    for step in range(1, 4):
        done      = step * per_step
        remaining = (n_tasks - n_failed) - done
        r = client.put(f"/trace/{wid}/progress", json={
            "progress": {
                "succeeded": done,
                "running":   min(2, max(0, remaining)),
                "pending":   max(0, remaining - 2),
                "failed":    n_failed if step == 3 else 0,
                "cached":    0,
            },
            "tasks": (
                [{"taskId": i, "status": "FAILED", "process": "PROC", "name": f"task_{i}"}
                 for i in range(n_failed)]
                if step == 3 else []
            ),
        })
        assert r.status_code == 200

    r = client.put(f"/trace/{wid}/complete", json={
        "progress": {
            "succeeded": n_tasks - n_failed,
            "failed":    n_failed,
            "running":   0,
            "pending":   0,
        },
    })
    assert r.status_code == 200
    return wid

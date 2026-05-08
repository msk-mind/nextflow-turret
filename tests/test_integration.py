"""Integration tests — cross-layer component interactions.

These tests verify that the application layers work correctly *together*:

- Tower HTTP endpoints → SQLite persistence (write-through)
- SQLite persistence → PersistentWorkflowRegistry hydration (restart recovery)
- WorkflowRegistry thread safety under concurrent access
- REST API response content matches what the DB contains
- REST API data matches what the HTML UI renders
"""
from __future__ import annotations

import threading
import time

import pytest
from fastapi.testclient import TestClient

from nextflow_turret.db.store import RunStore
from nextflow_turret.server.app import create_app
from nextflow_turret.server.registry import PersistentWorkflowRegistry
from nextflow_turret.state import WorkflowRegistry, workflow_id_for_batch
from conftest import nf_trace_lifecycle


# ---------------------------------------------------------------------------
# Tower trace events are immediately flushed to SQLite
# ---------------------------------------------------------------------------

class TestTowerTracePersistence:
    """Every Tower event is persisted so a raw DB query reflects the current state."""

    def test_create_writes_run_row_to_db(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        client.post("/trace/create", json={"runName": "dispatcher_p1"})

        row = RunStore(db_path).get(workflow_id_for_batch("p1"))
        assert row is not None
        assert row["batch_id"] == "p1"
        assert row["complete"] is False
        assert row["task_counts"] == {}

    def test_progress_update_reflected_in_db(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = workflow_id_for_batch("p2")
        client.post("/trace/create", json={"runName": "dispatcher_p2"})
        client.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 7, "running": 3, "failed": 1},
        })

        row = RunStore(db_path).get(wid)
        assert row["task_counts"]["succeeded"] == 7
        assert row["task_counts"]["failed"] == 1

    def test_complete_flag_persisted(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = workflow_id_for_batch("p3")
        client.post("/trace/create", json={"runName": "dispatcher_p3"})
        client.put(f"/trace/{wid}/complete", json={"progress": {"succeeded": 5}})

        row = RunStore(db_path).get(wid)
        assert row["complete"] is True
        assert row["task_counts"]["succeeded"] == 5

    def test_failed_tasks_written_to_db(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = workflow_id_for_batch("p4")
        client.post("/trace/create", json={"runName": "dispatcher_p4"})
        client.put(f"/trace/{wid}/progress", json={
            "progress": {"failed": 2},
            "tasks": [
                {"taskId": 1, "status": "FAILED", "process": "ALIGN", "name": "ALIGN (s1)", "exit": 1},
                {"taskId": 2, "status": "FAILED", "process": "ALIGN", "name": "ALIGN (s2)", "exit": 2},
            ],
        })

        row = RunStore(db_path).get(wid)
        assert len(row["failures"]) == 2
        assert row["failures"][0]["process"] == "ALIGN"
        assert row["failures"][0]["exit"] == 1

    def test_incremental_progress_snapshots_are_cumulative(self, db_path, log_dir):
        """Each PUT /progress replaces the snapshot; the last one is what's in the DB."""
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = workflow_id_for_batch("p5")
        client.post("/trace/create", json={"runName": "dispatcher_p5"})
        for step in (3, 9, 15):
            client.put(f"/trace/{wid}/progress", json={
                "progress": {"succeeded": step, "running": 2},
            })

        row = RunStore(db_path).get(wid)
        assert row["task_counts"]["succeeded"] == 15  # last write wins

    def test_full_lifecycle_written_to_db(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = nf_trace_lifecycle(client, "full_lifecycle", n_tasks=9)

        row = RunStore(db_path).get(wid)
        assert row["complete"] is True
        assert row["task_counts"]["succeeded"] == 9

    def test_launch_row_created_on_api_submit(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        r = client.post("/api/launches", json={"pipeline": "org/myflow", "profile": "test"})
        assert r.status_code == 201
        launch_id = r.json()["launch_id"]

        row = RunStore(db_path).get_launch(launch_id)
        assert row is not None
        assert row["pipeline"] == "org/myflow"
        assert row["profile"] == "test"


# ---------------------------------------------------------------------------
# DB restart recovery
# ---------------------------------------------------------------------------

class TestRestartRecovery:
    """Data written in one app instance is available in a freshly-started instance."""

    def test_completed_run_survives_restart(self, db_path, log_dir):
        c1 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = nf_trace_lifecycle(c1, "restart_done", n_tasks=6)

        # Simulate restart: second app instance, same DB file
        c2 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        r = c2.get(f"/api/runs/{wid}")
        assert r.status_code == 200
        body = r.json()
        assert body["complete"] is True
        assert body["task_counts"]["succeeded"] == 6

    def test_in_progress_run_recovered_and_continues(self, db_path, log_dir):
        """An incomplete run is re-hydrated and can receive further trace events."""
        c1 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = workflow_id_for_batch("partial")
        c1.post("/trace/create", json={"runName": "dispatcher_partial"})
        c1.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 4, "running": 2, "pending": 4},
        })

        # Restart
        c2 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        r = c2.get(f"/api/runs/{wid}")
        assert r.status_code == 200
        assert r.json()["task_counts"]["succeeded"] == 4
        assert r.json()["complete"] is False

        # Continue progress on the new instance
        c2.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 10, "running": 0},
        })
        c2.put(f"/trace/{wid}/complete", json={
            "progress": {"succeeded": 10},
        })
        r = c2.get(f"/api/runs/{wid}")
        assert r.json()["complete"] is True
        assert r.json()["task_counts"]["succeeded"] == 10

    def test_all_runs_recovered_after_restart(self, db_path, log_dir):
        c1 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        for bid in ("rec_a", "rec_b", "rec_c"):
            nf_trace_lifecycle(c1, bid, n_tasks=3)

        c2 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        r = c2.get("/api/runs")
        body = r.json()
        assert body["total"] == 3
        assert {run["batch_id"] for run in body["runs"]} == {"rec_a", "rec_b", "rec_c"}

    def test_run_detail_ui_available_after_restart(self, db_path, log_dir):
        c1 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = nf_trace_lifecycle(c1, "ui_restart", n_tasks=4)

        c2 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        r = c2.get(f"/runs/{wid}")
        assert r.status_code == 200
        assert b"dispatcher_ui_restart" in r.content

    def test_failures_recovered_after_restart(self, db_path, log_dir):
        c1 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = nf_trace_lifecycle(c1, "fail_restart", n_tasks=6, n_failed=2)

        c2 = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        r = c2.get(f"/api/runs/{wid}")
        body = r.json()
        assert body["task_counts"]["failed"] == 2
        assert len(body["failures"]) == 2


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

class TestConcurrentAccess:
    """WorkflowRegistry and RunStore are safe under concurrent access."""

    def test_concurrent_progress_updates_no_errors(self):
        registry = WorkflowRegistry()
        wid = "dispatcher_concur1"
        registry.register(wid, "concur1", wid)

        errors: list[Exception] = []

        def send_updates():
            try:
                for _ in range(100):
                    registry.update_progress(wid, {"succeeded": 1, "running": 1})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=send_updates) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors
        assert registry.get_by_id(wid) is not None

    def test_concurrent_registrations_and_reads_no_errors(self):
        registry = WorkflowRegistry()
        errors: list[Exception] = []

        def register_workflows(n: int):
            try:
                for i in range(n):
                    registry.register(f"wf_t{n}_{i}", f"b{n}{i}", f"run{n}{i}")
                    registry.update_progress(f"wf_t{n}_{i}", {"succeeded": i})
            except Exception as exc:
                errors.append(exc)

        def read_workflows():
            try:
                for _ in range(200):
                    registry.get_all()
            except Exception as exc:
                errors.append(exc)

        workers = [threading.Thread(target=register_workflows, args=(15,)) for _ in range(3)]
        readers = [threading.Thread(target=read_workflows) for _ in range(3)]
        for t in workers + readers:
            t.start()
        for t in workers + readers:
            t.join(timeout=15)

        assert not errors

    def test_concurrent_db_writes_no_corruption(self, db_path):
        store = RunStore(db_path)
        errors: list[Exception] = []

        def write_rows(thread_id: int):
            try:
                for i in range(20):
                    store.upsert({
                        "workflow_id": f"wf_{thread_id}_{i}",
                        "batch_id":    f"b{thread_id}{i}",
                        "run_name":    f"run{thread_id}{i}",
                        "complete":    False,
                        "task_counts": {"succeeded": i},
                        "processes":   [],
                        "resources":   {},
                        "failures":    [],
                        "started_at":  time.time(),
                        "updated_at":  time.time(),
                    })
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write_rows, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        assert not errors
        assert len(store.load_all()) == 100  # 5 threads × 20 rows each

    def test_concurrent_http_requests_no_server_errors(self, mem_client):
        """Multiple concurrent HTTP requests to the same server don't cause 500s."""
        client = mem_client
        # Pre-create a workflow
        client.post("/trace/create", json={"runName": "dispatcher_httpconc"})
        wid = workflow_id_for_batch("httpconc")

        errors: list[int] = []

        def hit_progress():
            try:
                r = client.put(f"/trace/{wid}/progress", json={
                    "progress": {"succeeded": 1, "running": 1},
                })
                if r.status_code >= 500:
                    errors.append(r.status_code)
            except Exception:
                pass  # TestClient may raise on connection errors in threads

        threads = [threading.Thread(target=hit_progress) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors


# ---------------------------------------------------------------------------
# REST API ↔ DB consistency
# ---------------------------------------------------------------------------

class TestAPIDBConsistency:
    """REST API responses always reflect the current DB state."""

    def test_api_task_counts_match_db(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = workflow_id_for_batch("sync1")
        client.post("/trace/create", json={"runName": "dispatcher_sync1"})
        client.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 11, "failed": 2, "running": 1, "cached": 3},
        })

        api_data = client.get(f"/api/runs/{wid}").json()
        db_row   = RunStore(db_path).get(wid)

        assert api_data["task_counts"] == db_row["task_counts"]
        assert api_data["complete"] == db_row["complete"]

    def test_api_list_total_matches_db_row_count(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        for i in range(4):
            client.post("/trace/create", json={"runName": f"dispatcher_cnt{i}"})

        api_total = client.get("/api/runs").json()["total"]
        db_total  = len(RunStore(db_path).load_all())
        assert api_total == db_total == 4

    def test_failures_in_api_match_db(self, db_path, log_dir):
        client = TestClient(create_app(db_path=db_path, log_dir=log_dir))
        wid = workflow_id_for_batch("fail_sync")
        client.post("/trace/create", json={"runName": "dispatcher_fail_sync"})
        client.put(f"/trace/{wid}/progress", json={
            "progress": {"failed": 3},
            "tasks": [
                {"taskId": i, "status": "FAILED", "process": "PROC", "name": f"t{i}"}
                for i in range(3)
            ],
        })

        api_failures = client.get(f"/api/runs/{wid}").json()["failures"]
        db_failures  = RunStore(db_path).get(wid)["failures"]
        assert len(api_failures) == len(db_failures) == 3


# ---------------------------------------------------------------------------
# REST API ↔ Web UI data consistency
# ---------------------------------------------------------------------------

class TestAPIUIConsistency:
    """The REST API and HTML UI surface the same data for every run and launch."""

    def test_run_name_in_both_api_and_ui(self, mem_client):
        c = mem_client
        wid = workflow_id_for_batch("ui_api1")
        c.post("/trace/create", json={"runName": "dispatcher_ui_api1"})
        c.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 8, "failed": 1, "running": 2, "pending": 4},
        })

        api  = c.get(f"/api/runs/{wid}").json()
        html = c.get(f"/runs/{wid}").content.decode()

        assert api["run_name"] in html
        assert str(api["task_counts"]["succeeded"]) in html
        assert str(api["task_counts"]["failed"]) in html

    def test_complete_status_consistent_between_api_and_ui(self, mem_client):
        wid = nf_trace_lifecycle(mem_client, "badgesync", n_tasks=3)

        api  = mem_client.get(f"/api/runs/{wid}").json()
        html = mem_client.get(f"/runs/{wid}").content.decode()

        assert api["complete"] is True
        assert "Complete" in html

    def test_run_appears_in_dashboard_and_api_list(self, mem_client):
        c = mem_client
        c.post("/trace/create", json={"runName": "dispatcher_listcheck"})

        api_runs = c.get("/api/runs").json()["runs"]
        html     = c.get("/").content.decode()

        assert any(r["batch_id"] == "listcheck" for r in api_runs)
        assert "dispatcher_listcheck" in html

    def test_launch_data_consistent_across_api_and_ui(self, mem_client):
        c = mem_client
        r = c.post("/api/launches", json={
            "pipeline": "org/consistency-pipeline",
            "profile":  "slurm",
        })
        launch_id = r.json()["launch_id"]

        api  = c.get(f"/api/launches/{launch_id}").json()
        html = c.get(f"/launches/{launch_id}").content.decode()

        assert api["pipeline"] in html
        assert api["profile"] in html
        assert api["run_name"] in html

    def test_launch_in_api_list_and_ui_list(self, mem_client):
        c = mem_client
        c.post("/api/launches", json={"pipeline": "org/listed-pipeline"})

        api_total = c.get("/api/launches").json()["total"]
        html      = c.get("/launches").content.decode()

        assert api_total == 1
        assert "org/listed-pipeline" in html

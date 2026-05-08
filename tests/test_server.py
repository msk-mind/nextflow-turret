"""Tests for the FastAPI server, persistence layer, and PersistentWorkflowRegistry."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from nextflow_turret.db.store import RunStore
from nextflow_turret.server.app import create_app
from nextflow_turret.server.registry import PersistentWorkflowRegistry
from nextflow_turret.state import workflow_id_for_batch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store():
    """In-memory SQLite store — isolated per test."""
    return RunStore(":memory:")


@pytest.fixture()
def registry(store):
    return PersistentWorkflowRegistry(store)


@pytest.fixture()
def client():
    """TestClient backed by an in-memory database."""
    app = create_app(db_path=":memory:")
    return TestClient(app)


# ---------------------------------------------------------------------------
# RunStore
# ---------------------------------------------------------------------------

class TestRunStore:
    def test_upsert_and_get(self, store):
        state = {
            "workflow_id": "dispatcher_b1",
            "batch_id":    "b1",
            "run_name":    "dispatcher_b1",
            "complete":    False,
            "task_counts": {"succeeded": 3},
            "processes":   [],
            "resources":   {},
            "failures":    [],
            "started_at":  1000.0,
            "updated_at":  1001.0,
        }
        store.upsert(state)
        result = store.get("dispatcher_b1")
        assert result is not None
        assert result["batch_id"] == "b1"
        assert result["task_counts"]["succeeded"] == 3
        assert result["complete"] is False

    def test_upsert_updates_existing(self, store):
        base = {
            "workflow_id": "wf1", "batch_id": "b", "run_name": "n",
            "complete": False, "task_counts": {"succeeded": 1},
            "processes": [], "resources": {}, "failures": [],
            "started_at": 1.0, "updated_at": 2.0,
        }
        store.upsert(base)
        updated = {**base, "complete": True, "task_counts": {"succeeded": 5}, "updated_at": 3.0}
        store.upsert(updated)
        result = store.get("wf1")
        assert result["complete"] is True
        assert result["task_counts"]["succeeded"] == 5

    def test_get_missing_returns_none(self, store):
        assert store.get("nonexistent") is None

    def test_load_all_ordered_by_started_at(self, store):
        for i in range(3):
            store.upsert({
                "workflow_id": f"wf{i}", "batch_id": f"b{i}", "run_name": f"n{i}",
                "complete": False, "task_counts": {}, "processes": [],
                "resources": {}, "failures": [],
                "started_at": float(i), "updated_at": float(i),
            })
        rows = store.load_all()
        assert [r["workflow_id"] for r in rows] == ["wf0", "wf1", "wf2"]

    def test_load_all_empty(self, store):
        assert store.load_all() == []


# ---------------------------------------------------------------------------
# PersistentWorkflowRegistry
# ---------------------------------------------------------------------------

class TestPersistentWorkflowRegistry:
    def test_register_persists(self, registry, store):
        registry.register("wf1", "b1", "dispatcher_b1")
        assert store.get("wf1") is not None

    def test_update_progress_persists(self, registry, store):
        registry.register("wf1", "b1", "n1")
        registry.update_progress("wf1", {"succeeded": 7})
        row = store.get("wf1")
        assert row["task_counts"]["succeeded"] == 7

    def test_mark_complete_persists(self, registry, store):
        registry.register("wf1", "b1", "n1")
        registry.mark_complete("wf1")
        row = store.get("wf1")
        assert row["complete"] is True

    def test_hydrates_on_startup(self, store):
        store.upsert({
            "workflow_id": "wf_old", "batch_id": "old", "run_name": "n",
            "complete": True, "task_counts": {"succeeded": 10},
            "processes": [], "resources": {}, "failures": [],
            "started_at": 1.0, "updated_at": 2.0,
        })
        reg2 = PersistentWorkflowRegistry(store)
        state = reg2.get_by_id("wf_old")
        assert state is not None
        assert state["task_counts"]["succeeded"] == 10
        assert state["complete"] is True


# ---------------------------------------------------------------------------
# Tower trace endpoints
# ---------------------------------------------------------------------------

class TestTowerEndpoints:
    def test_user_info(self, client):
        r = client.get("/user-info")
        assert r.status_code == 200
        assert "user" in r.json()

    def test_trace_create(self, client):
        r = client.post("/trace/create", json={"runName": "dispatcher_mybatch"})
        assert r.status_code == 200
        assert r.json()["workflowId"] == "dispatcher_mybatch"

    def test_trace_create_empty_body(self, client):
        r = client.post("/trace/create", json={})
        assert r.status_code == 200
        assert r.json()["workflowId"]  # UUID assigned

    def test_trace_progress(self, client):
        client.post("/trace/create", json={"runName": "dispatcher_p1"})
        wid = workflow_id_for_batch("p1")
        r = client.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 4, "running": 1},
            "tasks": [],
        })
        assert r.status_code == 200

    def test_trace_complete(self, client):
        client.post("/trace/create", json={"runName": "dispatcher_c1"})
        wid = workflow_id_for_batch("c1")
        r = client.put(f"/trace/{wid}/complete", json={"progress": {"succeeded": 5}})
        assert r.status_code == 200

    def test_unknown_action_returns_404(self, client):
        r = client.put("/trace/somewf/unknown", json={})
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------

class TestRestAPI:
    def test_list_runs_empty(self, client):
        r = client.get("/api/runs")
        assert r.status_code == 200
        assert r.json() == {"runs": [], "total": 0}

    def test_list_runs_after_create(self, client):
        client.post("/trace/create", json={"runName": "dispatcher_r1"})
        r = client.get("/api/runs")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["runs"][0]["batch_id"] == "r1"

    def test_get_run(self, client):
        client.post("/trace/create", json={"runName": "dispatcher_r2"})
        wid = workflow_id_for_batch("r2")
        r = client.get(f"/api/runs/{wid}")
        assert r.status_code == 200
        assert r.json()["workflow_id"] == wid

    def test_get_run_not_found(self, client):
        r = client.get("/api/runs/nonexistent_wf")
        assert r.status_code == 404

    def test_list_runs_sorted_most_recent_first(self, client):
        for name in ["dispatcher_a", "dispatcher_b", "dispatcher_c"]:
            client.post("/trace/create", json={"runName": name})
        runs = client.get("/api/runs").json()["runs"]
        # most recent submission should be first
        assert runs[0]["batch_id"] == "c"

    def test_progress_reflected_in_api(self, client):
        client.post("/trace/create", json={"runName": "dispatcher_api1"})
        wid = workflow_id_for_batch("api1")
        client.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 8, "running": 2},
        })
        r = client.get(f"/api/runs/{wid}")
        body = r.json()
        assert body["task_counts"]["succeeded"] == 8
        assert body["pct"] == 80

# ---------------------------------------------------------------------------
# Launcher API
# ---------------------------------------------------------------------------

class TestLauncherAPI:
    def test_list_launches_empty(self, client):
        r = client.get("/api/launches")
        assert r.status_code == 200
        assert r.json() == {"launches": [], "total": 0}

    def test_submit_launch(self, client):
        r = client.post("/api/launches", json={
            "pipeline": "https://github.com/example/pipeline",
            "revision": "main",
            "params":   {"input": "data.csv"},
            "profile":  "test",
        })
        assert r.status_code == 201
        body = r.json()
        assert body["launch_id"]
        assert body["pipeline"] == "https://github.com/example/pipeline"
        assert body["status"] in ("pending", "running", "failed")  # subprocess may fail fast
        assert body["run_name"].startswith("dispatcher_")

    def test_submit_launch_custom_run_name(self, client):
        r = client.post("/api/launches", json={
            "pipeline": "org/pipeline",
            "run_name": "dispatcher_mybatch",
        })
        assert r.status_code == 201
        assert r.json()["run_name"] == "dispatcher_mybatch"

    def test_get_launch(self, client):
        r = client.post("/api/launches", json={"pipeline": "org/p"})
        launch_id = r.json()["launch_id"]
        r2 = client.get(f"/api/launches/{launch_id}")
        assert r2.status_code == 200
        assert r2.json()["launch_id"] == launch_id

    def test_get_launch_not_found(self, client):
        r = client.get("/api/launches/nonexistent-id")
        assert r.status_code == 404

    def test_get_launch_log(self, client):
        r = client.post("/api/launches", json={"pipeline": "org/p"})
        launch_id = r.json()["launch_id"]
        r2 = client.get(f"/api/launches/{launch_id}/log")
        assert r2.status_code == 200
        assert isinstance(r2.text, str)

    def test_get_launch_log_tail(self, client):
        r = client.post("/api/launches", json={"pipeline": "org/p"})
        launch_id = r.json()["launch_id"]
        r2 = client.get(f"/api/launches/{launch_id}/log?tail=5")
        assert r2.status_code == 200

    def test_cancel_launch_not_cancellable(self, client):
        """Cancelling a launch that has already failed/succeeded returns 409."""
        import time
        r = client.post("/api/launches", json={"pipeline": "org/p"})
        launch_id = r.json()["launch_id"]
        # wait briefly for the subprocess to fail (nextflow not installed in test env)
        time.sleep(0.5)
        r2 = client.get(f"/api/launches/{launch_id}")
        status = r2.json()["status"]
        if status in ("failed", "succeeded", "cancelled"):
            r3 = client.delete(f"/api/launches/{launch_id}")
            assert r3.status_code == 409
        # if still running (unlikely in CI), skip assertion

    def test_list_launches_after_submit(self, client):
        client.post("/api/launches", json={"pipeline": "org/p1"})
        client.post("/api/launches", json={"pipeline": "org/p2"})
        r = client.get("/api/launches")
        body = r.json()
        assert body["total"] == 2


# ---------------------------------------------------------------------------
# RunStore — launches persistence
# ---------------------------------------------------------------------------

class TestRunStoreLaunches:
    def test_upsert_and_get_launch(self, store):
        launch = {
            "launch_id": "abc-123", "pipeline": "org/p", "revision": "main",
            "params": {"k": "v"}, "profile": "test", "work_dir": None,
            "run_name": "dispatcher_abc", "status": "running",
            "pid": 12345, "exit_code": None, "log_path": "/tmp/abc.log",
            "submitted_at": 1000.0, "started_at": 1001.0, "finished_at": None,
        }
        store.upsert_launch(launch)
        result = store.get_launch("abc-123")
        assert result is not None
        assert result["pipeline"] == "org/p"
        assert result["params"] == {"k": "v"}
        assert result["pid"] == 12345

    def test_upsert_updates_launch_status(self, store):
        base = {
            "launch_id": "xyz", "pipeline": "p", "revision": None,
            "params": {}, "profile": None, "work_dir": None,
            "run_name": "n", "status": "running",
            "pid": 99, "exit_code": None, "log_path": "/tmp/x.log",
            "submitted_at": 1.0, "started_at": 2.0, "finished_at": None,
        }
        store.upsert_launch(base)
        store.upsert_launch({**base, "status": "succeeded", "exit_code": 0, "finished_at": 3.0})
        result = store.get_launch("xyz")
        assert result["status"] == "succeeded"
        assert result["exit_code"] == 0

    def test_load_all_launches(self, store):
        for i in range(3):
            store.upsert_launch({
                "launch_id": f"l{i}", "pipeline": "p", "revision": None,
                "params": {}, "profile": None, "work_dir": None,
                "run_name": "n", "status": "running",
                "pid": None, "exit_code": None, "log_path": "/tmp/x.log",
                "submitted_at": float(i), "started_at": None, "finished_at": None,
            })
        rows = store.load_all_launches()
        assert len(rows) == 3
        # most-recent first
        assert rows[0]["launch_id"] == "l2"

    def test_get_launch_missing(self, store):
        assert store.get_launch("nope") is None


# ---------------------------------------------------------------------------
# Web UI routes
# ---------------------------------------------------------------------------

class TestWebUI:
    def test_index_renders(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert b"Nextflow Turret" in r.content

    def test_index_shows_run(self, client):
        client.post("/trace/create", json={"runName": "dispatcher_ui1"})
        r = client.get("/")
        assert r.status_code == 200
        assert b"dispatcher_ui1" in r.content

    def test_run_detail_renders(self, client):
        client.post("/trace/create", json={"runName": "dispatcher_det1"})
        wid = workflow_id_for_batch("det1")
        r = client.get(f"/runs/{wid}")
        assert r.status_code == 200
        assert b"dispatcher_det1" in r.content

    def test_run_detail_not_found(self, client):
        r = client.get("/runs/nonexistent_wf_id")
        assert r.status_code == 404

    def test_launches_page_renders(self, client):
        r = client.get("/launches")
        assert r.status_code == 200
        assert b"Nextflow Turret" in r.content

    def test_launch_form_renders(self, client):
        r = client.get("/launch")
        assert r.status_code == 200
        assert b"Pipeline" in r.content

    def test_launch_form_post_redirects(self, client):
        r = client.post(
            "/launch",
            data={"pipeline": "org/test-pipeline"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"].startswith("/launches/")

    def test_launch_form_invalid_json_params(self, client):
        r = client.post(
            "/launch",
            data={"pipeline": "org/p", "params": "not-json"},
        )
        assert r.status_code == 422
        assert b"Invalid params JSON" in r.content

    def test_launch_detail_renders(self, client):
        r = client.post(
            "/launch",
            data={"pipeline": "org/p"},
            follow_redirects=False,
        )
        launch_url = r.headers["location"]
        r2 = client.get(launch_url)
        assert r2.status_code == 200
        assert b"org/p" in r2.content

    def test_launches_list_shows_submitted_launch(self, client):
        client.post("/launch", data={"pipeline": "org/listed"}, follow_redirects=False)
        r = client.get("/launches")
        assert r.status_code == 200
        assert b"org/listed" in r.content


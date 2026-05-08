"""Tests for nextflow-turret."""
from __future__ import annotations

import time
import pytest

import nextflow_turret as nt
from nextflow_turret.state import WorkflowRegistry, WorkflowState, workflow_id_for_batch
from nextflow_turret.handlers import TowerRouter, user_info_response, trace_create_response
from nextflow_turret.utils import tower_process_to_slurm_name


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry():
    """Fresh isolated registry for each test."""
    return WorkflowRegistry()


@pytest.fixture()
def router(registry):
    return TowerRouter(registry=registry)


# ---------------------------------------------------------------------------
# WorkflowRegistry
# ---------------------------------------------------------------------------

class TestWorkflowRegistry:
    def test_register_and_lookup(self, registry):
        wid = "wf1"
        registry.register(wid, "batch1", "dispatcher_batch1")
        assert registry.is_registered(wid)
        state = registry.get_by_id(wid)
        assert state is not None
        assert state["batch_id"] == "batch1"
        assert state["run_name"] == "dispatcher_batch1"

    def test_get_by_batch(self, registry):
        registry.register("dispatcher_abc", "abc", "dispatcher_abc")
        state = registry.get_by_batch("abc")
        assert state is not None
        assert state["workflow_id"] == "dispatcher_abc"

    def test_get_by_batch_fallback(self, registry):
        """When canonical workflow_id not present, fall back to scanning batch_id."""
        registry.register("custom_wid", "abc", "dispatcher_abc")
        state = registry.get_by_batch("abc")
        assert state is not None
        assert state["workflow_id"] == "custom_wid"

    def test_get_by_batch_missing(self, registry):
        assert registry.get_by_batch("nonexistent") is None

    def test_update_progress(self, registry):
        wid = "dispatcher_x"
        registry.register(wid, "x", "dispatcher_x")
        registry.update_progress(wid, {"succeeded": 5, "running": 2})
        state = registry.get_by_id(wid)
        assert state["task_counts"]["succeeded"] == 5
        assert state["task_counts"]["running"] == 2

    def test_update_progress_ignores_unknown(self, registry):
        """update_progress on an unregistered workflow should not raise."""
        registry.update_progress("unknown", {"succeeded": 1})

    def test_mark_complete(self, registry):
        wid = "dispatcher_done"
        registry.register(wid, "done", wid)
        registry.update_progress(wid, {"succeeded": 3})
        registry.mark_complete(wid)
        state = registry.get_by_id(wid)
        assert state["complete"] is True

    def test_evict_old(self, registry):
        wid = "dispatcher_old"
        registry.register(wid, "old", wid)
        registry.mark_complete(wid)
        # Force updated_at into the past
        with registry._lock:
            registry._workflows[wid].updated_at = time.time() - 7200
        removed = registry.evict_old(max_age_seconds=3600)
        assert removed == 1
        assert not registry.is_registered(wid)

    def test_evict_does_not_remove_active(self, registry):
        wid = "dispatcher_new"
        registry.register(wid, "new", wid)
        registry.mark_complete(wid)
        removed = registry.evict_old(max_age_seconds=3600)
        assert removed == 0

    def test_failures_accumulate(self, registry):
        wid = "dispatcher_f"
        registry.register(wid, "f", wid)
        tasks = [{"taskId": i, "status": "FAILED", "process": "PROC", "name": f"task_{i}"}
                 for i in range(5)]
        registry.update_progress(wid, {}, tasks)
        state = registry.get_by_id(wid)
        assert len(state["failures"]) == 5

    def test_failures_capped_at_50(self, registry):
        wid = "dispatcher_many"
        registry.register(wid, "many", wid)
        for batch_start in range(0, 60, 10):
            tasks = [{"taskId": batch_start + i, "status": "FAILED", "process": "P"}
                     for i in range(10)]
            registry.update_progress(wid, {}, tasks)
        state = registry.get_by_id(wid)
        assert len(state["failures"]) <= 50

    def test_get_all(self, registry):
        registry.register("wf1", "b1", "n1")
        registry.register("wf2", "b2", "n2")
        all_states = registry.get_all()
        assert len(all_states) == 2

    def test_stalled_detection(self, registry):
        wid = "dispatcher_stale"
        registry.register(wid, "stale", wid)
        with registry._lock:
            registry._workflows[wid].updated_at = time.time() - 400
        state = registry.get_by_id(wid)
        assert state["stalled"] is True

    def test_not_stalled_when_complete(self, registry):
        wid = "dispatcher_done2"
        registry.register(wid, "done2", wid)
        registry.mark_complete(wid)
        with registry._lock:
            registry._workflows[wid].updated_at = time.time() - 400
        state = registry.get_by_id(wid)
        assert state["stalled"] is False


# ---------------------------------------------------------------------------
# WorkflowState.as_dict
# ---------------------------------------------------------------------------

class TestWorkflowStateAsDict:
    def test_pct_zero_when_no_tasks(self, registry):
        registry.register("wf", "b", "n")
        state = registry.get_by_id("wf")
        assert state["pct"] == 0
        assert state["total"] == 0

    def test_pct_calculation(self, registry):
        registry.register("wf", "b", "n")
        registry.update_progress("wf", {"succeeded": 8, "running": 2})
        state = registry.get_by_id("wf")
        assert state["done"] == 8
        assert state["total"] == 10
        assert state["pct"] == 80


# ---------------------------------------------------------------------------
# TowerRouter — GET
# ---------------------------------------------------------------------------

class TestTowerRouterGet:
    def test_user_info(self, router):
        status, body = router.handle_get("/user-info")
        assert status == 200
        assert "user" in body

    def test_user_info_with_query_string(self, router):
        status, body = router.handle_get("/user-info?foo=bar")
        assert status == 200

    def test_unrecognised_path_returns_none(self, router):
        assert router.handle_get("/api/status") is None


# ---------------------------------------------------------------------------
# TowerRouter — POST
# ---------------------------------------------------------------------------

class TestTowerRouterPost:
    def test_trace_create(self, router, registry):
        status, body = router.handle_post("/trace/create", {"runName": "dispatcher_bat1"})
        assert status == 200
        assert body["workflowId"] == "dispatcher_bat1"
        assert registry.is_registered("dispatcher_bat1")

    def test_trace_create_no_prefix(self, router, registry):
        status, body = router.handle_post("/trace/create", {"runName": "mypipe_run"})
        assert status == 200
        # With default extractor (no matching prefix), batch_id == run_name
        assert registry.is_registered(body["workflowId"])

    def test_trace_create_empty_run_name_assigns_uuid(self, router, registry):
        status, body = router.handle_post("/trace/create", {})
        assert status == 200
        assert body["workflowId"]

    def test_unrecognised_path_returns_none(self, router):
        assert router.handle_post("/other", {}) is None


# ---------------------------------------------------------------------------
# TowerRouter — PUT
# ---------------------------------------------------------------------------

class TestTowerRouterPut:
    def _register(self, registry, batch_id="b"):
        wid = workflow_id_for_batch(batch_id)
        registry.register(wid, batch_id, f"dispatcher_{batch_id}")
        return wid

    def test_progress(self, router, registry):
        wid = self._register(registry)
        status, body = router.handle_put(
            f"/trace/{wid}/progress",
            {"progress": {"succeeded": 3, "running": 1}, "tasks": []},
        )
        assert status == 200
        state = registry.get_by_id(wid)
        assert state["task_counts"]["succeeded"] == 3

    def test_heartbeat(self, router, registry):
        wid = self._register(registry)
        status, body = router.handle_put(
            f"/trace/{wid}/heartbeat",
            {"progress": {"succeeded": 7}},
        )
        assert status == 200

    def test_complete(self, router, registry):
        wid = self._register(registry)
        status, body = router.handle_put(
            f"/trace/{wid}/complete",
            {"progress": {"succeeded": 10}},
        )
        assert status == 200
        assert registry.get_by_id(wid)["complete"] is True

    def test_begin_auto_registers(self, router, registry):
        wid = "dispatcher_newbatch"
        status, body = router.handle_put(
            f"/trace/{wid}/begin",
            {"workflow": {"runName": "dispatcher_newbatch"}},
        )
        assert status == 200
        assert registry.is_registered(wid)

    def test_progress_auto_registers_if_missing(self, router, registry):
        wid = "dispatcher_missing"
        status, body = router.handle_put(
            f"/trace/{wid}/progress",
            {"progress": {"succeeded": 1}, "workflow": {"runName": "dispatcher_missing"}},
        )
        assert status == 200
        assert registry.is_registered(wid)

    def test_unrecognised_path_returns_none(self, router):
        assert router.handle_put("/api/other", {}) is None

    def test_unrecognised_action_returns_none(self, router, registry):
        wid = self._register(registry)
        result = router.handle_put(f"/trace/{wid}/unknown", {})
        assert result is None


# ---------------------------------------------------------------------------
# Custom run_name extractor
# ---------------------------------------------------------------------------

class TestCustomExtractor:
    def test_custom_prefix(self):
        reg = WorkflowRegistry()
        router = TowerRouter(
            registry=reg,
            run_name_to_batch_id=lambda n: n.removeprefix("mypipe_"),
        )
        status, body = router.handle_post("/trace/create", {"runName": "mypipe_run42"})
        assert status == 200
        # batch_id should be "run42", canonical workflow_id = "dispatcher_run42"
        assert reg.get_by_batch("run42") is not None


# ---------------------------------------------------------------------------
# Standalone response factories
# ---------------------------------------------------------------------------

class TestResponseFactories:
    def test_user_info_has_user(self):
        r = user_info_response()
        assert "user" in r
        assert r["user"]["trusted"] is True

    def test_trace_create_has_workflow_id(self):
        r = trace_create_response("wf-abc")
        assert r["workflowId"] == "wf-abc"
        assert "watchUrl" in r


# ---------------------------------------------------------------------------
# Module-level singleton convenience API (mirrors old tower_shim interface)
# ---------------------------------------------------------------------------

class TestModuleLevelAPI:
    def test_round_trip(self):
        """Smoke-test the module-level singleton functions."""
        wid = "test_singleton_wid"
        batch = "test_singleton_batch"
        nt.register_workflow(wid, batch, f"dispatcher_{batch}")
        assert nt.is_registered(wid)
        nt.update_progress(wid, {"succeeded": 2, "running": 1})
        progress = nt.get_progress(batch)
        assert progress is not None
        assert progress["task_counts"]["succeeded"] == 2
        nt.mark_complete(wid)
        progress = nt.get_progress(batch)
        assert progress["complete"] is True


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

class TestTowerProcessToSlurmName:
    def test_single_colon(self):
        assert tower_process_to_slurm_name("MUSSEL:TESSELLATE") == "MUSSEL_TESSELLATE"

    def test_multiple_colons(self):
        assert tower_process_to_slurm_name(
            "MUSSEL:EXTRACT_FEATURES:TESSELLATE_FEATURIZE_BATCH"
        ) == "MUSSEL_EXTRACT_FEATURES_TESSELLATE_FEATURIZE_BATCH"

    def test_no_colon(self):
        assert tower_process_to_slurm_name("TESSELLATE") == "TESSELLATE"

    def test_trailing_colon_stripped(self):
        assert tower_process_to_slurm_name("FOO:BAR:") == "FOO_BAR"


# ---------------------------------------------------------------------------
# workflow_id_for_batch
# ---------------------------------------------------------------------------

def test_workflow_id_for_batch():
    assert workflow_id_for_batch("abc123") == "dispatcher_abc123"

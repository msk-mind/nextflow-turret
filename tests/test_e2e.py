"""End-to-end scenario tests.

Scenario-level tests that simulate complete user workflows and realistic
Nextflow ``-with-tower`` trace sequences from start to finish.

All interactions go through the FastAPI TestClient — real HTTP
request/response cycle, real SQLite, real subprocess spawning — with no
mocking.  These tests verify the system behaves correctly as an integrated
whole across all its layers.
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from nextflow_turret.server.app import create_app
from nextflow_turret.state import workflow_id_for_batch
from conftest import nf_trace_lifecycle


# ---------------------------------------------------------------------------
# Complete Nextflow trace lifecycle
# ---------------------------------------------------------------------------

class TestNextflowTraceLifecycle:
    """Simulate exactly what ``nextflow run -with-tower`` does, step by step."""

    def test_full_trace_sequence_five_steps(self, mem_client):
        """auth → create → begin → progress ×2 → heartbeat → complete."""
        c = mem_client
        batch_id = "nf_full"
        run_name = f"dispatcher_{batch_id}"
        wid      = workflow_id_for_batch(batch_id)

        # Step 1: auth check
        r = c.get("/user-info")
        assert r.status_code == 200
        assert r.json()["user"]["trusted"] is True

        # Step 2: NF registers itself
        r = c.post("/trace/create", json={"runName": run_name})
        assert r.status_code == 200
        assert r.json()["workflowId"] == wid
        assert c.get(f"/api/runs/{wid}").json()["complete"] is False

        # Step 3: NF signals it is running
        r = c.put(f"/trace/{wid}/begin", json={"workflow": {"runName": run_name}})
        assert r.status_code == 200

        # Step 4a: First progress
        c.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 5, "running": 5, "pending": 10},
        })
        state = c.get(f"/api/runs/{wid}").json()
        assert state["task_counts"]["succeeded"] == 5
        assert state["pct"] == 25  # 5/20

        # Step 4b: Heartbeat (same payload structure as progress)
        r = c.put(f"/trace/{wid}/heartbeat", json={
            "progress": {"succeeded": 12, "running": 3, "pending": 5},
        })
        assert r.status_code == 200
        assert c.get(f"/api/runs/{wid}").json()["task_counts"]["succeeded"] == 12

        # Step 4c: Final progress before completion
        c.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 18, "running": 2, "pending": 0},
        })

        # Step 5: Complete
        r = c.put(f"/trace/{wid}/complete", json={
            "progress": {"succeeded": 20, "running": 0, "pending": 0, "failed": 0},
        })
        assert r.status_code == 200

        final = c.get(f"/api/runs/{wid}").json()
        assert final["complete"] is True
        assert final["pct"] == 100
        assert final["task_counts"]["succeeded"] == 20

    def test_trace_api_state_at_each_step(self, mem_client):
        """API reflects correct state after each individual trace event."""
        c = mem_client
        wid = workflow_id_for_batch("stepcheck")
        c.post("/trace/create", json={"runName": "dispatcher_stepcheck"})

        # After create: zero progress, not complete
        s = c.get(f"/api/runs/{wid}").json()
        assert s["pct"] == 0 and not s["complete"]

        c.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 3, "running": 7, "pending": 0},
        })
        s = c.get(f"/api/runs/{wid}").json()
        assert s["task_counts"]["succeeded"] == 3
        assert not s["complete"]

        c.put(f"/trace/{wid}/complete", json={"progress": {"succeeded": 10}})
        s = c.get(f"/api/runs/{wid}").json()
        assert s["complete"] is True

    def test_trace_begin_auto_registers_on_missing_create(self, mem_client):
        """If /trace/create was never called, /begin auto-registers the workflow."""
        c = mem_client
        wid = "dispatcher_autobegin"
        r = c.put(f"/trace/{wid}/begin", json={"workflow": {"runName": "dispatcher_autobegin"}})
        assert r.status_code == 200
        assert c.get(f"/api/runs/{wid}").status_code == 200

    def test_trace_progress_auto_registers_on_missing_create(self, mem_client):
        """If /trace/create was never called, /progress auto-registers."""
        c = mem_client
        wid = "dispatcher_autoprogs"
        r = c.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 1},
            "workflow": {"runName": "dispatcher_autoprogs"},
        })
        assert r.status_code == 200
        assert c.get(f"/api/runs/{wid}").json()["task_counts"]["succeeded"] == 1

    def test_cached_tasks_count_towards_done_pct(self, mem_client):
        """Cached tasks contribute to 'done' count and percentage."""
        c = mem_client
        wid = workflow_id_for_batch("cached_pct")
        c.post("/trace/create", json={"runName": "dispatcher_cached_pct"})
        c.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 3, "cached": 7, "running": 0},
        })
        s = c.get(f"/api/runs/{wid}").json()
        assert s["done"] == 10
        assert s["pct"] == 100

    def test_trace_with_per_process_breakdown(self, mem_client):
        """Per-process data from progress is returned in API and rendered in UI."""
        c = mem_client
        wid = workflow_id_for_batch("procs")
        c.post("/trace/create", json={"runName": "dispatcher_procs"})
        c.put(f"/trace/{wid}/progress", json={
            "progress": {
                "succeeded": 10,
                "running":   2,
                "processes": [
                    {"process": "ALIGN",  "succeeded": 7,  "running": 1, "failed": 0},
                    {"process": "REPORT", "succeeded": 3,  "running": 1, "failed": 0},
                ],
            },
        })

        state = c.get(f"/api/runs/{wid}").json()
        assert len(state["processes"]) == 2
        assert state["processes"][0]["process"] == "ALIGN"

        # Verify process names appear in the detail UI
        html = c.get(f"/runs/{wid}").content.decode()
        assert "ALIGN" in html
        assert "REPORT" in html

    def test_resource_metrics_in_api(self, mem_client):
        """peakCpus / peakMemory from progress payload surface in API state."""
        c = mem_client
        wid = workflow_id_for_batch("resources")
        c.post("/trace/create", json={"runName": "dispatcher_resources"})
        c.put(f"/trace/{wid}/progress", json={
            "progress": {
                "succeeded":   5,
                "peakCpus":    32,
                "peakMemory":  68719476736,   # 64 GiB
                "peakRunning": 16,
            },
        })
        r = c.get(f"/api/runs/{wid}").json()
        assert r["resources"]["peakCpus"] == 32
        assert r["resources"]["peakMemory"] == 68719476736


# ---------------------------------------------------------------------------
# Failed tasks tracking
# ---------------------------------------------------------------------------

class TestFailedTasksScenario:
    """Workflows with failed tasks are correctly tracked in the API and UI."""

    def test_failed_tasks_appear_in_api_and_ui(self, mem_client):
        c = mem_client
        wid = workflow_id_for_batch("fail_scenario")
        c.post("/trace/create", json={"runName": "dispatcher_fail_scenario"})
        c.put(f"/trace/{wid}/progress", json={
            "progress": {"succeeded": 8, "failed": 3, "running": 1},
            "tasks": [
                {"taskId": 1, "status": "FAILED", "process": "ALIGN",         "name": "ALIGN (s1)",         "exit": 137},
                {"taskId": 2, "status": "FAILED", "process": "ALIGN",         "name": "ALIGN (s2)",         "exit": 1},
                {"taskId": 3, "status": "FAILED", "process": "CALL_VARIANTS", "name": "CALL_VARIANTS (s1)", "exit": 1},
            ],
        })

        state = c.get(f"/api/runs/{wid}").json()
        assert state["task_counts"]["failed"] == 3
        assert len(state["failures"]) == 3
        procs = {f["process"] for f in state["failures"]}
        assert procs == {"ALIGN", "CALL_VARIANTS"}

        html = c.get(f"/runs/{wid}").content.decode()
        assert "Failed Tasks" in html
        assert "ALIGN" in html
        assert "CALL_VARIANTS" in html

    def test_failed_tasks_deduplicated_by_task_id(self, mem_client):
        """The same taskId reported FAILED twice should only appear once."""
        c = mem_client
        wid = workflow_id_for_batch("dedup")
        c.post("/trace/create", json={"runName": "dispatcher_dedup"})
        task = {"taskId": 99, "status": "FAILED", "process": "PROC", "name": "PROC (x)"}
        c.put(f"/trace/{wid}/progress", json={"progress": {}, "tasks": [task]})
        c.put(f"/trace/{wid}/progress", json={"progress": {}, "tasks": [task]})  # same ID again

        assert len(c.get(f"/api/runs/{wid}").json()["failures"]) == 1

    def test_failures_accumulate_across_progress_events(self, mem_client):
        """Failures from separate progress events are merged."""
        c = mem_client
        wid = workflow_id_for_batch("fail_accum")
        c.post("/trace/create", json={"runName": "dispatcher_fail_accum"})
        c.put(f"/trace/{wid}/progress", json={
            "progress": {},
            "tasks": [{"taskId": 1, "status": "FAILED", "process": "P"}],
        })
        c.put(f"/trace/{wid}/progress", json={
            "progress": {},
            "tasks": [{"taskId": 2, "status": "FAILED", "process": "P"}],
        })
        assert len(c.get(f"/api/runs/{wid}").json()["failures"]) == 2

    def test_failures_capped_at_50(self, mem_client):
        """No more than 50 failures are retained."""
        c = mem_client
        wid = workflow_id_for_batch("fail_cap")
        c.post("/trace/create", json={"runName": "dispatcher_fail_cap"})
        # Send 70 unique failures in batches of 10
        for batch in range(7):
            tasks = [
                {"taskId": batch * 10 + i, "status": "FAILED", "process": "P", "name": f"t{i}"}
                for i in range(10)
            ]
            c.put(f"/trace/{wid}/progress", json={"progress": {}, "tasks": tasks})

        assert len(c.get(f"/api/runs/{wid}").json()["failures"]) == 50


# ---------------------------------------------------------------------------
# Multi-run dashboard
# ---------------------------------------------------------------------------

class TestMultiRunDashboard:
    """Dashboard shows correct stats when multiple runs are present."""

    def test_all_runs_visible_in_dashboard(self, mem_client):
        c = mem_client
        for i in range(4):
            c.post("/trace/create", json={"runName": f"dispatcher_dash{i}"})

        html = c.get("/").content.decode()
        for i in range(4):
            assert f"dispatcher_dash{i}" in html

    def test_dashboard_empty_shows_placeholder(self, mem_client):
        assert b"No pipelines yet" in mem_client.get("/").content

    def test_complete_badge_visible_for_finished_run(self, mem_client):
        wid = nf_trace_lifecycle(mem_client, "badge_run", n_tasks=3)
        html = mem_client.get("/").content.decode()
        assert "Complete" in html

    def test_runs_sorted_newest_first_in_api(self, mem_client):
        c = mem_client
        for name in ("alpha", "beta", "gamma"):
            c.post("/trace/create", json={"runName": f"dispatcher_{name}"})

        runs = c.get("/api/runs").json()["runs"]
        assert runs[0]["batch_id"] == "gamma"
        assert runs[-1]["batch_id"] == "alpha"

    def test_mixed_status_runs(self, mem_client):
        """Dashboard handles running, complete, and failed-tasks runs simultaneously."""
        c = mem_client

        # Complete run
        nf_trace_lifecycle(c, "mix_done", n_tasks=4)

        # In-progress run
        wid_running = workflow_id_for_batch("mix_running")
        c.post("/trace/create", json={"runName": "dispatcher_mix_running"})
        c.put(f"/trace/{wid_running}/progress", json={
            "progress": {"succeeded": 2, "running": 3, "pending": 5},
        })

        # Run with failures
        wid_fail = workflow_id_for_batch("mix_fail")
        c.post("/trace/create", json={"runName": "dispatcher_mix_fail"})
        c.put(f"/trace/{wid_fail}/progress", json={
            "progress": {"succeeded": 5, "failed": 2},
            "tasks": [
                {"taskId": i, "status": "FAILED", "process": "PROC", "name": f"t{i}"}
                for i in range(2)
            ],
        })

        r = c.get("/api/runs")
        runs = r.json()["runs"]
        assert len(runs) == 3

        complete_runs = [r for r in runs if r["complete"]]
        running_runs  = [r for r in runs if not r["complete"] and not r["stalled"]]
        assert len(complete_runs) == 1
        assert len(running_runs) == 2

        # UI renders all three
        html = c.get("/").content.decode()
        assert "dispatcher_mix_done" in html
        assert "dispatcher_mix_running" in html
        assert "dispatcher_mix_fail" in html


# ---------------------------------------------------------------------------
# Launch UI flow — complete user journey
# ---------------------------------------------------------------------------

class TestLaunchUIFlow:
    """Complete launch user journey: form → redirect → detail → list → API."""

    def test_full_launch_form_journey(self, mem_client):
        c = mem_client

        # Step 1: Render the form
        r = c.get("/launch")
        assert r.status_code == 200
        assert b"Pipeline" in r.content
        assert b"Revision" in r.content

        # Step 2: Submit the form
        r = c.post("/launch", data={
            "pipeline": "https://github.com/nf-core/rnaseq",
            "revision": "3.14.0",
            "profile":  "docker",
            "params":   '{"input": "samplesheet.csv", "genome": "GRCh38"}',
        }, follow_redirects=False)
        assert r.status_code == 303
        detail_url = r.headers["location"]
        assert detail_url.startswith("/launches/")
        launch_id = detail_url.split("/")[-1]

        # Step 3: Detail page shows submitted data
        html = c.get(detail_url).content.decode()
        assert "nf-core/rnaseq" in html
        assert "3.14.0" in html
        assert "docker" in html

        # Step 4: Launches list page shows this launch
        assert b"nf-core/rnaseq" in c.get("/launches").content

        # Step 5: REST API returns same data
        api = c.get(f"/api/launches/{launch_id}").json()
        assert api["pipeline"] == "https://github.com/nf-core/rnaseq"
        assert api["revision"] == "3.14.0"
        assert api["profile"]  == "docker"
        assert api["params"]["input"] == "samplesheet.csv"
        assert api["params"]["genome"] == "GRCh38"

    def test_json_params_parsed_correctly_via_form(self, mem_client):
        c = mem_client
        r = c.post("/launch", data={
            "pipeline": "org/flow",
            "params":   '{"n_jobs": 8, "threshold": 0.05}',
        }, follow_redirects=False)
        launch_id = r.headers["location"].split("/")[-1]

        api = c.get(f"/api/launches/{launch_id}").json()
        assert api["params"]["n_jobs"] == 8
        assert api["params"]["threshold"] == 0.05

    def test_empty_optional_fields_handled_gracefully(self, mem_client):
        """Form with only pipeline filled in should succeed."""
        r = mem_client.post("/launch", data={"pipeline": "org/minimal"}, follow_redirects=False)
        assert r.status_code == 303
        api = mem_client.get(f"/api/launches/{r.headers['location'].split('/')[-1]}").json()
        assert api["revision"] is None
        assert api["profile"]  is None
        assert api["params"]   == {}

    def test_invalid_json_params_re_renders_form_with_error(self, mem_client):
        r = mem_client.post("/launch", data={"pipeline": "org/p", "params": "not-json"})
        assert r.status_code == 422
        html = r.content.decode()
        assert "Invalid params JSON" in html
        assert "org/p" in html  # form values preserved

    def test_params_must_be_json_object_not_array(self, mem_client):
        r = mem_client.post("/launch", data={"pipeline": "org/p", "params": "[1,2,3]"})
        assert r.status_code == 422
        assert b"Invalid params JSON" in r.content

    def test_multiple_launches_all_listed(self, mem_client):
        c = mem_client
        pipelines = ["org/pipe1", "org/pipe2", "org/pipe3"]
        for p in pipelines:
            c.post("/launch", data={"pipeline": p}, follow_redirects=False)

        html = c.get("/launches").content.decode()
        for p in pipelines:
            assert p in html

    def test_cancel_via_ui_redirects_to_detail(self, mem_client):
        c = mem_client
        r = c.post("/launch", data={"pipeline": "org/cancel-me"}, follow_redirects=False)
        launch_id = r.headers["location"].split("/")[-1]

        r = c.post(f"/launches/{launch_id}/cancel", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == f"/launches/{launch_id}"

    def test_api_launch_appears_in_ui_list_and_detail(self, mem_client):
        """A launch submitted via REST API is visible in the HTML UI."""
        c = mem_client
        r = c.post("/api/launches", json={"pipeline": "org/api-submitted"})
        assert r.status_code == 201
        launch_id = r.json()["launch_id"]

        assert b"org/api-submitted" in c.get("/launches").content
        assert b"org/api-submitted" in c.get(f"/launches/{launch_id}").content

    def test_launch_detail_log_section_rendered(self, mem_client):
        c = mem_client
        r = c.post("/launch", data={"pipeline": "org/logtest"}, follow_redirects=False)
        html = c.get(r.headers["location"]).content.decode()
        assert "Log" in html

    def test_launches_empty_state_message(self, mem_client):
        assert b"No pipelines yet" in mem_client.get("/launches").content


# ---------------------------------------------------------------------------
# Log endpoint
# ---------------------------------------------------------------------------

class TestLogEndpoint:
    """Launch log is accessible via the REST API and the HTML detail page."""

    def test_log_endpoint_returns_plain_text(self, mem_client):
        c = mem_client
        r = c.post("/api/launches", json={"pipeline": "org/logme"})
        launch_id = r.json()["launch_id"]

        r = c.get(f"/api/launches/{launch_id}/log")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/plain")

    def test_log_endpoint_tail_parameter_accepted(self, mem_client):
        c = mem_client
        r = c.post("/api/launches", json={"pipeline": "org/tail"})
        launch_id = r.json()["launch_id"]

        r = c.get(f"/api/launches/{launch_id}/log?tail=5")
        assert r.status_code == 200

    def test_log_tail_limits_returned_lines(self, mem_client, tmp_path, log_dir):
        """Writing a known log file and requesting tail=3 returns ≤3 lines."""
        c = mem_client
        r = c.post("/api/launches", json={"pipeline": "org/tail-lines"})
        launch_id = r.json()["launch_id"]
        log_path  = Path(r.json()["log_path"])

        # Wait for subprocess to exit and release the file, then overwrite
        time.sleep(0.5)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("\n".join(f"line {i}" for i in range(10)))

        r = c.get(f"/api/launches/{launch_id}/log?tail=3")
        assert r.status_code == 200
        returned_lines = [ln for ln in r.text.strip().splitlines() if ln]
        assert len(returned_lines) <= 3
        assert "line 9" in r.text  # tail returns the last lines

    def test_log_of_nonexistent_launch_returns_404(self, mem_client):
        r = mem_client.get("/api/launches/nonexistent-id/log")
        assert r.status_code == 404

    def test_log_section_present_on_detail_page(self, mem_client):
        c = mem_client
        r = c.post("/api/launches", json={"pipeline": "org/ui-log"})
        launch_id = r.json()["launch_id"]
        assert b"Log" in c.get(f"/launches/{launch_id}").content


# ---------------------------------------------------------------------------
# Error and edge-case scenarios
# ---------------------------------------------------------------------------

class TestErrorScenarios:
    """Error paths return the correct HTTP status codes end-to-end."""

    def test_get_unknown_run_api_404(self, mem_client):
        assert mem_client.get("/api/runs/no_such_run").status_code == 404

    def test_get_unknown_run_ui_404(self, mem_client):
        assert mem_client.get("/runs/no_such_run").status_code == 404

    def test_get_unknown_launch_api_404(self, mem_client):
        assert mem_client.get("/api/launches/00000000-0000-0000-0000-000000000000").status_code == 404

    def test_get_unknown_launch_ui_404(self, mem_client):
        assert mem_client.get("/launches/00000000-0000-0000-0000-000000000000").status_code == 404

    def test_cancel_finished_launch_returns_409(self, mem_client):
        c = mem_client
        r = c.post("/api/launches", json={"pipeline": "org/short"})
        launch_id = r.json()["launch_id"]
        time.sleep(0.8)  # wait for nextflow to fail (not installed in test env)
        status = c.get(f"/api/launches/{launch_id}").json()["status"]
        if status in ("failed", "succeeded", "cancelled"):
            assert c.delete(f"/api/launches/{launch_id}").status_code == 409

    def test_unknown_tower_action_returns_404(self, mem_client):
        r = mem_client.put("/trace/dispatcher_wf1/badaction", json={})
        assert r.status_code == 404

    def test_api_launch_missing_pipeline_returns_422(self, mem_client):
        assert mem_client.post("/api/launches", json={}).status_code == 422

    def test_form_launch_missing_pipeline_returns_422(self, mem_client):
        assert mem_client.post("/launch", data={}).status_code == 422

    def test_cancel_nonexistent_launch_ui_404(self, mem_client):
        r = mem_client.post("/launches/no-such-id/cancel")
        assert r.status_code == 404

    def test_api_docs_accessible(self, mem_client):
        """OpenAPI docs endpoint is reachable (smoke test)."""
        assert mem_client.get("/docs").status_code == 200

    def test_openapi_json_accessible(self, mem_client):
        r = mem_client.get("/openapi.json")
        assert r.status_code == 200
        schema = r.json()
        assert "paths" in schema
        assert "/api/runs" in schema["paths"]
        assert "/api/launches" in schema["paths"]

"""FastAPI application for Nextflow Turret.

Two groups of endpoints are served:

Tower trace endpoints (called by Nextflow ``-with-tower``)
----------------------------------------------------------
These mirror the Seqera Platform / Nextflow Tower REST API that Nextflow
itself calls during a pipeline run.

=====  ========================  ========================================
Verb   Path                      Purpose
=====  ========================  ========================================
GET    /user-info                NF auth check on startup
POST   /trace/create             Workflow start → returns workflowId
PUT    /trace/{id}/begin         Workflow running (runName available)
PUT    /trace/{id}/progress      Periodic task counts + per-task list
PUT    /trace/{id}/heartbeat     Keepalive (same payload as progress)
PUT    /trace/{id}/complete      Workflow finished
=====  ========================  ========================================

REST API (consumed by the UI / external clients)
-------------------------------------------------
GET     /api/runs                    List all workflow runs
GET     /api/runs/{workflow_id}      Single run detail
GET     /api/launches                List all pipeline launches
POST    /api/launches                Submit a new pipeline launch
GET     /api/launches/{id}           Single launch detail
GET     /api/launches/{id}/log       Launch log (tail= query param)
DELETE  /api/launches/{id}           Cancel a running launch

Web UI
------
GET     /                            Runs dashboard
GET     /runs/{workflow_id}          Run detail page
GET     /launches                    Launches list
GET     /launch                      Launch form
POST    /launch                      Submit launch (HTML form)
GET     /launches/{id}               Launch detail page
POST    /launches/{id}/cancel        Cancel a launch (HTML form)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ..handlers import TowerRouter
from ..db.store import RunStore
from ..launcher.launcher import Launcher
from ..schema import fetch_pipeline_schema
from .registry import PersistentWorkflowRegistry

_TEMPLATES_DIR = Path(__file__).parent / "templates"


def _make_templates() -> Jinja2Templates:
    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    def fmt_time(ts: Optional[float]) -> str:
        if ts is None:
            return "—"
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    templates.env.filters["fmt_time"] = fmt_time
    return templates


async def _body(request: Request) -> dict:
    """Parse JSON body; return empty dict on missing/invalid body."""
    try:
        data = await request.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


class LaunchRequest(BaseModel):
    pipeline:   str
    revision:   Optional[str] = None
    params:     dict          = {}
    profile:    Optional[str] = None
    work_dir:   Optional[str] = None
    run_name:   Optional[str] = None


def _enrich_run(state: dict) -> dict:
    """Add derived fields used by templates."""
    tc = state.get("task_counts") or {}
    succeeded = tc.get("succeeded", 0)
    cached    = tc.get("cached", 0)
    failed    = tc.get("failed", 0)
    running   = tc.get("running", 0)
    pending   = tc.get("pending", 0)
    submitted = tc.get("submitted", 0)

    done  = succeeded + cached
    total = done + failed + running + pending + submitted
    pct   = round(100 * done / total) if total else 0

    state = dict(state)
    state["done"]    = done
    state["total"]   = total
    state["pct"]     = pct
    state["stalled"] = state.get("stalled", False)
    return state


def create_app(
    db_path:          str | Path    = "turret.db",
    tower_url:        str           = "http://localhost:8000",
    log_dir:          str | Path    = "turret-logs",
    nextflow_bin:     str           = "nextflow",
    default_work_dir: Optional[str] = None,
    default_profile:  Optional[str] = None,
) -> FastAPI:
    """Create and return the Nextflow Turret FastAPI application.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Pass ``":memory:"`` for an
        ephemeral in-process database (useful for testing).
    tower_url:
        URL of this server, injected as ``-with-tower`` for launched pipelines.
    log_dir:
        Directory for per-launch log files.
    nextflow_bin:
        Path to the ``nextflow`` executable.
    default_work_dir:
        Default ``-work-dir`` for every launched pipeline (unless overridden
        per-launch).
    default_profile:
        Default ``-profile`` for every launched pipeline (unless overridden
        per-launch).
    """
    store     = RunStore(db_path)
    registry  = PersistentWorkflowRegistry(store)
    router    = TowerRouter(registry=registry)
    launcher  = Launcher(
        tower_url        = tower_url,
        log_dir          = log_dir,
        nextflow_bin     = nextflow_bin,
        default_work_dir = default_work_dir,
        default_profile  = default_profile,
    )
    templates = _make_templates()

    app = FastAPI(
        title="Nextflow Turret",
        description="Self-hosted Nextflow Tower / Seqera Platform replacement",
        version="0.1.0",
    )

    # ------------------------------------------------------------------ #
    # Tower trace endpoints                                                #
    # ------------------------------------------------------------------ #

    @app.get("/user-info", tags=["tower"])
    async def user_info(request: Request):
        status, body = router.handle_get(request.url.path)
        return JSONResponse(body, status_code=status)

    @app.post("/trace/create", tags=["tower"])
    async def trace_create(request: Request):
        result = router.handle_post("/trace/create", await _body(request))
        if result is None:
            raise HTTPException(404)
        status, body = result
        return JSONResponse(body, status_code=status)

    @app.put("/trace/{workflow_id}/{action}", tags=["tower"])
    async def trace_put(workflow_id: str, action: str, request: Request):
        path = f"/trace/{workflow_id}/{action}"
        result = router.handle_put(path, await _body(request))
        if result is None:
            raise HTTPException(404, detail=f"Unknown Tower action: {action!r}")
        status, body = result
        return JSONResponse(body, status_code=status)

    # ------------------------------------------------------------------ #
    # REST API — runs                                                      #
    # ------------------------------------------------------------------ #

    @app.get("/api/runs", tags=["api"])
    async def list_runs():
        """Return all workflow runs, most-recent first."""
        runs = registry.get_all()
        runs.sort(key=lambda r: r["started_at"], reverse=True)
        return {"runs": runs, "total": len(runs)}

    @app.get("/api/runs/{workflow_id}", tags=["api"])
    async def get_run(workflow_id: str):
        """Return the full state dict for a single workflow run."""
        state = registry.get_by_id(workflow_id)
        if state is None:
            raise HTTPException(404, detail=f"Workflow '{workflow_id}' not found")
        return state

    # ------------------------------------------------------------------ #
    # REST API — launches                                                  #
    # ------------------------------------------------------------------ #

    @app.get("/api/launches", tags=["api"])
    async def list_launches():
        """Return all pipeline launches, most-recent first."""
        records = launcher.list_all()
        launches = [r.as_dict() for r in records]
        launches.sort(key=lambda r: r["submitted_at"], reverse=True)
        return {"launches": launches, "total": len(launches)}

    @app.post("/api/launches", status_code=201, tags=["api"])
    async def submit_launch(req: LaunchRequest):
        """Submit a new pipeline launch."""
        launch_id = launcher.submit(
            pipeline  = req.pipeline,
            revision  = req.revision,
            params    = req.params,
            profile   = req.profile,
            work_dir  = req.work_dir,
            run_name  = req.run_name,
        )
        record = launcher.get(launch_id)
        store.upsert_launch(record.as_dict())
        return record.as_dict()

    @app.get("/api/launches/{launch_id}", tags=["api"])
    async def get_launch(launch_id: str):
        """Return current status of a single launch."""
        record = launcher.get(launch_id)
        if record is None:
            raise HTTPException(404, detail=f"Launch '{launch_id}' not found")
        return record.as_dict()

    @app.get("/api/launches/{launch_id}/log", tags=["api"], response_class=PlainTextResponse)
    async def get_launch_log(
        launch_id: str,
        tail: Optional[int] = Query(default=None, description="Return only the last N lines"),
    ):
        """Return the stdout/stderr log for a launch."""
        record = launcher.get(launch_id)
        if record is None:
            raise HTTPException(404, detail=f"Launch '{launch_id}' not found")
        return PlainTextResponse(launcher.read_log(launch_id, tail=tail))

    @app.delete("/api/launches/{launch_id}", tags=["api"])
    async def cancel_launch(launch_id: str):
        """Cancel a running pipeline launch (sends SIGTERM)."""
        record = launcher.get(launch_id)
        if record is None:
            raise HTTPException(404, detail=f"Launch '{launch_id}' not found")
        sent = launcher.cancel(launch_id)
        if not sent:
            raise HTTPException(409, detail="Launch is not in a cancellable state")
        return {"launch_id": launch_id, "status": "cancelled"}

    @app.get("/api/pipeline/schema", tags=["api"])
    async def get_pipeline_schema(
        pipeline: str           = Query(...,      description="Pipeline identifier (e.g. nf-core/rnaseq)"),
        revision: Optional[str] = Query(default=None, description="Git revision / tag"),
    ):
        """Return parameter specs from the pipeline's nextflow_schema.json."""
        params = fetch_pipeline_schema(pipeline, revision)
        return {
            "params": [p.to_dict() for p in params],
            "count":  len(params),
            "source": "nextflow_schema.json" if params else None,
        }

    # ------------------------------------------------------------------ #
    # Web UI                                                               #
    # ------------------------------------------------------------------ #

    @app.get("/", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_index(request: Request):
        runs = registry.get_all()
        runs.sort(key=lambda r: r["started_at"], reverse=True)
        runs = [_enrich_run(r) for r in runs]
        total_failed = sum(r.get("task_counts", {}).get("failed", 0) for r in runs)
        return templates.TemplateResponse(
            request,
            "runs.html",
            {"runs": runs, "total_failed": total_failed, "active_page": "runs"},
        )

    @app.get("/runs/{workflow_id}", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_run_detail(workflow_id: str, request: Request):
        state = registry.get_by_id(workflow_id)
        if state is None:
            raise HTTPException(404, detail=f"Workflow '{workflow_id}' not found")
        run = _enrich_run(state)
        return templates.TemplateResponse(
            request,
            "run_detail.html",
            {"run": run, "active_page": "runs"},
        )

    @app.get("/launches", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_launches(request: Request):
        records = launcher.list_all()
        launches = [r.as_dict() for r in records]
        launches.sort(key=lambda r: r["submitted_at"], reverse=True)
        return templates.TemplateResponse(
            request,
            "launches.html",
            {"launches": launches, "active_page": "launches"},
        )

    @app.get("/launch", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_launch_form(request: Request):
        return templates.TemplateResponse(
            request,
            "launch_form.html",
            {"form": {}, "error": None, "active_page": "launch"},
        )

    @app.post("/launch", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_launch_submit(
        request: Request,
        pipeline:  str           = Form(...),
        revision:  Optional[str] = Form(default=None),
        profile:   Optional[str] = Form(default=None),
        work_dir:  Optional[str] = Form(default=None),
        run_name:  Optional[str] = Form(default=None),
        params:    Optional[str] = Form(default=None),
    ):
        form_data = {
            "pipeline": pipeline, "revision": revision,
            "profile": profile, "work_dir": work_dir,
            "run_name": run_name, "params": params,
        }
        # Collect individual param__KEY fields from the form
        raw_form   = await request.form()
        import json as _json
        parsed_params: dict = {}

        # Priority 1: individual param__KEY fields (schema-driven form)
        individual = {
            k[7:]: v
            for k, v in raw_form.items()
            if k.startswith("param__") and str(v).strip()
        }
        if individual:
            parsed_params = individual
        elif params and params.strip():
            # Priority 2: legacy JSON textarea fallback
            try:
                parsed_params = _json.loads(params)
                if not isinstance(parsed_params, dict):
                    raise ValueError("params must be a JSON object")
            except Exception as exc:
                return templates.TemplateResponse(
                    request,
                    "launch_form.html",
                    {"form": form_data, "error": f"Invalid params JSON: {exc}", "active_page": "launch"},
                    status_code=422,
                )

        # Sanitise empty strings to None
        revision = revision or None
        profile  = profile  or None
        work_dir = work_dir or None
        run_name = run_name or None

        launch_id = launcher.submit(
            pipeline  = pipeline,
            revision  = revision,
            params    = parsed_params,
            profile   = profile,
            work_dir  = work_dir,
            run_name  = run_name,
        )
        record = launcher.get(launch_id)
        store.upsert_launch(record.as_dict())
        return RedirectResponse(url=f"/launches/{launch_id}", status_code=303)

    @app.get("/launches/{launch_id}", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_launch_detail(launch_id: str, request: Request):
        record = launcher.get(launch_id)
        if record is None:
            raise HTTPException(404, detail=f"Launch '{launch_id}' not found")
        launch = record.as_dict()
        log    = launcher.read_log(launch_id, tail=200)

        # Try to find the associated workflow run
        run = None
        state = registry.get_by_id(f"dispatcher_{launch_id}")
        if state is None:
            # fall back: search by run_name
            for r in registry.get_all():
                if r.get("run_name") == launch["run_name"]:
                    state = r
                    break
        if state:
            run = _enrich_run(state)

        return templates.TemplateResponse(
            request,
            "launch_detail.html",
            {"launch": launch, "run": run, "log": log, "active_page": "launches"},
        )

    @app.post("/launches/{launch_id}/cancel", tags=["ui"], include_in_schema=False)
    async def ui_cancel_launch(launch_id: str):
        record = launcher.get(launch_id)
        if record is None:
            raise HTTPException(404)
        launcher.cancel(launch_id)
        return RedirectResponse(url=f"/launches/{launch_id}", status_code=303)

    return app


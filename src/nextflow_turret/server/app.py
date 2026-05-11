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

import asyncio
import collections
import json
import os
import re
import secrets
import shutil
import stat
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from ..auth import AuthConfig, AuthManager, AuthMiddleware, AuthMode, safe_next_url
from ..handlers import TowerRouter
from ..db.store import RunStore
from ..launcher.launcher import Launcher
from ..schema import fetch_pipeline_schema, fetch_pipeline_profiles, fetch_pipeline_refs, fetch_pipeline_config_text, resolve_pipeline_clone_url
from ..state import _task_counts_from_progress
from .registry import PersistentWorkflowRegistry

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STATIC_DIR    = Path(__file__).parent / "static"

# Param key validation — module-level constants (reused in form handler)
_VALID_PARAM_KEY    = re.compile(r'^[\w][\w\-]*$')
_MAX_PARAMS_JSON_BYTES = 100_000


# ---------------------------------------------------------------------------
# Security headers middleware
# ---------------------------------------------------------------------------

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add defensive HTTP security headers to every response."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self'",
        )
        return response


# ---------------------------------------------------------------------------
# CSRF helpers
# ---------------------------------------------------------------------------

def _csrf_get_or_create(request: Request) -> str:
    """Return (creating if needed) a CSRF token stored in the session."""
    try:
        token = request.session.get("_csrf")
        if not token:
            token = secrets.token_urlsafe(32)
            request.session["_csrf"] = token
        return token
    except AssertionError:
        # SessionMiddleware not installed (auth disabled) — no CSRF needed
        return ""


def _csrf_validate(request: Request, form_token: Optional[str]) -> None:
    """Raise HTTPException(403) if the CSRF token is missing or invalid.

    Only enforced when SessionMiddleware is active (i.e. auth is enabled).
    """
    try:
        expected = request.session.get("_csrf")
    except AssertionError:
        return  # no session middleware → auth disabled → skip CSRF check
    if not expected or not form_token or not secrets.compare_digest(expected, form_token):
        raise HTTPException(403, detail="CSRF check failed")


# ---------------------------------------------------------------------------
# Login rate limiter
# ---------------------------------------------------------------------------

class _LoginRateLimiter:
    """Simple in-memory sliding-window rate limiter for login attempts.

    Keyed by IP address.  After *max_attempts* failures within *window* seconds
    the IP is blocked until the window rolls forward.  Successful logins clear
    the counter for that IP.
    """

    def __init__(self, max_attempts: int = 10, window_seconds: int = 60) -> None:
        self._max    = max_attempts
        self._window = window_seconds
        self._lock   = threading.Lock()
        self._log: dict[str, collections.deque] = {}

    def _client_key(self, request: Request) -> str:
        return request.client.host if request.client else "unknown"

    def check_and_record(self, request: Request) -> bool:
        """Return True if the request is allowed; False if rate-limited."""
        key = self._client_key(request)
        now = time.time()
        with self._lock:
            dq = self._log.setdefault(key, collections.deque())
            # Drop timestamps outside the window
            while dq and dq[0] < now - self._window:
                dq.popleft()
            if len(dq) >= self._max:
                return False
            dq.append(now)
        return True

    def reset(self, request: Request) -> None:
        """Clear the counter for this IP (call on successful login)."""
        key = self._client_key(request)
        with self._lock:
            self._log.pop(key, None)


_login_rate_limiter = _LoginRateLimiter(max_attempts=10, window_seconds=60)


def _make_templates(auth_mgr: "AuthManager") -> Jinja2Templates:
    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    def fmt_time(ts: Optional[float]) -> str:
        if ts is None:
            return "—"
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    templates.env.filters["fmt_time"] = fmt_time

    # Inject current_user and csrf_token into every template context
    original_response = templates.TemplateResponse

    def template_response_with_user(request, name_or_request, context=None, **kwargs):
        # Support both old and new Jinja2/Starlette calling conventions
        if context is None:
            context = {}
        context.setdefault("current_user", auth_mgr.get_user(request))
        context.setdefault("csrf_token", _csrf_get_or_create(request))
        return original_response(request, name_or_request, context, **kwargs)

    templates.TemplateResponse = template_response_with_user  # type: ignore[method-assign]
    return templates


_MAX_BODY_BYTES = 100_000  # 100 KB limit for API JSON bodies


async def _body(request: Request) -> dict:
    """Parse JSON body; return empty dict on missing/invalid body.

    Enforces a 100 KB size limit to prevent memory exhaustion from large payloads.
    """
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > _MAX_BODY_BYTES:
        return {}
    try:
        raw = await request.body()
        if len(raw) > _MAX_BODY_BYTES:
            return {}
        data = json.loads(raw)
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


def _compute_display_status(run: dict) -> str:
    """Return a display status string for a Tower run dict."""
    if run["complete"]:
        return "complete"
    if run.get("stalled"):
        return "stalled"
    return "running"


def _enrich_run(state: dict) -> dict:
    """Add derived fields used by templates."""
    tc        = _task_counts_from_progress(state.get("task_counts") or {})
    succeeded = tc["succeeded"]
    cached    = tc["cached"]
    failed    = tc["failed"]
    running   = tc["running"]
    pending   = tc["pending"]
    submitted = tc["submitted"]

    done  = succeeded + cached
    total = done + failed + running + pending + submitted
    pct   = round(100 * done / total) if total else 0

    state = dict(state)
    state["done"]    = done
    state["total"]   = total
    state["pct"]     = pct
    state["stalled"] = state.get("stalled", False)
    return state


def _build_pipeline_rows(launcher: "Launcher", registry: "PersistentWorkflowRegistry") -> list[dict]:
    """Merge launch records and Tower trace runs into a unified dashboard list.

    Each row has a ``display_status`` field and a ``detail_url`` for linking.
    Launches without a matched run (e.g. still pending) and Tower runs that
    were not submitted via Turret (external) are both included.
    """
    # Index enriched runs by run_name for O(1) lookup
    runs_by_name: dict[str, dict] = {}
    for run in registry.get_all():
        runs_by_name[run["run_name"]] = _enrich_run(run)

    rows: list[dict] = []
    seen_run_names: set[str] = set()

    for record in launcher.list_all():
        launch = record.as_dict()
        run    = runs_by_name.get(launch["run_name"])
        seen_run_names.add(launch["run_name"])

        row: dict = {
            # launch identity
            "launch_id":     launch["launch_id"],
            "pipeline":      launch["pipeline"],
            "revision":      launch.get("revision"),
            "profile":       launch.get("profile"),
            "launch_status": launch["status"],
            "submitted_at":  launch["submitted_at"],
            "has_launch":    True,
            "has_run":       run is not None,
            "detail_url":    f"/launches/{launch['launch_id']}",
            # run defaults (overridden below when run exists)
            "run_name":    launch["run_name"],
            "workflow_id": None,
            "batch_id":    None,
            "complete":    launch["status"] in ("succeeded", "failed", "cancelled"),
            "stalled":     False,
            "pct":         100 if launch["status"] == "succeeded" else 0,
            "done":        0,
            "total":       0,
            "task_counts": {},
            "started_at":  launch.get("started_at"),
            "failures":    [],
        }

        if run:
            row.update({
                "workflow_id": run["workflow_id"],
                "batch_id":    run.get("batch_id"),
                "complete":    run["complete"],
                "stalled":     run.get("stalled", False),
                "pct":         run["pct"],
                "done":        run["done"],
                "total":       run["total"],
                "task_counts": run["task_counts"],
                "started_at":  run.get("started_at") or launch.get("started_at"),
                "failures":    run.get("failures", []),
            })

        # Unified status badge
        if run:
            row["display_status"] = _compute_display_status(run)
        else:
            row["display_status"] = launch["status"]  # pending / running / failed / cancelled

        rows.append(row)

    # External Tower runs (not submitted via Turret)
    for run_name, run in runs_by_name.items():
        if run_name in seen_run_names:
            continue
        rows.append({
            "launch_id":     None,
            "pipeline":      run_name,
            "revision":      None,
            "profile":       None,
            "launch_status": None,
            "submitted_at":  run.get("started_at"),
            "has_launch":    False,
            "has_run":       True,
            "detail_url":    f"/runs/{run['workflow_id']}",
            "run_name":      run_name,
            "workflow_id":   run["workflow_id"],
            "batch_id":      run.get("batch_id"),
            "complete":      run["complete"],
            "stalled":       run.get("stalled", False),
            "pct":           run["pct"],
            "done":          run["done"],
            "total":         run["total"],
            "task_counts":   run["task_counts"],
            "started_at":    run.get("started_at"),
            "failures":      run.get("failures", []),
            "display_status": _compute_display_status(run),
        })

    rows.sort(key=lambda r: r.get("submitted_at") or r.get("started_at") or 0, reverse=True)
    return rows


def create_app(
    db_path:          str | Path    = "turret.db",
    tower_url:        str           = "http://localhost:8000",
    log_dir:          str | Path    = "turret-logs",
    nextflow_bin:     str           = "nextflow",
    default_work_dir: Optional[str] = None,
    default_profile:  Optional[str] = None,
    auth_config:      Optional[AuthConfig] = None,
    browse_roots:     list[str | Path]     = (),
    upload_dir:       Optional[str | Path] = None,
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
    auth_config:
        Authentication configuration.  Pass ``None`` or ``AuthConfig()``
        (mode=none) to disable auth entirely.
    browse_roots:
        Filesystem directories users are allowed to browse via the
        ``/api/fs/browse`` endpoint.  Defaults to ``default_work_dir`` (if
        set) plus the current user's home directory.
    upload_dir:
        Root directory where files uploaded via ``POST /api/fs/upload`` are
        stored.  Defaults to a ``turret-uploads`` sub-directory of
        ``default_work_dir`` if set, otherwise ``~/.turret/uploads``.
        Individual uploads are placed in a project subdirectory
        (``<upload_dir>/<project>/``) when the ``project`` query parameter
        is supplied; otherwise a date-stamped subdirectory is auto-generated.
    """
    if auth_config is None:
        auth_config = AuthConfig()

    # ---- resolve filesystem roots ------------------------------------------
    _resolved_roots: list[Path] = []
    for r in browse_roots:
        _resolved_roots.append(Path(r).resolve())
    if not _resolved_roots:
        if default_work_dir:
            _resolved_roots.append(Path(default_work_dir).resolve())
        _resolved_roots.append(Path.home())

    _upload_dir: Path
    if upload_dir:
        _upload_dir = Path(upload_dir).resolve()
    elif default_work_dir:
        _upload_dir = Path(default_work_dir).resolve() / "turret-uploads"
    else:
        _upload_dir = Path.home() / ".turret" / "uploads"
    _upload_dir.mkdir(parents=True, exist_ok=True)
    # upload dir is implicitly also browseable
    if _upload_dir not in _resolved_roots:
        _resolved_roots.append(_upload_dir)
    # -------------------------------------------------------------------------

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
    auth_mgr   = AuthManager(auth_config)
    templates  = _make_templates(auth_mgr)

    app = FastAPI(
        title="Nextflow Turret",
        description="Self-hosted Nextflow Tower / Seqera Platform replacement",
        version="0.1.0",
    )

    # ------------------------------------------------------------------ #
    # Session + Auth middleware                                            #
    # ------------------------------------------------------------------ #
    if auth_mgr.enabled:
        from starlette.middleware.sessions import SessionMiddleware
        # AuthMiddleware added first → becomes the *inner* middleware (runs last on request)
        app.add_middleware(AuthMiddleware, auth_manager=auth_mgr)
        # SessionMiddleware → wraps AuthMiddleware so sessions are available inside it
        app.add_middleware(SessionMiddleware, secret_key=auth_config.session_secret)

    # ------------------------------------------------------------------ #
    # Security headers — added LAST so it is the outermost middleware and #
    # applies to ALL responses including 401s from AuthMiddleware          #
    # ------------------------------------------------------------------ #
    app.add_middleware(SecurityHeadersMiddleware)

    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    # ------------------------------------------------------------------ #
    # Route-level helpers (closures over app-scoped objects)               #
    # ------------------------------------------------------------------ #

    def _get_launch_or_404(launch_id: str):
        """Return a launch record or raise HTTP 404."""
        record = launcher.get(launch_id)
        if record is None:
            raise HTTPException(404, detail="Launch not found")
        return record

    def _error_launch_form_response(request: Request, error: str, form_data: dict, status_code: int = 422):
        """Return a launch form template response with an error message."""
        return templates.TemplateResponse(
            request,
            "launch_form.html",
            {"form": form_data, "error": error, "active_page": "launch"},
            status_code=status_code,
        )

    def _error_login_response(request: Request, next_url: str, error: str, status_code: int = 401):
        """Return a login form template response with an error message."""
        return templates.TemplateResponse(
            request,
            "login.html",
            {"next": next_url, "error": error, "active_page": None},
            status_code=status_code,
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
            raise HTTPException(404, detail="Unknown Tower action")
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
            raise HTTPException(404, detail="Workflow not found")
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
        return _get_launch_or_404(launch_id).as_dict()

    @app.get("/api/launches/{launch_id}/log", tags=["api"], response_class=PlainTextResponse)
    async def get_launch_log(
        launch_id: str,
        tail: Optional[int] = Query(default=None, description="Return only the last N lines"),
    ):
        """Return the stdout/stderr log for a launch."""
        _get_launch_or_404(launch_id)
        return PlainTextResponse(launcher.read_log(launch_id, tail=tail))

    @app.delete("/api/launches/{launch_id}", tags=["api"])
    async def cancel_launch(launch_id: str):
        """Cancel a running pipeline launch (sends SIGTERM)."""
        _get_launch_or_404(launch_id)
        sent = launcher.cancel(launch_id)
        if not sent:
            raise HTTPException(409, detail="Launch is not in a cancellable state")
        return {"launch_id": launch_id, "status": "cancelled"}

    @app.get("/api/pipeline/schema", tags=["api"])
    async def get_pipeline_schema(
        pipeline: str           = Query(...,      description="Pipeline identifier (e.g. nf-core/rnaseq)"),
        revision: Optional[str] = Query(default=None, description="Git revision / tag"),
    ):
        """Return parameter specs and available profiles for a pipeline.

        Fetches ``nextflow_schema.json`` for parameters and ``nextflow.config``
        for profile names in parallel.  Both are returned even if only one
        is found.
        """
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            params_future   = pool.submit(fetch_pipeline_schema,  pipeline, revision)
            profiles_future = pool.submit(fetch_pipeline_profiles, pipeline, revision)
            params   = params_future.result()
            profiles = profiles_future.result()

        return {
            "params":   [p.to_dict() for p in params],
            "count":    len(params),
            "profiles": profiles,
            "source":   "nextflow_schema.json" if params else None,
        }

    @app.get("/api/pipeline/refs", tags=["api"])
    async def get_pipeline_refs(
        pipeline: str = Query(..., description="Pipeline identifier (e.g. nf-core/rnaseq)"),
    ):
        """Return branches and tags for a GitHub-hosted pipeline."""
        return fetch_pipeline_refs(pipeline)

    # ------------------------------------------------------------------ #
    # Filesystem endpoints                                                 #
    # ------------------------------------------------------------------ #

    def _assert_under_root(path: Path) -> None:
        """Raise 403 if *path* is not within any allowed browse root."""
        resolved = path.resolve()
        for root in _resolved_roots:
            try:
                resolved.relative_to(root)
                return
            except ValueError:
                continue
        raise HTTPException(
            403,
            detail=(
                f"Access denied: {path} is not under an allowed browse root. "
                f"Allowed roots: {[str(r) for r in _resolved_roots]}"
            ),
        )

    @app.get("/api/fs/roots", tags=["api"])
    async def fs_roots():
        """Return the list of allowed filesystem browse roots."""
        return {"roots": [str(r) for r in _resolved_roots]}

    @app.get("/api/fs/browse", tags=["api"])
    async def fs_browse(path: str = Query("/", description="Absolute directory path to list")):
        """List the contents of a server-side directory.

        Only paths within the configured ``browse_roots`` are accessible.
        Returns a JSON object with the current path, its parent, and a list
        of entries (files and sub-directories).
        """
        target = Path(path).resolve()
        _assert_under_root(target)

        if not target.exists():
            raise HTTPException(404, detail=f"Path not found: {path}")
        if not target.is_dir():
            raise HTTPException(400, detail=f"Not a directory: {path}")

        entries = []
        try:
            for child in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
                if child.name.startswith("."):
                    continue
                try:
                    st = child.stat()
                    entries.append({
                        "name":    child.name,
                        "is_dir":  child.is_dir(),
                        "size":    st.st_size if not child.is_dir() else None,
                        "mtime":   datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
                        "path":    str(child),
                    })
                except PermissionError:
                    entries.append({"name": child.name, "is_dir": child.is_dir(), "error": "permission denied", "path": str(child)})
        except PermissionError as exc:
            raise HTTPException(403, detail=str(exc)) from exc

        parent = str(target.parent) if str(target) != str(target.parent) else None
        # Clamp parent to browse roots so users can't navigate above all roots
        if parent:
            parent_path = Path(parent)
            under_root = any(
                _is_relative_to(parent_path.resolve(), r) for r in _resolved_roots
            )
            if not under_root:
                parent = None

        return {
            "path":    str(target),
            "parent":  parent,
            "roots":   [str(r) for r in _resolved_roots],
            "entries": entries,
        }

    def _is_relative_to(child: Path, parent: Path) -> bool:
        """Return True if *child* is relative to *parent* (Python 3.9 compat)."""
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False

    @app.post("/api/fs/upload", tags=["api"])
    async def fs_upload(
        file:       UploadFile      = File(...),
        project:    str             = Query(default="", description="Project subdirectory for the uploaded file"),
        upload_dir: Optional[str]   = Query(default=None, alias="dir", description="Explicit destination directory (overrides project)"),
    ):
        """Upload a file to the server-side upload directory.

        If *dir* is supplied the file is placed directly in that directory
        (must be under an allowed browse root).  Otherwise files are stored
        under ``<upload_dir>/<project>/`` when *project* is supplied, or
        ``<upload_dir>/<YYYY-MM-DD>/`` when neither is given.  A numeric
        suffix is appended to the filename if a collision occurs.
        """
        if not file.filename:
            raise HTTPException(400, detail="No filename provided")

        safe_name = Path(file.filename).name
        if not safe_name or safe_name in (".", ".."):
            raise HTTPException(400, detail="Invalid filename")

        # Resolve destination directory
        if upload_dir:
            dest_dir = Path(upload_dir).resolve()
            _assert_under_root(dest_dir)
            safe_project = dest_dir.name
        elif project:
            safe_project = re.sub(r'[/\\]', '_', project.strip()).strip("._") or "default"
            dest_dir = _upload_dir / safe_project
        else:
            safe_project = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            dest_dir = _upload_dir / safe_project

        dest_dir.mkdir(parents=True, exist_ok=True)

        dest = dest_dir / safe_name
        if dest.exists():
            stem, suffix = dest.stem, dest.suffix
            counter = 1
            while dest.exists():
                dest = dest_dir / f"{stem}_{counter}{suffix}"
                counter += 1

        try:
            with dest.open("wb") as fh:
                shutil.copyfileobj(file.file, fh)
        except OSError as exc:
            raise HTTPException(500, detail=f"Failed to save file: {exc}") from exc
        finally:
            await file.close()

        return {"path": str(dest), "filename": dest.name, "size": dest.stat().st_size, "project": safe_project}

    @app.post("/api/fs/mkdir", tags=["api"])
    async def fs_mkdir(
        path: str = Query(..., description="Absolute path of the directory to create"),
    ):
        """Create a new directory on the server.

        The target path must be under one of the configured browse roots.
        Intermediate directories are created as needed (``mkdir -p`` semantics).
        Returns 409 if the path already exists and is not a directory.
        """
        target = Path(path).resolve()
        _assert_under_root(target)
        if target.exists():
            if target.is_dir():
                return {"path": str(target), "created": False}
            raise HTTPException(409, detail=f"Path exists and is not a directory: {path}")
        try:
            target.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise HTTPException(500, detail=f"Failed to create directory: {exc}") from exc
        return {"path": str(target), "created": True}

    @app.post("/api/pipeline/clone", tags=["api"])
    async def pipeline_clone(
        pipeline: str           = Query(..., description="Pipeline identifier (org/repo, full URL)"),
        path:     str           = Query(..., description="Destination parent directory; pipeline is cloned into a sub-directory"),
        revision: Optional[str] = Query(default=None, description="Branch or tag to clone"),
    ):
        """Clone a remote pipeline into a sub-directory of *path*.

        Resolves ``pipeline`` to a ``git clone``-able HTTPS URL, then runs::

            git clone [--branch <revision>] <url> <path>/<repo-name>

        Returns ``{"dest": "...", "repo": "..."}`` on success.
        Raises 400 for local paths, 409 if the destination already exists.
        """
        clone_url = resolve_pipeline_clone_url(pipeline)
        if clone_url is None:
            raise HTTPException(400, detail="Pipeline must be a remote URL or 'org/repo' short-form (not a local path)")

        parent = Path(path).resolve()
        _assert_under_root(parent)

        # Derive repo name from URL (strip .git suffix)
        repo_name = clone_url.rstrip("/").rsplit("/", 1)[-1]
        if repo_name.endswith(".git"):
            repo_name = repo_name[:-4]

        dest = parent / repo_name
        if dest.exists():
            raise HTTPException(409, detail=f"Destination already exists: {dest}")

        parent.mkdir(parents=True, exist_ok=True)

        cmd = ["git", "clone", "--depth", "1"]
        if revision:
            cmd += ["--branch", revision]
        cmd += [clone_url, str(dest)]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                raise HTTPException(504, detail="git clone timed out after 180 s")
        except FileNotFoundError:
            raise HTTPException(500, detail="git is not installed or not on PATH") from None

        if proc.returncode != 0:
            msg = stderr.decode(errors="replace").strip().splitlines()
            raise HTTPException(500, detail="git clone failed: " + (msg[-1] if msg else "unknown error"))

        return {"dest": str(dest), "repo": repo_name}

    # ------------------------------------------------------------------ #
    # Config editor endpoints                                              #
    # ------------------------------------------------------------------ #

    @app.get("/api/config", tags=["api"])
    async def get_config(
        path:     str           = Query(..., description="Project directory containing nextflow.config"),
        pipeline: Optional[str] = Query(default=None),
        revision: Optional[str] = Query(default=None),
    ):
        """Return nextflow.config content from the project directory.

        Falls back to fetching from the pipeline remote when the file does not
        yet exist locally.
        """
        config_path = Path(path) / "nextflow.config"
        if config_path.is_file():
            return {"content": config_path.read_text(errors="replace"), "source": "local", "path": str(config_path)}
        if pipeline:
            text = fetch_pipeline_config_text(pipeline, revision)
            if text:
                return {"content": text, "source": "pipeline", "path": str(config_path)}
        return {"content": "", "source": "empty", "path": str(config_path)}

    @app.post("/api/config", tags=["api"])
    async def save_config(
        path:    str     = Query(..., description="Project directory where nextflow.config will be written"),
        request: Request = None,
    ):
        """Write nextflow.config content to the project directory."""
        body    = await request.json()
        content = body.get("content", "")
        dir_path = Path(path).resolve()
        _assert_under_root(dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)
        config_path = dir_path / "nextflow.config"
        try:
            config_path.write_text(content)
        except OSError as exc:
            raise HTTPException(500, detail=f"Failed to save config: {exc}") from exc
        return {"saved": True, "path": str(config_path)}

    @app.get("/api/config/remote", tags=["api"])
    async def get_config_remote(
        pipeline: str           = Query(...),
        revision: Optional[str] = Query(default=None),
    ):
        """Fetch nextflow.config directly from the pipeline remote (no local cache)."""
        text = fetch_pipeline_config_text(pipeline, revision)
        if not text:
            raise HTTPException(404, detail="Could not fetch nextflow.config from pipeline")
        return {"content": text}

    @app.get("/config/edit", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_config_editor(
        path:     str           = Query(...),
        pipeline: Optional[str] = Query(default=None),
        revision: Optional[str] = Query(default=None),
        request:  Request       = None,
    ):
        """Config editor page for nextflow.config in the given project directory."""
        return templates.TemplateResponse(
            request,
            "config_editor.html",
            {
                "project_dir": path,
                "pipeline":    pipeline or "",
                "revision":    revision or "",
                "active_page": "launch",
            },
        )


    def _pipelines_context(request: Request) -> dict:
        rows         = _build_pipeline_rows(launcher, registry)
        total_failed = sum(r.get("task_counts", {}).get("failed", 0) for r in rows)
        n_running    = sum(1 for r in rows if r["display_status"] in ("running",))
        n_complete   = sum(1 for r in rows if r["display_status"] in ("complete", "succeeded"))
        return {
            "rows":         rows,
            "total_failed": total_failed,
            "n_running":    n_running,
            "n_complete":   n_complete,
            "active_page":  "pipelines",
        }

    @app.get("/", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_index(request: Request):
        return templates.TemplateResponse(request, "pipelines.html", _pipelines_context(request))

    @app.get("/runs/{workflow_id}", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_run_detail(workflow_id: str, request: Request):
        state = registry.get_by_id(workflow_id)
        if state is None:
            raise HTTPException(404, detail="Workflow not found")
        run = _enrich_run(state)
        return templates.TemplateResponse(
            request,
            "run_detail.html",
            {"run": run, "active_page": "pipelines"},
        )

    @app.get("/launches", response_class=HTMLResponse, tags=["ui"], include_in_schema=False)
    async def ui_launches(request: Request):
        """Alias for the unified pipelines dashboard."""
        return templates.TemplateResponse(request, "pipelines.html", _pipelines_context(request))

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
        pipeline:     str           = Form(...),
        revision:     Optional[str] = Form(default=None),
        profile:      Optional[str] = Form(default=None),
        work_dir:     Optional[str] = Form(default=None),
        run_name:     Optional[str] = Form(default=None),
        project_dir:  Optional[str] = Form(default=None),
        params:       Optional[str] = Form(default=None),
        csrf_token_field: Optional[str] = Form(default=None, alias="_csrf_token"),
    ):
        _csrf_validate(request, csrf_token_field)
        form_data = {
            "pipeline": pipeline, "revision": revision,
            "profile": profile, "work_dir": work_dir,
            "run_name": run_name, "params": params,
            "project_dir": project_dir,
        }
        # Collect individual param__KEY fields from the form
        raw_form      = await request.form()
        parsed_params: dict = {}

        def _validate_param_key(key: str) -> bool:
            return bool(_VALID_PARAM_KEY.match(key))

        # Priority 1: individual param__KEY fields (schema-driven form)
        individual = {
            k[7:]: str(v)
            for k, v in raw_form.items()
            if k.startswith("param__") and str(v).strip()
        }
        invalid_keys = [k for k in individual if not _validate_param_key(k)]
        if invalid_keys:
            return _error_launch_form_response(
                request, f"Invalid parameter name(s): {', '.join(invalid_keys)}", form_data
            )
        if individual:
            parsed_params = individual
        elif params and params.strip():
            # Priority 2: legacy JSON textarea fallback
            if len(params.encode()) > _MAX_PARAMS_JSON_BYTES:
                return _error_launch_form_response(request, "params JSON exceeds maximum size", form_data)
            try:
                parsed_params = json.loads(params)
                if not isinstance(parsed_params, dict):
                    raise ValueError("params must be a JSON object")
                invalid_keys = [k for k in parsed_params if not _validate_param_key(str(k))]
                if invalid_keys:
                    raise ValueError(f"Invalid parameter name(s): {', '.join(invalid_keys)}")
            except Exception as exc:
                return _error_launch_form_response(request, f"Invalid params JSON: {exc}", form_data)

        # Sanitise empty strings to None
        revision    = revision    or None
        profile     = profile     or None
        work_dir    = work_dir    or None
        run_name    = run_name    or None
        project_dir = project_dir or None

        # When pipeline is a local path the project directory IS the pipeline directory.
        _pipeline_is_local = pipeline.startswith("/") or pipeline.startswith(".")
        if _pipeline_is_local and not project_dir:
            project_dir = str(Path(pipeline).resolve())

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
        record = _get_launch_or_404(launch_id)
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
    async def ui_cancel_launch(
        launch_id:   str,
        request:     Request,
        csrf_token_field: Optional[str] = Form(default=None, alias="_csrf_token"),
    ):
        _csrf_validate(request, csrf_token_field)
        _get_launch_or_404(launch_id)
        launcher.cancel(launch_id)
        return RedirectResponse(url=f"/launches/{launch_id}", status_code=303)

    # ------------------------------------------------------------------ #
    # Auth routes                                                          #
    # ------------------------------------------------------------------ #

    @app.get("/auth/login", response_class=HTMLResponse, tags=["auth"], include_in_schema=False)
    async def auth_login_form(request: Request, next: Optional[str] = None):
        # Validate and sanitise the `next` param before using it
        destination = safe_next_url(next, default="/")
        if auth_mgr.config.mode == AuthMode.OIDC:
            # OIDC: store the intended destination in the session; use a random
            # opaque token as the `state` parameter (prevents open-redirect via state).
            oauth    = auth_mgr.get_oauth()
            cb_url   = auth_mgr.config.oidc.redirect_uri or str(request.url_for("auth_callback"))
            oidc_state = secrets.token_urlsafe(32)
            request.session["oidc_state"]       = oidc_state
            request.session["oidc_next"]        = destination
            return await oauth.oidc.authorize_redirect(request, cb_url, state=oidc_state)
        # Basic auth — show login form
        return templates.TemplateResponse(
            request,
            "login.html",
            {"next": destination, "error": None, "active_page": None},
        )

    @app.post("/auth/login", response_class=HTMLResponse, tags=["auth"], include_in_schema=False)
    async def auth_login_submit(
        request: Request,
        username:    str           = Form(...),
        password:    str           = Form(...),
        next:        Optional[str] = Form(default="/"),
        csrf_token_field: Optional[str] = Form(default=None, alias="_csrf_token"),
    ):
        # CSRF check
        _csrf_validate(request, csrf_token_field)
        # Rate limiting: check before verifying credentials
        if not _login_rate_limiter.check_and_record(request):
            return _error_login_response(
                request, safe_next_url(next, default="/"),
                "Too many login attempts. Please wait and try again.", 429,
            )
        # Validate next before use
        destination = safe_next_url(next, default="/")
        user = auth_mgr.verify_basic_credentials(username, password)
        if user is None:
            return _error_login_response(request, destination, "Invalid username or password", 401)
        _login_rate_limiter.reset(request)
        auth_mgr.set_user(request, user)
        return RedirectResponse(destination, status_code=303)

    @app.get("/auth/callback", tags=["auth"], include_in_schema=False)
    async def auth_callback(request: Request):
        """OIDC authorization code callback."""
        if auth_mgr.config.mode != AuthMode.OIDC:
            raise HTTPException(404)

        # Verify state matches what we stored in the session (CSRF protection)
        stored_state   = request.session.pop("oidc_state", None)
        received_state = request.query_params.get("state")
        if not stored_state or stored_state != received_state:
            raise HTTPException(403, detail="Invalid OIDC state")

        oauth  = auth_mgr.get_oauth()
        token  = await oauth.oidc.authorize_access_token(request)
        claims = token.get("userinfo") or token.get("id_token_claims") or {}
        user   = {
            "username":    claims.get("email") or claims.get("sub", "unknown"),
            "email":       claims.get("email"),
            "name":        claims.get("name"),
            "auth_method": "oidc",
        }
        auth_mgr.set_user(request, user)
        # Retrieve and clear the saved destination (already validated when stored)
        destination = request.session.pop("oidc_next", "/")
        return RedirectResponse(safe_next_url(destination, default="/"), status_code=303)

    @app.get("/auth/logout", tags=["auth"], include_in_schema=False)
    async def auth_logout(request: Request):
        auth_mgr.clear_user(request)
        return RedirectResponse("/auth/login", status_code=303)

    @app.get("/auth/whoami", tags=["auth"])
    async def auth_whoami(request: Request):
        """Return the currently authenticated user (or 401)."""
        user = auth_mgr.get_user(request)
        if user is None:
            raise HTTPException(401, detail="Not authenticated")
        return user

    return app


# Copilot Instructions — Nextflow Turret

## Project Purpose

**Nextflow Turret** is a self-hosted, in-process replacement for [Seqera Platform](https://seqera.io) (formerly Nextflow Tower). It implements just enough of the Tower REST API to accept `-with-tower` trace events from Nextflow without requiring a Seqera Platform subscription, accumulating them into an in-memory registry that your application can query for live progress.

## Build & Test

```bash
# Install core library (stdlib only, no deps)
pip install -e .

# Install with server extras (FastAPI + uvicorn)
pip install -e ".[server]"

# Install dev dependencies (pytest, httpx, fastapi)
pip install -e ".[dev]"

# Run all tests
pytest

# Run a single test
pytest tests/test_turret.py::TestWorkflowRegistry::test_register_and_lookup

# Run a test class
pytest tests/test_turret.py::TestTowerRouterPut

# Start the server
python -m nextflow_turret.server
python -m nextflow_turret.server --host 127.0.0.1 --port 9000 --db /data/turret.db

# Or via the installed script
turret --port 8000 --db turret.db
```

---

## Architecture

Five source modules + one public surface:

| File | Role |
|---|---|
| `state.py` | `WorkflowState` + `WorkflowRegistry` — thread-safe in-memory store |
| `handlers.py` | `TowerRouter` — framework-agnostic HTTP route dispatcher |
| `utils.py` | Standalone helpers (currently just `tower_process_to_slurm_name`) |
| `__init__.py` | Re-exports the full public API; also holds the module-level singleton |
| `db/store.py` | `RunStore` — SQLite persistence (stdlib `sqlite3`, no ORM) |
| `server/registry.py` | `PersistentWorkflowRegistry` — write-through subclass of `WorkflowRegistry` |
| `server/app.py` | FastAPI application (`create_app(db_path)`) |
| `server/__main__.py` | CLI entry point (`python -m nextflow_turret.server`) |

**Data flow:**  
Nextflow calls `POST /trace/create` → `PUT /trace/{id}/begin` → `PUT /trace/{id}/progress` (repeatedly) → `PUT /trace/{id}/complete`. `TowerRouter.handle_*` methods translate each call into `WorkflowRegistry` mutations. The server uses `PersistentWorkflowRegistry` so every mutation is immediately written to SQLite. Callers read state back via `registry.get_by_batch(batch_id)` or the REST API (`GET /api/runs`).

**Module-level singleton:**  
`state.py` defines `default_registry = WorkflowRegistry()` at module level. The convenience functions (`register_workflow`, `update_progress`, etc.) delegate to it. Tests always construct a fresh `WorkflowRegistry()` fixture to stay isolated from the singleton.

---

## Key Conventions

**`TowerRouter` handlers return `None` for unrecognised routes** — the caller is expected to send a 404. They return `(int, dict)` tuples for matched routes; the caller serialises to JSON.

**Canonical `workflow_id` format is `dispatcher_{batch_id}`** (see `workflow_id_for_batch`). `TowerRouter` derives `batch_id` from the Nextflow `runName` by stripping the `dispatcher_` prefix. This mapping is customisable via `run_name_to_batch_id=` at construction time.

**`WorkflowState._ingest` must only be called while holding `registry._lock`.** Mutation methods on `WorkflowRegistry` acquire the lock themselves before calling `_ingest`. Do not call `_ingest` from outside the registry.

**`WorkflowState` uses `__slots__`** — do not add attributes dynamically; declare new fields in `__slots__` and initialise them in `__init__`.

**`pct` calculation** counts `succeeded + cached` as "done"; `failed`, `running`, `pending`, and `submitted` contribute to `total`. `aborted` tasks are excluded from both.

**Failures are capped at 50** (most-recent kept). Duplicate `taskId` entries within `failures` are deduplicated on ingest.

**`get_by_batch` lookup** first tries the canonical key `dispatcher_{batch_id}`, then falls back to a linear scan by `batch_id` field. Prefer the canonical naming to avoid the O(n) fallback.

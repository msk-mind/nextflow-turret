# Nextflow Turret

A self-hosted replacement for [Seqera Platform](https://seqera.io/) (formerly Nextflow Tower).
Run any Nextflow pipeline with `-with-tower`, track progress in real time, launch new runs from
a web UI, and inspect logs — all without a cloud subscription.

---

## Features

| Feature | Details |
|---------|---------|
| **Tower trace receiver** | Accepts the full Nextflow `-with-tower` HTTP protocol |
| **SQLite persistence** | Every run is immediately written to disk; survives restarts |
| **Pipeline launcher** | Submit pipelines via web form or REST API; cancel running jobs |
| **Web dashboard** | Live run list, per-run progress detail, failed-task drill-down |
| **REST API** | JSON API for runs and launches (OpenAPI docs at `/docs`) |
| **Embeddable library** | Use `WorkflowRegistry` + `TowerRouter` inside your own Python app |

---

## Installation

```bash
# Into an existing project
uv add "nextflow-turret @ git+https://github.com/msk-mind/nextflow-turret.git"

# Or with pip
pip install "nextflow-turret @ git+https://github.com/msk-mind/nextflow-turret.git"
```

---

## Quick start — standalone server

```bash
# Start the server (default: http://0.0.0.0:8000, DB: turret.db)
turret

# Or with explicit options (always override config file)
turret --host 127.0.0.1 --port 9000 --db /data/turret.db --log-dir /data/logs

# Or via Python
python -m nextflow_turret.server --port 9000
```

### Config file

Create a `turret.toml` in the working directory (or `~/.config/turret/config.toml` for user-level defaults). CLI flags always win over config values.

```toml
[server]
host    = "0.0.0.0"
port    = 8000
db      = "/data/turret.db"
log_dir = "/data/turret-logs"

[launcher]
nextflow        = "/opt/nextflow/nextflow"
work_dir        = "/scratch/nf-work"
default_profile = "slurm"
```

A fully-commented template is at [`turret.toml.example`](turret.toml.example).

Then point Nextflow at it:

```bash
nextflow run nf-core/rnaseq -with-tower http://localhost:8000 -name dispatcher_mybatch \
  -profile docker -r 3.14.0
```

Open **http://localhost:8000** to see the live dashboard.

---

## REST API

Interactive docs are available at **`/docs`** when the server is running.

### Tower trace endpoints (called by Nextflow)

| Method | Path | Purpose |
|--------|------|---------|
| `GET`  | `/user-info` | Auth check on Nextflow startup |
| `POST` | `/trace/create` | Workflow registered → returns `{workflowId}` |
| `PUT`  | `/trace/{id}/begin` | Workflow running |
| `PUT`  | `/trace/{id}/progress` | Periodic task counts + per-task list |
| `PUT`  | `/trace/{id}/heartbeat` | Keepalive (same payload as progress) |
| `PUT`  | `/trace/{id}/complete` | Workflow finished |

### Runs API

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/api/runs` | List all runs (newest first) |
| `GET`  | `/api/runs/{workflow_id}` | Single run detail with task counts, failures, resources |

### Launches API

| Method   | Path | Description |
|----------|------|-------------|
| `GET`    | `/api/launches` | List all launches (newest first) |
| `POST`   | `/api/launches` | Submit a new pipeline launch |
| `GET`    | `/api/launches/{id}` | Launch status and metadata |
| `GET`    | `/api/launches/{id}/log?tail=N` | stdout/stderr log (optionally tailed) |
| `DELETE` | `/api/launches/{id}` | Cancel a running launch (SIGTERM) |

#### Submit a launch via REST

```bash
curl -X POST http://localhost:8000/api/launches \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline":  "nf-core/rnaseq",
    "revision":  "3.14.0",
    "profile":   "docker",
    "params":    {"input": "samplesheet.csv", "genome": "GRCh38"},
    "work_dir":  "/scratch/work"
  }'
```

---

## run_name → batch_id mapping

Nextflow is launched with `-name dispatcher_{batch_id}`.  The `dispatcher_` prefix
is stripped automatically to produce the *batch_id* that identifies the run.

To use a different prefix, create your own `TowerRouter`:

```python
from nextflow_turret import TowerRouter, WorkflowRegistry

registry = WorkflowRegistry()
router   = TowerRouter(
    registry=registry,
    run_name_to_batch_id=lambda name: name.removeprefix("mypipeline_"),
)
```

---

## Embedding in your own Python app

If you prefer to host the Tower receiver inside an existing service rather than running
the standalone server, use the core library directly:

```python
from nextflow_turret import TowerRouter, WorkflowRegistry

registry = WorkflowRegistry()
router   = TowerRouter(registry=registry)

# In your HTTP handler:
result = router.handle_get(path)          # GET /user-info
result = router.handle_post(path, body)   # POST /trace/create
result = router.handle_put(path, body)    # PUT  /trace/{id}/progress

# Read progress at any time:
state = registry.get_by_batch("mybatch")
print(state["pct"], "% complete")
print(state["task_counts"])   # {"succeeded": 42, "running": 3, ...}
print(state["failures"])      # [{"taskId": 7, "process": "ALIGN", "exit": 1}, ...]
```

### Module-level singleton

For quick scripts, a global registry is available:

```python
import nextflow_turret as turret

turret.register_workflow(workflow_id, batch_id, run_name)
turret.update_progress(workflow_id, progress_dict, tasks_list)
turret.mark_complete(workflow_id)

state = turret.get_progress(batch_id)   # → dict or None
```

---

## Utilities

```python
from nextflow_turret import tower_process_to_slurm_name

# Convert a Tower process name to a SLURM job name prefix
tower_process_to_slurm_name("MUSSEL:EXTRACT_FEATURES:TESSELLATE_FEATURIZE_BATCH")
# → "MUSSEL_EXTRACT_FEATURES_TESSELLATE_FEATURIZE_BATCH"
```

---

## Architecture

```
nextflow_turret/
├── state.py            # WorkflowRegistry, WorkflowState — core in-memory model
├── handlers.py         # TowerRouter — parses Tower HTTP payloads
├── db/
│   └── store.py        # RunStore — SQLite persistence (runs + launches)
├── launcher/
│   └── launcher.py     # Launcher — subprocess management for pipeline runs
└── server/
    ├── app.py          # FastAPI application factory (create_app)
    ├── registry.py     # PersistentWorkflowRegistry — DB-backed registry
    ├── __main__.py     # CLI entry point (turret / python -m nextflow_turret.server)
    └── templates/      # Jinja2 HTML templates (dashboard, detail, launch form)
```

Key design decisions:

- **Write-through persistence** — every Tower trace event is committed to SQLite before the HTTP response is sent.
- **Restart recovery** — on startup, `PersistentWorkflowRegistry` re-hydrates all rows from the DB so in-progress runs can continue receiving trace events.
- **No external dependencies** for the core library — only `fastapi`, `uvicorn`, and `jinja2` are needed for the server extras.

---

## Development

```bash
git clone https://github.com/msk-mind/nextflow-turret.git
cd nextflow-turret

# Run tests (uv installs deps automatically)
uv run pytest

# Or sync into a venv and work interactively
uv sync --extra server
source .venv/bin/activate
pytest --tb=short -q
```

### Test layout

| File | Tests | Coverage |
|------|-------|---------|
| `tests/test_turret.py` | 39 | Core library (`WorkflowRegistry`, `WorkflowState`, `TowerRouter`) |
| `tests/test_server.py` | 44 | Server layer (`RunStore`, `PersistentWorkflowRegistry`, all endpoints) |
| `tests/test_integration.py` | 32 | Cross-layer: Tower→SQLite, restart recovery, concurrency, API/DB/UI consistency |
| `tests/test_e2e.py` | 34 | Full scenarios: NF trace lifecycle, failed tasks, multi-run dashboard, launch UI journey, error paths |

---

## License

Nextflow Turret is released under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0-or-later).

This means:
- **Free to use, modify, and self-host** for any purpose.
- If you modify Nextflow Turret and **run it as a network service**, you must make your modified source code available to users of that service.

See [LICENSE](LICENSE) for the full terms.

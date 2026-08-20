"""In-memory Nextflow Tower API state store.

Accumulates Tower trace events (progress, task counts, per-process status,
failures) emitted by ``nextflow run -with-tower`` into thread-safe
:class:`WorkflowState` objects keyed by workflow ID.

Typical lifecycle
-----------------
::

    registry = WorkflowRegistry()          # or use module-level singleton

    # NF sends POST /trace/create
    registry.register(workflow_id, batch_id, run_name)

    # NF sends PUT /trace/{id}/progress repeatedly
    registry.update_progress(workflow_id, progress_dict, tasks_list)

    # NF sends PUT /trace/{id}/complete
    registry.mark_complete(workflow_id)

    # Dashboard reads current state
    state = registry.get_by_batch(batch_id)   # -> dict or None

Tower progress payload fields
-------------------------------
``succeeded``, ``failed``, ``cached``, ``running``, ``pending``,
``submitted``, ``aborted``, ``loadCpus``, ``loadMemory``, ``peakCpus``,
``peakMemory``, ``peakRunning``,
``processes`` (list of per-process ProgressRecord dicts).

Each task in ``tasks[]`` (/trace/progress only):
``taskId``, ``status``, ``hash``, ``name``, ``process``, ``tag``,
``exit``, ``start``, ``complete``, …
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Mapping, Optional


_MAX_AGE_SECONDS = 3600   # evict completed workflows older than this
_STALE_SECONDS   = 5 * 60 # mark workflow stalled if no update in this window


def _task_counts_from_progress(p: Mapping) -> dict:
    if not isinstance(p, Mapping):
        p = {}
    def _count(name: str) -> int:
        try:
            return max(0, int(p.get(name, 0) or 0))
        except (TypeError, ValueError):
            return 0

    return {
        "succeeded": _count("succeeded"),
        "failed":    _count("failed"),
        "cached":    _count("cached"),
        "running":   _count("running"),
        "pending":   _count("pending"),
        "submitted": _count("submitted"),
        "aborted":   _count("aborted"),
    }


class WorkflowState:
    """Snapshot of one Nextflow workflow's Tower-reported progress."""

    __slots__ = (
        "workflow_id", "batch_id", "run_name",
        "task_counts", "processes", "resources",
        "failures", "complete",
        "started_at", "updated_at",
    )

    def __init__(self, workflow_id: str, batch_id: str, run_name: str) -> None:
        self.workflow_id  = workflow_id
        self.batch_id     = batch_id
        self.run_name     = run_name
        self.task_counts: dict       = {}
        self.processes:   list[dict] = []
        self.resources:   dict       = {}
        self.failures:    list[dict] = []   # last 50 FAILED tasks
        self.complete     = False
        self.started_at   = time.time()
        self.updated_at   = time.time()

    # ------------------------------------------------------------------
    def _ingest(self, progress: dict, tasks: list[dict] | None = None) -> None:
        """Merge a progress payload.  Call only while holding the registry lock."""
        self.task_counts = _task_counts_from_progress(progress)
        self.processes   = progress.get("processes") or []
        self.resources   = {
            k: progress[k]
            for k in ("loadCpus", "loadMemory", "peakCpus", "peakMemory", "peakRunning")
            if progress.get(k) is not None
        }
        if tasks:
            seen = {f["taskId"] for f in self.failures if "taskId" in f}
            for t in tasks:
                if (t.get("status") or "").upper() == "FAILED" and t.get("taskId") not in seen:
                    self.failures.append({
                        "taskId":  t.get("taskId"),
                        "process": t.get("process", ""),
                        "name":    t.get("name", ""),
                        "tag":     t.get("tag"),
                        "exit":    t.get("exit"),
                        "hash":    t.get("hash"),
                    })
            if len(self.failures) > 50:
                self.failures = self.failures[-50:]
        self.updated_at = time.time()

    def is_stalled(self, stale_seconds: float = _STALE_SECONDS) -> bool:
        return not self.complete and (time.time() - self.updated_at) > stale_seconds

    def as_dict(self) -> dict:
        return normalize_workflow_state({
            "workflow_id":  self.workflow_id,
            "batch_id":     self.batch_id,
            "run_name":     self.run_name,
            "task_counts":  self.task_counts,
            "processes":    self.processes,
            "resources":    self.resources,
            "failures":     self.failures,
            "complete":     self.complete,
            "started_at":   self.started_at,
            "updated_at":   self.updated_at,
        })


class WorkflowRegistry:
    """Thread-safe registry of :class:`WorkflowState` objects.

    Instantiate directly for isolated use (e.g. testing), or use the
    module-level :data:`default_registry` singleton via the convenience
    functions at the bottom of this module.
    """

    def __init__(self) -> None:
        self._lock      = threading.Lock()
        self._workflows: dict[str, WorkflowState] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def register(self, workflow_id: str, batch_id: str, run_name: str) -> None:
        """Register (or replace) a workflow."""
        with self._lock:
            self._workflows[workflow_id] = WorkflowState(workflow_id, batch_id, run_name)

    def update_progress(
        self,
        workflow_id: str,
        progress: dict,
        tasks: list[dict] | None = None,
    ) -> None:
        with self._lock:
            state = self._workflows.get(workflow_id)
            if state is not None:
                state._ingest(progress, tasks)

    def mark_complete(self, workflow_id: str, progress: dict | None = None) -> None:
        with self._lock:
            state = self._workflows.get(workflow_id)
            if state is None:
                return
            state.complete = True
            if progress:
                state._ingest(progress)
            else:
                state.updated_at = time.time()

    def evict_old(self, max_age_seconds: float = _MAX_AGE_SECONDS) -> int:
        """Remove completed workflows older than *max_age_seconds*.  Returns count removed."""
        cutoff = time.time() - max_age_seconds
        with self._lock:
            to_remove = [
                wid for wid, s in self._workflows.items()
                if s.complete and s.updated_at < cutoff
            ]
            for wid in to_remove:
                del self._workflows[wid]
        return len(to_remove)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def is_registered(self, workflow_id: str) -> bool:
        with self._lock:
            return workflow_id in self._workflows

    def get_by_id(self, workflow_id: str) -> Optional[dict]:
        with self._lock:
            state = self._workflows.get(workflow_id)
            return state.as_dict() if state else None

    def get_by_batch(self, batch_id: str) -> Optional[dict]:
        """Retrieve state for *batch_id*, returning ``None`` if not known."""
        with self._lock:
            state = self._by_batch_id_locked(batch_id)
            return state.as_dict() if state else None

    def get_all(self) -> list[dict]:
        with self._lock:
            return [s.as_dict() for s in self._workflows.values()]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _by_batch_id_locked(self, batch_id: str) -> Optional[WorkflowState]:
        matches = [s for s in self._workflows.values() if s.batch_id == batch_id]
        if not matches:
            return None
        return max(matches, key=lambda state: state.updated_at)


# ---------------------------------------------------------------------------
# Canonical workflow_id convention
# ---------------------------------------------------------------------------

def workflow_id_for_batch(batch_id: str) -> str:
    """Return a deterministic nf-tower-compatible ID for *batch_id*.

    nf-tower limits workflow IDs to 16 characters. Keep the historical
    ``dispatcher_`` form where it fits so existing short IDs remain stable;
    long IDs use a deterministic opaque 16-character value instead.
    """
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("batch_id must be a non-empty string")
    legacy = f"dispatcher_{batch_id}"
    if len(legacy) <= 16:
        return legacy
    digest = uuid.uuid5(uuid.NAMESPACE_URL, f"nextflow-turret:{batch_id}").hex[:15]
    return f"r{digest}"


# ---------------------------------------------------------------------------
# Module-level singleton + backward-compatible convenience functions
# ---------------------------------------------------------------------------

default_registry = WorkflowRegistry()


def register_workflow(workflow_id: str, batch_id: str, run_name: str) -> None:
    default_registry.register(workflow_id, batch_id, run_name)


def is_registered(workflow_id: str) -> bool:
    return default_registry.is_registered(workflow_id)


def update_progress(
    workflow_id: str,
    progress: dict,
    tasks: list[dict] | None = None,
) -> None:
    default_registry.update_progress(workflow_id, progress, tasks)


def mark_complete(workflow_id: str, progress: dict | None = None) -> None:
    default_registry.mark_complete(workflow_id, progress)


def get_progress(batch_id: str) -> Optional[dict]:
    """Return full state dict for a batch, or ``None`` if not tracked via Tower."""
    return default_registry.get_by_batch(batch_id)


# Alias
get_state = get_progress


def get_all_states() -> list[dict]:
    return default_registry.get_all()


def evict_old(max_age_seconds: float = _MAX_AGE_SECONDS) -> int:
    return default_registry.evict_old(max_age_seconds)


def normalize_workflow_state(
    row: Mapping,
    *,
    now: float | None = None,
    stale_seconds: float = _STALE_SECONDS,
) -> dict:
    """Return a normalized, derived-field-enriched copy of a persisted state.

    This is useful when a consumer reads :class:`RunStore` rows directly
    instead of through :class:`PersistentWorkflowRegistry`.
    """
    raw_counts = row.get("task_counts") or {}
    if not isinstance(raw_counts, Mapping):
        raw_counts = {}
    counts = _task_counts_from_progress(raw_counts)

    def _copy_items(value) -> list[dict]:
        result = []
        for item in value or []:
            try:
                result.append(dict(item))
            except (TypeError, ValueError):
                continue
        return result

    state = dict(row)
    state["task_counts"] = counts
    state["processes"] = _copy_items(row.get("processes"))
    state["failures"] = _copy_items(row.get("failures"))
    state["complete"] = bool(row.get("complete"))
    done = counts["succeeded"] + counts["cached"]
    total = done + sum(
        counts[key] for key in ("failed", "running", "pending", "submitted")
    )
    state["done"] = done
    state["total"] = total
    state["pct"] = round(done / total * 100) if total else 0
    if "stalled" not in state:
        current_time = time.time() if now is None else now
        try:
            updated_at = float(state.get("updated_at") or 0.0)
        except (TypeError, ValueError):
            updated_at = 0.0
        state["stalled"] = (
            not state["complete"] and current_time - updated_at > stale_seconds
        )
    return state

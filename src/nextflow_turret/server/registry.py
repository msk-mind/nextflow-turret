"""Write-through WorkflowRegistry backed by a RunStore.

:class:`PersistentWorkflowRegistry` extends :class:`WorkflowRegistry` so that
every mutation (register, update_progress, mark_complete) is immediately
persisted to the :class:`~nextflow_turret.db.RunStore`.  On construction it
hydrates the in-memory cache from the DB so previous runs survive restarts.
"""
from __future__ import annotations

from ..state import WorkflowRegistry, WorkflowState
from ..db.store import RunStore


class PersistentWorkflowRegistry(WorkflowRegistry):
    """A :class:`WorkflowRegistry` that writes through to a :class:`RunStore`.

    Parameters
    ----------
    store:
        The backing SQLite store.
    """

    def __init__(self, store: RunStore) -> None:
        super().__init__()
        self._store = store
        self._hydrate()

    # ------------------------------------------------------------------

    def _hydrate(self) -> None:
        """Load all persisted runs into the in-memory cache on startup."""
        for row in self._store.load_all():
            state = WorkflowState(row["workflow_id"], row["batch_id"], row["run_name"])
            state.task_counts = row["task_counts"]
            state.processes   = row["processes"]
            state.resources   = row["resources"]
            state.failures    = row["failures"]
            state.complete    = row["complete"]
            state.started_at  = row["started_at"]
            state.updated_at  = row["updated_at"]
            self._workflows[state.workflow_id] = state

    # ------------------------------------------------------------------
    # Overrides — call super then persist
    # ------------------------------------------------------------------

    def register(self, workflow_id: str, batch_id: str, run_name: str) -> None:
        super().register(workflow_id, batch_id, run_name)
        self._persist(workflow_id)

    def update_progress(
        self,
        workflow_id: str,
        progress: dict,
        tasks: list[dict] | None = None,
    ) -> None:
        super().update_progress(workflow_id, progress, tasks)
        self._persist(workflow_id)

    def mark_complete(self, workflow_id: str, progress: dict | None = None) -> None:
        super().mark_complete(workflow_id, progress)
        self._persist(workflow_id)

    # ------------------------------------------------------------------

    def _persist(self, workflow_id: str) -> None:
        state_dict = self.get_by_id(workflow_id)
        if state_dict is not None:
            self._store.upsert(state_dict)

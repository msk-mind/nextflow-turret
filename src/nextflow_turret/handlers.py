"""Framework-agnostic Tower HTTP route handlers.

:class:`TowerRouter` sits in front of a :class:`~nextflow_turret.state.WorkflowRegistry`
and translates raw HTTP path/body pairs into structured results.  Each
handler method returns ``(status_code: int, body: dict)`` — the caller
is responsible for serialising to JSON and sending the HTTP response.

This design is framework-neutral: plug it into ``http.server``, Flask,
FastAPI, or any other HTTP layer by forwarding the path and parsed body.

Nextflow Tower protocol (endpoints that NF calls)
--------------------------------------------------
::

    GET  /user-info                 auth check on NF startup
    POST /trace/create              workflow start → returns {workflowId}
    PUT  /trace/{id}/begin          workflow running (runName available)
    PUT  /trace/{id}/progress       periodic task counts + per-task list
    PUT  /trace/{id}/heartbeat      keepalive (same payload as progress)
    PUT  /trace/{id}/complete       workflow finished

The ``runName`` sent by Nextflow is the string passed via ``-name`` on the
``nextflow run`` command line.  By convention, mussel-nf uses
``dispatcher_{batch_id}`` so that :class:`TowerRouter` can automatically
map a workflow ID back to a *batch_id* for display.

Customising the runName→batch_id mapping
-----------------------------------------
Pass a *run_name_to_batch_id* callable to :class:`TowerRouter`:

.. code-block:: python

    def my_extractor(run_name: str) -> str:
        return run_name.removeprefix("mypipeline_")

    router = TowerRouter(registry=reg, run_name_to_batch_id=my_extractor)
"""
from __future__ import annotations

import time
import uuid
from typing import Callable, Optional

from .state import WorkflowRegistry, default_registry, workflow_id_for_batch


# ---------------------------------------------------------------------------
# Standalone response factories (no registry needed)
# ---------------------------------------------------------------------------

def user_info_response() -> dict:
    """Minimal payload for ``GET /user-info`` so Nextflow passes its auth check."""
    return {
        "user": {
            "id":       1,
            "userName": "turret",
            "email":    "turret@local",
            "firstName": "Turret",
            "lastName":  "Shim",
            "organization": "",
            "avatar":    None,
            "trusted":   True,
        }
    }


def trace_create_response(workflow_id: str) -> dict:
    """Payload for ``POST /trace/create`` — tells NF its assigned workflow ID."""
    return {"workflowId": workflow_id, "watchUrl": None}


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

def _default_run_name_to_batch_id(run_name: str) -> str:
    """Extract a batch_id from a run_name using the ``dispatcher_`` prefix convention."""
    prefix = "dispatcher_"
    if run_name.startswith(prefix):
        batch_id = run_name[len(prefix):]
        return batch_id if batch_id else str(uuid.uuid4())
    return run_name


class TowerRouter:
    """Route Tower trace HTTP calls to a :class:`WorkflowRegistry`.

    Parameters
    ----------
    registry:
        Registry to update.  Defaults to the module-level singleton.
    run_name_to_batch_id:
        Callable that converts a NF run name string to a *batch_id*.
        Defaults to stripping the ``dispatcher_`` prefix.
    """

    def __init__(
        self,
        registry: Optional[WorkflowRegistry] = None,
        run_name_to_batch_id: Optional[Callable[[str], str]] = None,
    ) -> None:
        self._reg = registry if registry is not None else default_registry
        self._extract_batch_id: Callable[[str], str] = (
            run_name_to_batch_id if run_name_to_batch_id is not None
            else _default_run_name_to_batch_id
        )

    # ------------------------------------------------------------------
    # Public: one method per HTTP verb
    # ------------------------------------------------------------------

    def handle_get(self, path: str) -> Optional[tuple[int, dict]]:
        """Handle a GET request.

        Returns ``(status, body)`` if the path is a Tower route,
        ``None`` if the path is not recognised (caller handles 404).
        """
        p = path.split("?")[0]
        if p == "/user-info":
            return 200, user_info_response()
        return None

    def handle_post(self, path: str, body: dict) -> Optional[tuple[int, dict]]:
        """Handle a POST request.

        Returns ``(status, body)`` or ``None`` if not a Tower route.
        """
        p = path.split("?")[0]
        if p == "/trace/create":
            run_name    = body.get("runName") or ""
            batch_id    = self._extract_batch_id(run_name) if run_name else str(uuid.uuid4())
            workflow_id = workflow_id_for_batch(batch_id)
            self._reg.register(workflow_id, batch_id, run_name or workflow_id)
            return 200, trace_create_response(workflow_id)
        return None

    def handle_put(self, path: str, body: dict) -> Optional[tuple[int, dict]]:
        """Handle a PUT request.

        Returns ``(status, body)`` or ``None`` if not a Tower route.
        """
        p      = path.split("?")[0]
        parts  = p.strip("/").split("/")
        if len(parts) != 3 or parts[0] != "trace":
            return None

        workflow_id = parts[1]
        action      = parts[2]

        if action in ("progress", "heartbeat"):
            progress = body.get("progress") or {}
            tasks    = body.get("tasks")    or []
            self._auto_register(workflow_id, body)
            self._reg.update_progress(workflow_id, progress, tasks)
            return 200, {}

        if action == "complete":
            progress = body.get("progress") or {}
            self._reg.mark_complete(workflow_id, progress or None)
            return 200, {}

        if action == "begin":
            run_name = (body.get("workflow") or {}).get("runName", "")
            self._auto_register(workflow_id, body, run_name=run_name)
            return 200, {"watchUrl": None}

        return None  # unknown action

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _auto_register(
        self,
        workflow_id: str,
        body: dict,
        run_name: Optional[str] = None,
    ) -> None:
        """Register the workflow if it slipped through POST /trace/create."""
        if self._reg.is_registered(workflow_id):
            return
        if run_name is None:
            run_name = (body.get("workflow") or {}).get("runName", "") or workflow_id
        batch_id = self._extract_batch_id(run_name) if run_name else workflow_id
        self._reg.register(workflow_id, batch_id, run_name)

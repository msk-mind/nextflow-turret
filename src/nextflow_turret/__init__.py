"""nextflow-turret — in-process Nextflow Tower shim.

Public surface
--------------
State / registry::

    from nextflow_turret import WorkflowState, WorkflowRegistry
    from nextflow_turret import (
        register_workflow, is_registered,
        update_progress, mark_complete,
        get_progress, get_all_states, evict_old,
        workflow_id_for_batch,
    )

HTTP routing::

    from nextflow_turret import TowerRouter
    from nextflow_turret import user_info_response, trace_create_response

Persistence::

    from nextflow_turret import RunStore, PersistentWorkflowRegistry

Utilities::

    from nextflow_turret import tower_process_to_slurm_name
"""

from .state import (
    WorkflowState,
    WorkflowRegistry,
    default_registry,
    workflow_id_for_batch,
    register_workflow,
    is_registered,
    update_progress,
    mark_complete,
    get_progress,
    get_state,
    get_all_states,
    evict_old,
)

from .handlers import (
    TowerRouter,
    user_info_response,
    trace_create_response,
)

from .db.store import RunStore
from .server.registry import PersistentWorkflowRegistry

from .utils import tower_process_to_slurm_name

__all__ = [
    # State
    "WorkflowState",
    "WorkflowRegistry",
    "default_registry",
    "workflow_id_for_batch",
    # Convenience singletons
    "register_workflow",
    "is_registered",
    "update_progress",
    "mark_complete",
    "get_progress",
    "get_state",
    "get_all_states",
    "evict_old",
    # HTTP routing
    "TowerRouter",
    "user_info_response",
    "trace_create_response",
    # Persistence
    "RunStore",
    "PersistentWorkflowRegistry",
    # Utilities
    "tower_process_to_slurm_name",
]

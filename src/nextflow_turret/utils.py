"""Utility helpers for Nextflow / Tower interoperability."""
from __future__ import annotations


def tower_process_to_slurm_name(tower_process: str) -> str:
    """Convert a Tower process name to its SLURM job name prefix.

    Nextflow maps ``SCOPE:PROCESS_NAME`` to ``nf-SCOPE_PROCESS_NAME_(N)``.
    This function replicates the colon → underscore substitution so that
    Tower process names can be correlated with SLURM job names (after
    stripping the ``nf-`` prefix and ``(N)`` task-index suffix).

    Example
    -------
    ::

        tower_process_to_slurm_name(
            "MUSSEL:EXTRACT_FEATURES:TESSELLATE_FEATURIZE_BATCH"
        )
        # → "MUSSEL_EXTRACT_FEATURES_TESSELLATE_FEATURIZE_BATCH"
    """
    return tower_process.replace(":", "_").rstrip("_")

"""Utility helpers for Nextflow / Tower interoperability."""
from __future__ import annotations

import csv
import os
import re
import subprocess


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


# ---------------------------------------------------------------------------
# Nextflow log / trace parsing
# ---------------------------------------------------------------------------

_NF_PROGRESS_RE = re.compile(r'\[\s*(\d+)%\]\s+(\d+)\s+of\s+(\d+)')
_NF_EXECUTOR_RE = re.compile(r'^executor\s*>\s*\S+\s*\((\d+)\)', re.MULTILINE)
_NF_WARN_RE     = re.compile(r'^WARN[:\s](.+)', re.MULTILINE)
_NF_ERROR_RE    = re.compile(r"^ERROR ~ Error executing process > '([^']+)'", re.MULTILINE)
_NF_KILLED_RE   = re.compile(r'Killing running tasks \((\d+)\)', re.MULTILINE)

_TRACE_DONE_STATUSES = {"COMPLETED", "CACHED"}
_TRACE_FAIL_STATUSES = {"FAILED", "ABORTED"}


def trace_path_for_log(log_path: str) -> str:
    """Return the expected trace TSV path for a given batch log path.

    Nextflow's ``-with-trace <log>.log`` companion file is ``<log>.trace.tsv``.
    """
    base = log_path[: -len(".log")] if log_path.endswith(".log") else log_path
    return base + ".trace.tsv"


def parse_nf_trace(trace_path: str) -> dict:
    """Parse a Nextflow trace TSV file and return task counts + failure details.

    The trace file (written by ``-with-trace``) is a tab-separated file
    updated in real-time as tasks complete.  It is more reliable than log
    regex for accurate done/failed counts and provides the exact process
    name and exit code of failed tasks.

    Returns a dict with:
        completed  (int)  – tasks with status COMPLETED
        cached     (int)  – tasks with status CACHED (resumed from cache)
        failed     (int)  – tasks with status FAILED or ABORTED
        total      (int)  – all finished tasks seen so far
        failures   (list) – up to 5 dicts {name, exit, hash} for failed tasks
    """
    result: dict = {"completed": 0, "cached": 0, "failed": 0, "total": 0, "failures": []}
    if not trace_path or not os.path.exists(trace_path):
        return result
    try:
        with open(trace_path, newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                status = (row.get("status") or "").strip().upper()
                if status in _TRACE_DONE_STATUSES:
                    result["cached" if status == "CACHED" else "completed"] += 1
                    result["total"] += 1
                elif status in _TRACE_FAIL_STATUSES:
                    result["failed"] += 1
                    result["total"] += 1
                    if len(result["failures"]) < 5:
                        result["failures"].append({
                            "name": (row.get("name") or "").strip(),
                            "exit": (row.get("exit") or "").strip(),
                            "hash": (row.get("hash") or "").strip(),
                        })
    except Exception:
        pass
    return result


def parse_nf_log(log_path: str) -> dict:
    """Parse a Nextflow batch stdout log and return a dict with all useful metrics.

    When a companion trace file (``<batch>.trace.tsv``) exists, failure
    counts and details are taken from it instead of log regex — giving the
    exact process name and exit code rather than a best-effort regex match.
    Progress (done/total) still comes from the log's progress line because
    the trace only contains *finished* tasks and cannot report the total
    expected task count while the run is still in progress.

    Returns a dict with keys:
        progress     – {pct, done, total} or None
        slurm_jobs   – current active SLURM job count or None
        warn_count   – number of WARN lines
        last_warn    – last warning text (truncated)
        error_count  – number of failed tasks
        first_error  – description of first error
        killed       – number of tasks killed by signal
        failures     – [{name, exit, hash}] from trace (up to 5)
    """
    result = {
        "progress": None,
        "slurm_jobs": None,
        "warn_count": 0,
        "last_warn": None,
        "error_count": 0,
        "first_error": None,
        "killed": None,
        "failures": [],
    }
    if not log_path:
        return result
    try:
        with open(log_path, "rb") as f:
            text = f.read().decode("utf-8", errors="replace")

        prog_matches = _NF_PROGRESS_RE.findall(text)
        if prog_matches:
            pct_s, done_s, total_s = max(prog_matches, key=lambda m: int(m[2]))
            result["progress"] = {"pct": int(pct_s), "done": int(done_s), "total": int(total_s)}

        slurm_matches = _NF_EXECUTOR_RE.findall(text)
        if slurm_matches:
            result["slurm_jobs"] = int(slurm_matches[-1])

        warns = _NF_WARN_RE.findall(text)
        result["warn_count"] = len(warns)
        if warns:
            result["last_warn"] = warns[-1].strip()[:120]

        killed = _NF_KILLED_RE.findall(text)
        if killed:
            result["killed"] = int(killed[-1])

        trace = parse_nf_trace(trace_path_for_log(log_path))
        if trace["failed"] > 0:
            result["error_count"] = trace["failed"]
            result["failures"] = trace["failures"]
            if trace["failures"]:
                f0 = trace["failures"][0]
                result["first_error"] = f"{f0['name']} (exit {f0['exit']})"
        else:
            errors = _NF_ERROR_RE.findall(text)
            result["error_count"] = len(errors)
            if errors:
                result["first_error"] = errors[0].strip()[:120]
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# SLURM / task failure helpers
# ---------------------------------------------------------------------------

def parse_elapsed_s(elapsed: str) -> int | None:
    """Parse an elapsed time string (HH:MM:SS, D-HH:MM:SS, or M:SS) to seconds."""
    try:
        if "-" in elapsed:
            days, rest = elapsed.split("-", 1)
            d = int(days)
        else:
            rest, d = elapsed, 0
        parts = rest.split(":")
        if len(parts) == 3:
            return d * 86400 + int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        if len(parts) == 2:
            return d * 86400 + int(parts[0]) * 60 + int(parts[1])
    except Exception:
        pass
    return None


def classify_task_failure(work_dir: str, exit_code: str, slurm_state: str = "") -> str:
    """Classify a failed Nextflow task by SLURM state, exit code, and ``.command.err``.

    Returns a short string label suitable for bucketing failure types in a
    dashboard, e.g. ``"oom_gpu"``, ``"oom_host"``, ``"sigterm"``,
    ``"disk_full"``, ``"s3_error"``, ``"python_error"``, ``"exit_N"``,
    ``"unknown"``.
    """
    code = exit_code.split(":")[0] if ":" in exit_code else exit_code
    try:
        code_i = int(code)
    except ValueError:
        code_i = -1

    if slurm_state.startswith("CANCEL") or code_i == 143:
        return "sigterm"

    err_text = ""
    try:
        err_path = os.path.join(work_dir, ".command.err")
        with open(err_path, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - 4096))
            err_text = f.read().decode("utf-8", errors="replace").lower()
    except Exception:
        pass

    if "cuda out of memory" in err_text or "cudaoutofmemoryerror" in err_text:
        return "oom_gpu"
    if "out of memory" in err_text or "oom-kill" in err_text or "cannot allocate memory" in err_text:
        return "oom_host"
    if code_i == 137:
        return "oom_host"
    if "no space left" in err_text or "disk quota" in err_text:
        return "disk_full"
    if "s3://" in err_text and ("error" in err_text or "exception" in err_text):
        return "s3_error"
    if "traceback" in err_text or "runtimeerror" in err_text or "valueerror" in err_text:
        return "python_error"
    if code_i == 1:
        return "error_exit1"
    if code_i > 0:
        return f"exit_{code_i}"
    return "unknown"

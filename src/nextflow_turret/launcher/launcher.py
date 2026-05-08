"""Pipeline launcher — submits ``nextflow run`` as a managed subprocess.

Each launch is assigned a *launch_id* (UUID).  The Nextflow process is spawned
with ``-with-tower`` pointing back at the local Turret server so that progress
is automatically tracked in the :class:`~nextflow_turret.state.WorkflowRegistry`.

The run name is set to ``dispatcher_{launch_id}`` (matching the existing
batch-id convention) unless the caller supplies an explicit ``run_name``.

Lifecycle
---------
::

    launcher = Launcher(tower_url="http://localhost:8000")

    launch_id = launcher.submit(
        pipeline="https://github.com/org/pipeline",
        revision="main",
        params={"input": "s3://bucket/samplesheet.csv"},
        profile="slurm",
    )

    # poll status
    record = launcher.get(launch_id)
    print(record.status, record.log_path)

    # cancel
    launcher.cancel(launch_id)
"""
from __future__ import annotations

import enum
import json
import os
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


class LaunchStatus(str, enum.Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCEEDED = "succeeded"
    FAILED    = "failed"
    CANCELLED = "cancelled"


@dataclass
class LaunchRecord:
    """Metadata for a single pipeline launch."""
    launch_id:    str
    pipeline:     str
    revision:     Optional[str]
    params:       dict
    profile:      Optional[str]
    work_dir:     Optional[str]
    run_name:     str
    status:       LaunchStatus
    pid:          Optional[int]
    exit_code:    Optional[int]
    log_path:     str
    submitted_at: float
    started_at:   Optional[float]
    finished_at:  Optional[float]

    def as_dict(self) -> dict:
        return {
            "launch_id":    self.launch_id,
            "pipeline":     self.pipeline,
            "revision":     self.revision,
            "params":       self.params,
            "profile":      self.profile,
            "work_dir":     self.work_dir,
            "run_name":     self.run_name,
            "status":       self.status.value,
            "pid":          self.pid,
            "exit_code":    self.exit_code,
            "log_path":     self.log_path,
            "submitted_at": self.submitted_at,
            "started_at":   self.started_at,
            "finished_at":  self.finished_at,
        }


class Launcher:
    """Spawn and track ``nextflow run`` subprocesses.

    Parameters
    ----------
    tower_url:
        URL of the local Turret server.  Injected as ``-with-tower`` so the
        launched pipeline reports back automatically.
    log_dir:
        Directory to write per-launch ``.log`` files.  Defaults to
        ``./turret-logs``.
    nextflow_bin:
        Path to the ``nextflow`` executable.  Defaults to ``nextflow`` (found
        on PATH).
    """

    def __init__(
        self,
        tower_url:        str           = "http://localhost:8000",
        log_dir:          str | Path    = "turret-logs",
        nextflow_bin:     str           = "nextflow",
        default_work_dir: Optional[str] = None,
        default_profile:  Optional[str] = None,
    ) -> None:
        self._tower_url       = tower_url
        self._log_dir         = Path(log_dir)
        self._nextflow        = nextflow_bin
        self._default_work_dir = default_work_dir
        self._default_profile  = default_profile
        self._lock            = threading.Lock()
        self._records:        dict[str, LaunchRecord] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(
        self,
        pipeline: str,
        revision: Optional[str] = None,
        params: Optional[dict] = None,
        profile: Optional[str] = None,
        work_dir: Optional[str] = None,
        run_name: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
    ) -> str:
        """Submit a pipeline run.  Returns the *launch_id*.

        *profile* and *work_dir* fall back to the launcher-level defaults
        (``default_profile`` / ``default_work_dir``) when not supplied.
        """
        launch_id = str(uuid.uuid4())
        run_name  = run_name or f"dispatcher_{launch_id}"
        log_path  = str(self._log_dir / f"{launch_id}.log")

        resolved_profile  = profile  if profile  is not None else self._default_profile
        resolved_work_dir = work_dir if work_dir is not None else self._default_work_dir

        record = LaunchRecord(
            launch_id    = launch_id,
            pipeline     = pipeline,
            revision     = revision,
            params       = params or {},
            profile      = resolved_profile,
            work_dir     = resolved_work_dir,
            run_name     = run_name,
            status       = LaunchStatus.PENDING,
            pid          = None,
            exit_code    = None,
            log_path     = log_path,
            submitted_at = time.time(),
            started_at   = None,
            finished_at  = None,
        )

        with self._lock:
            self._records[launch_id] = record

        thread = threading.Thread(
            target=self._run,
            args=(launch_id,),
            daemon=True,
            name=f"turret-launch-{launch_id[:8]}",
        )
        thread.start()
        return launch_id

    def get(self, launch_id: str) -> Optional[LaunchRecord]:
        with self._lock:
            return self._records.get(launch_id)

    def list_all(self) -> list[LaunchRecord]:
        with self._lock:
            return list(self._records.values())

    def cancel(self, launch_id: str) -> bool:
        """Send SIGTERM to the Nextflow process.  Returns True if a signal was sent."""
        with self._lock:
            record = self._records.get(launch_id)
        if record is None or record.pid is None:
            return False
        if record.status not in (LaunchStatus.PENDING, LaunchStatus.RUNNING):
            return False
        try:
            os.kill(record.pid, 15)  # SIGTERM
            with self._lock:
                record.status = LaunchStatus.CANCELLED
            return True
        except ProcessLookupError:
            return False

    def read_log(self, launch_id: str, tail: Optional[int] = None) -> str:
        """Return log contents for a launch.  Pass *tail* to limit to last N lines."""
        with self._lock:
            record = self._records.get(launch_id)
        if record is None:
            return ""
        try:
            lines = Path(record.log_path).read_text(errors="replace").splitlines()
            if tail is not None:
                lines = lines[-tail:]
            return "\n".join(lines)
        except FileNotFoundError:
            return ""

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_cmd(self, record: LaunchRecord) -> list[str]:
        cmd = [
            self._nextflow, "run", record.pipeline,
            "-with-tower", self._tower_url,
            "-name",       record.run_name,
        ]
        if record.revision:
            cmd += ["-revision", record.revision]
        if record.profile:
            cmd += ["-profile", record.profile]
        if record.work_dir:
            cmd += ["-work-dir", record.work_dir]
        if record.params:
            for key, val in record.params.items():
                cmd += [f"--{key}", str(val)]
        return cmd

    def _run(self, launch_id: str) -> None:
        with self._lock:
            record = self._records[launch_id]

        self._log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        cmd = self._build_cmd(record)

        with self._lock:
            record.status     = LaunchStatus.RUNNING
            record.started_at = time.time()

        try:
            # Create the log file with owner-read-only permissions before writing
            log_path = Path(record.log_path)
            log_path.touch(mode=0o600, exist_ok=True)
            # Log the command without param values to avoid leaking secrets
            param_summary = ", ".join(f"--{k}" for k in record.params) if record.params else "(none)"
            with open(log_path, "w") as log_fh:
                log_fh.write(f"# pipeline: {record.pipeline}\n")
                log_fh.write(f"# params:   {param_summary}\n\n")
                log_fh.flush()
                proc = subprocess.Popen(
                    cmd,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            with self._lock:
                record.pid = proc.pid

            exit_code = proc.wait()

            with self._lock:
                record.exit_code   = exit_code
                record.finished_at = time.time()
                if record.status != LaunchStatus.CANCELLED:
                    record.status = (
                        LaunchStatus.SUCCEEDED if exit_code == 0
                        else LaunchStatus.FAILED
                    )
        except Exception as exc:  # nextflow not found, permissions, etc.
            with self._lock:
                record.status      = LaunchStatus.FAILED
                record.finished_at = time.time()
            try:
                # Truncate the error message to avoid leaking sensitive system info
                err_msg = str(exc)[:200]
                Path(record.log_path).open("a").write(f"\n# ERROR: {err_msg}\n")
            except Exception:
                pass

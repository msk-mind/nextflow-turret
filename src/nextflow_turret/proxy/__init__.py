"""Resilient Tower proxy: keeps Nextflow runs alive while turret restarts.

Nextflow aborts a run whose ``-with-tower`` endpoint stops answering. Point
Nextflow at this proxy instead of at turret: it forwards every request to
turret, and answers ``200 {}`` while turret is unreachable, so restarting or
upgrading turret never kills a running pipeline. Events sent while turret is
down are dropped; the next progress update brings the run's state back.

Usage::

    turret-proxy --upstream http://localhost:8000 --port 8001
    nextflow run main.nf -with-tower http://localhost:8001
"""
from __future__ import annotations

from .server import forward, make_handler, make_server, main

__all__ = ["forward", "make_handler", "make_server", "main"]

"""HTTP server for :mod:`nextflow_turret.proxy`."""
from __future__ import annotations

import argparse
import http.client
import json
import logging
import socket
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ..handlers import _default_run_name_to_batch_id, trace_create_response
from ..state import workflow_id_for_batch

log = logging.getLogger("nextflow_turret.proxy")

_HOP_BY_HOP = {"host", "connection", "transfer-encoding"}
# Gateway answers while turret restarts behind a reverse proxy: treat as down.
_UNAVAILABLE_STATUSES = {502, 503, 504}


class _Upstream:
    """Where to forward to, plus up/down state so outages are logged once."""

    def __init__(self, url: str, timeout: float):
        parsed = urllib.parse.urlparse(url or "")
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"upstream must be an http(s) URL like http://host:8000, got {url!r}")
        self.scheme = parsed.scheme
        self.netloc = parsed.netloc
        self.prefix = parsed.path.rstrip("/")
        self.timeout = timeout
        self._down = False
        self._lock = threading.Lock()

    def connection(self) -> http.client.HTTPConnection:
        cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        return cls(self.netloc, timeout=self.timeout)

    def mark(self, down: bool, reason: str = "") -> None:
        with self._lock:
            if down and not self._down:
                log.warning("upstream %s://%s unavailable (%s); answering Nextflow locally",
                            self.scheme, self.netloc, reason)
            elif not down and self._down:
                log.info("upstream %s://%s is back", self.scheme, self.netloc)
            self._down = down


def _fallback(method: str, path: str, body: bytes) -> bytes:
    """What turret would have answered, as far as Nextflow needs it.

    ``POST /trace/create`` must return a ``workflowId`` or Nextflow aborts the
    run; return the deterministic ID turret assigns for the run name, so events
    sent once turret is back reach the same run.
    """
    if method == "POST" and path.split("?", 1)[0].rstrip("/").endswith("/trace/create"):
        try:
            run_name = json.loads(body or b"{}").get("runName") or ""
        except (ValueError, AttributeError):
            run_name = ""
        batch_id = _default_run_name_to_batch_id(run_name) if run_name else ""
        if batch_id:
            return json.dumps(trace_create_response(workflow_id_for_batch(batch_id))).encode()
    return b"{}"


def forward(upstream: _Upstream, method: str, path: str,
            headers: dict, body: bytes) -> tuple[int, bytes]:
    """Forward one request; return ``(status, body)``.

    When the upstream is unreachable or answers with a gateway error, return
    ``200`` and a local fallback body (see :func:`_fallback`).
    """
    try:
        conn = upstream.connection()
        try:
            fwd_headers = {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}
            conn.request(method, upstream.prefix + path, body=body or None, headers=fwd_headers)
            resp = conn.getresponse()
            status, data = resp.status, resp.read()
        finally:
            conn.close()
    except (OSError, http.client.HTTPException, socket.timeout) as exc:
        upstream.mark(True, f"{type(exc).__name__}: {exc}")
        return 200, _fallback(method, path, body)
    if status in _UNAVAILABLE_STATUSES:
        upstream.mark(True, f"HTTP {status}")
        return 200, _fallback(method, path, body)
    upstream.mark(False)
    return status, data


def make_handler(upstream_url: str, timeout: float = 5.0):
    """A request handler class forwarding to ``upstream_url``."""
    upstream = _Upstream(upstream_url, timeout)

    class ProxyHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # no per-request access log
            pass

        def _proxy(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else b""
            status, data = forward(upstream, self.command, self.path, dict(self.headers), body)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_PUT = do_DELETE = _proxy

    return ProxyHandler


def make_server(upstream_url: str, host: str = "localhost", port: int = 8001,
                timeout: float = 5.0) -> ThreadingHTTPServer:
    """A threaded proxy server (concurrent Nextflow runs don't wait on each other)."""
    server = ThreadingHTTPServer((host, port), make_handler(upstream_url, timeout=timeout))
    server.daemon_threads = True
    return server


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Forward Nextflow -with-tower traffic to turret; answer locally while turret is down.")
    parser.add_argument("--upstream", default="http://localhost:8000",
                        help="turret URL to forward to, http(s)://host[:port][/prefix] "
                             "(default: http://localhost:8000)")
    parser.add_argument("--host", default="localhost", help="Host to bind to (default: localhost)")
    parser.add_argument("--port", type=int, default=8001, help="Port to listen on (default: 8001)")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="Seconds to wait for the upstream before answering locally (default: 5)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
    try:
        server = make_server(args.upstream, host=args.host, port=args.port, timeout=args.timeout)
    except ValueError as exc:
        parser.error(str(exc))
    log.info("Tower proxy on %s:%d forwarding to %s", args.host, args.port, args.upstream)
    server.serve_forever()


if __name__ == "__main__":
    main()

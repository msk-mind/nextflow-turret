"""HTTP server for :mod:`nextflow_turret.proxy`."""
from __future__ import annotations

import argparse
import http.client
import logging
import socket
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("nextflow_turret.proxy")

_HOP_BY_HOP = {"host", "connection", "transfer-encoding"}


def forward(upstream_netloc: str, method: str, path: str,
            headers: dict, body: bytes, timeout: float = 5.0) -> tuple[int, bytes]:
    """Forward one request to the upstream; return ``(status, body)``.

    Returns ``(200, b"{}")`` when the upstream is unreachable.
    """
    try:
        conn = http.client.HTTPConnection(upstream_netloc, timeout=timeout)
        try:
            fwd_headers = {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}
            conn.request(method, path, body=body or None, headers=fwd_headers)
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()
    except (OSError, http.client.HTTPException, socket.timeout):
        return 200, b"{}"


def make_handler(upstream_url: str, timeout: float = 5.0):
    """A request handler class forwarding to ``upstream_url``."""
    upstream_netloc = urllib.parse.urlparse(upstream_url).netloc

    class ProxyHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # no per-request access log
            pass

        def _proxy(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else b""
            status, data = forward(upstream_netloc, self.command, self.path,
                                   dict(self.headers), body, timeout=timeout)
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
        description="Forward Nextflow -with-tower traffic to turret; answer 200 while turret is down.")
    parser.add_argument("--upstream", default="http://localhost:8000",
                        help="turret URL to forward to (default: http://localhost:8000)")
    parser.add_argument("--host", default="localhost", help="Host to bind to (default: localhost)")
    parser.add_argument("--port", type=int, default=8001, help="Port to listen on (default: 8001)")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="Seconds to wait for the upstream before answering 200 (default: 5)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
    log.info("Tower proxy on %s:%d forwarding to %s", args.host, args.port, args.upstream)
    make_server(args.upstream, host=args.host, port=args.port, timeout=args.timeout).serve_forever()


if __name__ == "__main__":
    main()

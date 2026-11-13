"""Tests for the resilient Tower proxy (nextflow_turret.proxy)."""
from __future__ import annotations

import json
import socket
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from nextflow_turret.proxy import make_server


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def _serve(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


@pytest.fixture()
def upstream():
    """A stand-in turret that echoes the method, path and body it got."""
    seen = []

    class Echo(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _reply(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode() if length else ""
            seen.append((self.command, self.path, body))
            data = json.dumps({"method": self.command, "path": self.path}).encode()
            self.send_response(201)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_PUT = do_DELETE = _reply

    server = _serve(HTTPServer(("localhost", _free_port()), Echo))
    yield f"http://localhost:{server.server_address[1]}", seen
    server.shutdown()
    server.server_close()


def _request(url, method, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read() or b"{}")


def test_forwards_requests_to_the_upstream(upstream):
    upstream_url, seen = upstream
    proxy = _serve(make_server(upstream_url, host="localhost", port=_free_port()))
    try:
        base = f"http://localhost:{proxy.server_address[1]}"
        status, body = _request(f"{base}/trace/create", "POST", {"runName": "r1"})
        assert status == 201
        assert body == {"method": "POST", "path": "/trace/create"}
        assert seen == [("POST", "/trace/create", '{"runName": "r1"}')]
    finally:
        proxy.shutdown()
        proxy.server_close()


def test_answers_ok_when_the_upstream_is_down():
    """A turret restart must not make Nextflow abort the run."""
    dead = f"http://localhost:{_free_port()}"
    proxy = _serve(make_server(dead, host="localhost", port=_free_port()))
    try:
        base = f"http://localhost:{proxy.server_address[1]}"
        for method in ("POST", "PUT", "GET"):
            status, body = _request(f"{base}/trace/abc/progress", method, {"progress": {}} if method != "GET" else None)
            assert (status, body) == (200, {})
    finally:
        proxy.shutdown()
        proxy.server_close()

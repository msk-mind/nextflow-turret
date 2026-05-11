"""Mixin for stdlib BaseHTTPRequestHandler providing Tower trace API POST/PUT handlers."""
from __future__ import annotations


class TowerHandlerMixin:
    """Mixin adding Tower trace API POST and PUT methods to BaseHTTPRequestHandler.

    The concrete handler class must:

    - Set ``tower_router`` as a class attribute (a :class:`TowerRouter` instance).
    - Implement ``_read_body() -> dict`` — reads and parses the JSON request body.
    - Implement ``_send_json(data, status=200)`` — serialises and sends a JSON response.

    Optionally override ``log_tower(method, path, status, note)`` to emit request logs.
    The default implementation is a no-op.
    """

    tower_router: object  # TowerRouter instance; set on the concrete Handler class

    def log_tower(self, method: str, path: str, status: int, note: str = "") -> None:
        """Hook called after each Tower request is handled. No-op by default."""

    def do_POST(self) -> None:
        """Handle Tower trace API POST requests from Nextflow."""
        path = self.path.split("?")[0]
        try:
            body = self._read_body()
            result = self.tower_router.handle_post(path, body)
            if result is not None:
                status, resp_body = result
                note = f"run={body.get('runName', '')} wid={resp_body.get('workflowId', '')}"
                self.log_tower("POST", path, status, note)
                self._send_json(resp_body, status=status)
            else:
                self.log_tower("POST", path, 404)
                self.send_response(404)
                self.end_headers()
        except Exception as exc:
            try:
                self._send_json({"error": str(exc)}, status=500)
            except Exception:
                pass

    def do_PUT(self) -> None:
        """Handle Tower trace API PUT requests from Nextflow."""
        path = self.path.split("?")[0]
        try:
            body = self._read_body()
            result = self.tower_router.handle_put(path, body)
            if result is not None:
                status, resp_body = result
                parts = path.strip("/").split("/")
                action = parts[2] if len(parts) == 3 else ""
                self.log_tower("PUT", path, status, action)
                self._send_json(resp_body, status=status)
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as exc:
            try:
                self._send_json({"error": str(exc)}, status=500)
            except Exception:
                pass

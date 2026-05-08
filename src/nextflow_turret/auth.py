"""Authentication for Nextflow Turret.

Three modes (configured via ``turret.toml``):

``none`` (default)
    No authentication.  All routes are public.

``basic``
    Username + bcrypt-hashed password.  Browser users go through a login
    form; API clients may use ``Authorization: Basic <base64>`` headers.
    Both methods set a signed session cookie on success.

``oidc``
    OpenID Connect / OAuth 2.0 code flow.  Works with any provider that
    supports OIDC discovery (Google, GitHub via GitHub Apps, Okta,
    Keycloak, Entra ID, …).  Requires ``pip install nextflow-turret[oidc]``
    (adds ``authlib`` and ``httpx``).

Public paths (always exempt from auth)
---------------------------------------
- ``/user-info``  and  ``/trace/*``   (Nextflow Tower protocol)
- ``/auth/*``                          (login / callback / logout)
- ``/docs``, ``/redoc``, ``/openapi.json``

Generating a password hash
--------------------------
::

    turret hash-password

Security notes
--------------
- ``next`` redirect parameters are validated to be same-origin (relative path
  only); absolute URLs to other hosts are rejected.
- OIDC state is a random token stored in the session; the post-login redirect
  URL is stored separately in the session to prevent open-redirect via ``state``.
- A missing ``session_secret`` triggers a loud warning on startup.
"""
from __future__ import annotations

import base64
import logging
import secrets
import warnings
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------

class AuthMode(str, Enum):
    NONE  = "none"
    BASIC = "basic"
    OIDC  = "oidc"


@dataclass
class BasicAuthConfig:
    username:      str
    password_hash: str   # bcrypt hash; use `turret hash-password` to generate


@dataclass
class OIDCConfig:
    client_id:     str
    client_secret: str
    discovery_url: str                              # .well-known/openid-configuration URL
    redirect_uri:  Optional[str]    = None          # auto-detected from request if omitted
    scopes:        list[str]        = field(default_factory=lambda: ["openid", "email", "profile"])


@dataclass
class AuthConfig:
    mode:           AuthMode             = AuthMode.NONE
    session_secret: str                  = ""
    basic:          Optional[BasicAuthConfig] = None
    oidc:           Optional[OIDCConfig]      = None

    def __post_init__(self):
        if self.mode != AuthMode.NONE and not self.session_secret:
            # Auto-generate a random secret when not configured.
            # Sessions will be invalidated on every server restart — warn loudly.
            self.session_secret = secrets.token_urlsafe(32)
            warnings.warn(
                "[turret] WARNING: auth.session_secret is not set in turret.toml. "
                "A random secret was generated; all sessions will be lost on every "
                "server restart. Set a persistent secret to avoid this.",
                stacklevel=2,
            )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def make_password_hash(password: str) -> str:
    """Return a bcrypt hash string suitable for ``[auth.basic] password_hash``."""
    try:
        import bcrypt
    except ImportError:
        raise RuntimeError("bcrypt is required: pip install nextflow-turret[auth]")
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _verify_bcrypt(password: str, hashed: str) -> bool:
    try:
        import bcrypt
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except Exception:
        return False


def is_safe_next_url(url: str) -> bool:
    """Return True only for relative (same-origin) redirect URLs.

    Absolute URLs pointing to other hosts are rejected to prevent open-redirect
    attacks via the ``?next=`` parameter.
    """
    if not url:
        return False
    parsed = urlparse(url)
    # Reject anything with a scheme (http://, https://, javascript:, etc.)
    # or a netloc (//evil.com style protocol-relative URLs)
    return not parsed.scheme and not parsed.netloc


def safe_next_url(url: Optional[str], default: str = "/") -> str:
    """Return *url* if it is a safe relative URL, otherwise *default*."""
    if url and is_safe_next_url(url):
        return url
    return default


# ---------------------------------------------------------------------------
# Auth manager
# ---------------------------------------------------------------------------

# Paths that are always public (no auth required).
_PUBLIC_PREFIXES = (
    "/user-info",
    "/trace/",
    "/auth/",
    "/docs",
    "/redoc",
    "/openapi.json",
)


class AuthManager:
    """Central auth handler — one instance per application."""

    def __init__(self, config: AuthConfig):
        self.config = config
        self._oauth  = None   # lazy-init for OIDC

    @property
    def enabled(self) -> bool:
        return self.config.mode != AuthMode.NONE

    # ------------------------------------------------------------------
    # Session helpers
    # ------------------------------------------------------------------

    @staticmethod
    def get_user(request: Request) -> Optional[dict]:
        """Return the session user dict, or ``None`` if not authenticated."""
        try:
            return request.session.get("user") or None
        except AssertionError:
            # SessionMiddleware not configured (e.g. auth disabled)
            return None

    @staticmethod
    def set_user(request: Request, user: dict) -> None:
        request.session["user"] = user

    @staticmethod
    def clear_user(request: Request) -> None:
        request.session.pop("user", None)

    # ------------------------------------------------------------------
    # Basic auth helpers
    # ------------------------------------------------------------------

    def verify_basic_header(self, request: Request) -> Optional[dict]:
        """Parse and verify an ``Authorization: Basic`` header.

        Returns a user dict on success, ``None`` otherwise.
        """
        if self.config.mode != AuthMode.BASIC or self.config.basic is None:
            return None
        header = request.headers.get("Authorization", "")
        if not header.lower().startswith("basic "):
            return None
        try:
            decoded  = base64.b64decode(header[6:]).decode()
            username, _, password = decoded.partition(":")
        except Exception:
            return None

        bc = self.config.basic
        if (
            secrets.compare_digest(username, bc.username)
            and _verify_bcrypt(password, bc.password_hash)
        ):
            return {"username": username, "auth_method": "basic"}
        return None

    def verify_basic_credentials(self, username: str, password: str) -> Optional[dict]:
        """Verify a username/password pair (login form).

        Returns a user dict on success, ``None`` otherwise.
        """
        if self.config.basic is None:
            return None
        bc = self.config.basic
        if (
            secrets.compare_digest(username, bc.username)
            and _verify_bcrypt(password, bc.password_hash)
        ):
            return {"username": username, "auth_method": "form"}
        return None

    # ------------------------------------------------------------------
    # OIDC helpers
    # ------------------------------------------------------------------

    def get_oauth(self):
        """Lazy-init the authlib OAuth client for OIDC."""
        if self._oauth is not None:
            return self._oauth
        try:
            from authlib.integrations.starlette_client import OAuth  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "OIDC auth requires authlib and httpx. "
                "Install with: pip install nextflow-turret[oidc]"
            ) from exc

        oc    = self.config.oidc
        oauth = OAuth()
        oauth.register(
            name="oidc",
            client_id=oc.client_id,
            client_secret=oc.client_secret,
            server_metadata_url=oc.discovery_url,
            client_kwargs={"scope": " ".join(oc.scopes)},
        )
        self._oauth = oauth
        return oauth


# ---------------------------------------------------------------------------
# ASGI middleware
# ---------------------------------------------------------------------------

class AuthMiddleware(BaseHTTPMiddleware):
    """HTTP middleware that enforces authentication for all non-public routes.

    - API paths (``/api/*``) get a JSON ``401`` response.
    - UI paths get a redirect to ``/auth/login?next=<original-url>``.
    - Basic-auth header is accepted for API callers.
    """

    def __init__(self, app, auth_manager: AuthManager):
        super().__init__(app)
        self.auth = auth_manager

    async def dispatch(self, request: Request, call_next) -> Response:
        if not self.auth.enabled:
            return await call_next(request)

        path = request.url.path

        # Always allow public paths
        if any(path == p or path.startswith(p) for p in _PUBLIC_PREFIXES):
            return await call_next(request)

        # Already authenticated via session
        if self.auth.get_user(request):
            return await call_next(request)

        # Basic-auth header accepted for any mode=basic request
        if self.auth.config.mode == AuthMode.BASIC:
            user = self.auth.verify_basic_header(request)
            if user:
                self.auth.set_user(request, user)
                return await call_next(request)

        # Not authenticated — decide response type
        if path.startswith("/api/"):
            return JSONResponse(
                {"detail": "Authentication required"},
                status_code=401,
                headers={"WWW-Authenticate": "Basic realm='Nextflow Turret'"},
            )

        # Only include path+query in the `next` param, never full URL (prevents open-redirect)
        from urllib.parse import quote
        next_path = request.url.path
        if request.url.query:
            next_path += "?" + request.url.query
        return RedirectResponse(f"/auth/login?next={quote(next_path, safe='')}", status_code=302)

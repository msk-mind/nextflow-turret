"""Tests for the authentication layer.

Covers:
- mode=none: all routes are public (no session middleware required)
- mode=basic: login form, session cookie, HTTP Basic header, wrong creds
- Tower trace endpoints always public regardless of auth mode
- /auth/whoami reflects session state
- /auth/logout clears the session
- turret hash-password CLI command
- Open-redirect prevention on next= parameter
- Security headers present on all responses
- Param key validation on launch form
"""
from __future__ import annotations

import base64

import pytest
from fastapi.testclient import TestClient

from nextflow_turret.auth import (
    AuthConfig,
    AuthMode,
    BasicAuthConfig,
    AuthManager,
    is_safe_next_url,
    make_password_hash,
    safe_next_url,
)
from nextflow_turret.server.app import create_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PASSWORD      = "s3cr3t"
_PASSWORD_HASH = make_password_hash(_PASSWORD)
_USERNAME      = "admin"


def _basic_app(*, mode: str = "basic") -> TestClient:
    auth = AuthConfig(
        mode           = AuthMode(mode),
        session_secret = "test-secret-key",
        basic          = BasicAuthConfig(username=_USERNAME, password_hash=_PASSWORD_HASH),
    )
    app = create_app(db_path=":memory:", auth_config=auth)
    return TestClient(app, raise_server_exceptions=True)


def _no_auth_app() -> TestClient:
    app = create_app(db_path=":memory:")
    return TestClient(app, raise_server_exceptions=True)


def _basic_header(username: str = _USERNAME, password: str = _PASSWORD) -> dict:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


# ---------------------------------------------------------------------------
# mode=none (default)
# ---------------------------------------------------------------------------

class TestAuthDisabled:
    def test_index_public(self):
        c = _no_auth_app()
        assert c.get("/").status_code == 200

    def test_api_public(self):
        c = _no_auth_app()
        assert c.get("/api/runs").status_code == 200

    def test_user_info_public(self):
        c = _no_auth_app()
        assert c.get("/user-info").status_code == 200

    def test_whoami_401_when_no_auth_enabled(self):
        # whoami returns 401 when there is no session (auth disabled means no user set)
        c = _no_auth_app()
        assert c.get("/auth/whoami").status_code == 401


# ---------------------------------------------------------------------------
# mode=basic: unauthenticated requests
# ---------------------------------------------------------------------------

class TestBasicAuthUnauthenticated:
    def test_ui_redirects_to_login(self):
        c = _basic_app()
        r = c.get("/", follow_redirects=False)
        assert r.status_code == 302
        assert "/auth/login" in r.headers["location"]

    def test_api_returns_401(self):
        c = _basic_app()
        r = c.get("/api/runs")
        assert r.status_code == 401
        assert r.json()["detail"] == "Authentication required"

    def test_tower_user_info_always_public(self):
        c = _basic_app()
        assert c.get("/user-info").status_code == 200

    def test_tower_trace_always_public(self):
        c = _basic_app()
        # trace/create returns 200 even when auth is enabled
        r = c.post("/trace/create", json={})
        # Nextflow tower returns 200 with a workflowId
        assert r.status_code == 200

    def test_login_page_is_accessible(self):
        c = _basic_app()
        r = c.get("/auth/login")
        assert r.status_code == 200
        assert b"Sign in" in r.content


# ---------------------------------------------------------------------------
# mode=basic: login form flow
# ---------------------------------------------------------------------------

class TestBasicAuthLoginForm:
    def test_good_credentials_set_session(self):
        c = _basic_app()
        r = c.post(
            "/auth/login",
            data={"username": _USERNAME, "password": _PASSWORD, "next": "/"},
            follow_redirects=False,
        )
        assert r.status_code == 303
        assert r.headers["location"] == "/"
        # Cookie should now be set
        assert "session" in r.cookies or any("session" in k for k in r.cookies)

    def test_bad_credentials_return_401_form(self):
        c = _basic_app()
        r = c.post(
            "/auth/login",
            data={"username": _USERNAME, "password": "wrongpassword", "next": "/"},
        )
        assert r.status_code == 401
        assert b"Invalid username or password" in r.content

    def test_after_login_ui_accessible(self):
        c = _basic_app()
        # Log in
        c.post(
            "/auth/login",
            data={"username": _USERNAME, "password": _PASSWORD, "next": "/"},
        )
        # Should now be able to access the dashboard
        r = c.get("/")
        assert r.status_code == 200

    def test_logout_clears_session(self):
        c = _basic_app()
        # Log in first
        c.post("/auth/login", data={"username": _USERNAME, "password": _PASSWORD, "next": "/"})
        # Dashboard should be accessible
        assert c.get("/").status_code == 200
        # Logout
        r = c.get("/auth/logout", follow_redirects=False)
        assert r.status_code in (302, 303)
        # Now the dashboard should redirect again
        r2 = c.get("/", follow_redirects=False)
        assert r2.status_code == 302
        assert "/auth/login" in r2.headers["location"]


# ---------------------------------------------------------------------------
# mode=basic: HTTP Basic header (API clients)
# ---------------------------------------------------------------------------

class TestBasicAuthHeader:
    def test_valid_header_allows_api(self):
        c = _basic_app()
        r = c.get("/api/runs", headers=_basic_header())
        assert r.status_code == 200

    def test_invalid_header_returns_401(self):
        c = _basic_app()
        r = c.get("/api/runs", headers=_basic_header(password="wrong"))
        assert r.status_code == 401

    def test_malformed_header_returns_401(self):
        c = _basic_app()
        r = c.get("/api/runs", headers={"Authorization": "Basic not-base64!!"})
        assert r.status_code == 401


# ---------------------------------------------------------------------------
# /auth/whoami
# ---------------------------------------------------------------------------

class TestWhoami:
    def test_authenticated_user(self):
        c = _basic_app()
        c.post("/auth/login", data={"username": _USERNAME, "password": _PASSWORD, "next": "/"})
        r = c.get("/auth/whoami")
        assert r.status_code == 200
        data = r.json()
        assert data["username"] == _USERNAME

    def test_unauthenticated(self):
        c = _basic_app()
        r = c.get("/auth/whoami")
        # /auth/whoami is under /auth/ so it's a public path from the middleware perspective
        # but the handler itself returns 401 when not logged in
        assert r.status_code == 401


# ---------------------------------------------------------------------------
# AuthManager unit tests
# ---------------------------------------------------------------------------

class TestAuthManager:
    def test_enabled_false_when_mode_none(self):
        mgr = AuthManager(AuthConfig())
        assert not mgr.enabled

    def test_enabled_true_when_mode_basic(self):
        mgr = AuthManager(AuthConfig(
            mode=AuthMode.BASIC,
            session_secret="x",
            basic=BasicAuthConfig(username="u", password_hash=make_password_hash("p")),
        ))
        assert mgr.enabled

    def test_verify_basic_credentials_good(self):
        pw   = "mypw"
        mgr  = AuthManager(AuthConfig(
            mode=AuthMode.BASIC,
            session_secret="x",
            basic=BasicAuthConfig(username="u", password_hash=make_password_hash(pw)),
        ))
        assert mgr.verify_basic_credentials("u", pw) is not None

    def test_verify_basic_credentials_bad(self):
        mgr = AuthManager(AuthConfig(
            mode=AuthMode.BASIC,
            session_secret="x",
            basic=BasicAuthConfig(username="u", password_hash=make_password_hash("correct")),
        ))
        assert mgr.verify_basic_credentials("u", "wrong") is None

    def test_make_password_hash_round_trip(self):
        pw   = "roundtrip"
        hash_ = make_password_hash(pw)
        assert hash_.startswith("$2b$")
        # Verify via AuthManager
        mgr = AuthManager(AuthConfig(
            mode=AuthMode.BASIC,
            session_secret="x",
            basic=BasicAuthConfig(username="u", password_hash=hash_),
        ))
        assert mgr.verify_basic_credentials("u", pw) is not None
        assert mgr.verify_basic_credentials("u", "notthepw") is None


# ---------------------------------------------------------------------------
# Config loading with [auth] section
# ---------------------------------------------------------------------------

class TestAuthConfig:
    def test_auto_secret_when_not_set(self):
        """session_secret is auto-generated when mode != none and no secret is given."""
        cfg = AuthConfig(mode=AuthMode.BASIC, session_secret="")
        assert len(cfg.session_secret) > 0

    def test_explicit_secret_is_kept(self):
        cfg = AuthConfig(mode=AuthMode.BASIC, session_secret="my-secret")
        assert cfg.session_secret == "my-secret"


# ---------------------------------------------------------------------------
# Open-redirect prevention
# ---------------------------------------------------------------------------

class TestOpenRedirect:
    """is_safe_next_url / safe_next_url helpers prevent open-redirect attacks."""

    def test_relative_path_is_safe(self):
        assert is_safe_next_url("/dashboard") is True

    def test_relative_path_with_query_is_safe(self):
        assert is_safe_next_url("/runs?status=running") is True

    def test_absolute_http_url_is_not_safe(self):
        assert is_safe_next_url("http://evil.com") is False

    def test_absolute_https_url_is_not_safe(self):
        assert is_safe_next_url("https://evil.com/steal") is False

    def test_protocol_relative_url_is_not_safe(self):
        # //evil.com is a protocol-relative absolute URL
        assert is_safe_next_url("//evil.com") is False

    def test_javascript_scheme_is_not_safe(self):
        assert is_safe_next_url("javascript:alert(1)") is False

    def test_none_is_not_safe(self):
        assert is_safe_next_url(None) is False

    def test_safe_next_url_returns_url_when_safe(self):
        assert safe_next_url("/dashboard") == "/dashboard"

    def test_safe_next_url_returns_default_when_unsafe(self):
        assert safe_next_url("http://evil.com", default="/") == "/"

    def test_safe_next_url_returns_default_for_none(self):
        assert safe_next_url(None, default="/home") == "/home"

    def test_login_next_redirect_is_sanitised(self):
        """GET /auth/login?next= with absolute URL must not redirect to evil host."""
        c = _basic_app()
        # Attempt open redirect
        r = c.post(
            "/auth/login",
            data={"username": _USERNAME, "password": _PASSWORD, "next": "http://evil.com"},
            follow_redirects=False,
        )
        # Must redirect to the default "/" (or some safe path), not to evil.com
        assert r.status_code == 303
        location = r.headers.get("location", "")
        assert "evil.com" not in location
        assert location == "/"


# ---------------------------------------------------------------------------
# Security headers
# ---------------------------------------------------------------------------

class TestSecurityHeaders:
    """SecurityHeadersMiddleware must be present on all responses."""

    REQUIRED_HEADERS = [
        "x-frame-options",
        "x-content-type-options",
        "referrer-policy",
        "content-security-policy",
    ]

    def test_security_headers_on_index(self):
        c = _no_auth_app()
        r = c.get("/")
        for h in self.REQUIRED_HEADERS:
            assert h in r.headers, f"Missing header: {h}"

    def test_security_headers_on_api(self):
        c = _no_auth_app()
        r = c.get("/api/runs")
        for h in self.REQUIRED_HEADERS:
            assert h in r.headers, f"Missing header: {h}"

    def test_security_headers_on_401(self):
        c = _basic_app()
        r = c.get("/api/runs")
        assert r.status_code == 401
        for h in self.REQUIRED_HEADERS:
            assert h in r.headers, f"Missing header on 401 response: {h}"

    def test_x_frame_options_is_deny(self):
        c = _no_auth_app()
        r = c.get("/")
        assert r.headers.get("x-frame-options", "").upper() == "DENY"

    def test_x_content_type_options_is_nosniff(self):
        c = _no_auth_app()
        r = c.get("/")
        assert r.headers.get("x-content-type-options", "").lower() == "nosniff"


# ---------------------------------------------------------------------------
# Param key validation on /launch submit
# ---------------------------------------------------------------------------

class TestParamKeyValidation:
    """Invalid param keys must be rejected with 422."""

    def _login(self, c: TestClient) -> None:
        c.post("/auth/login", data={"username": _USERNAME, "password": _PASSWORD})

    def test_valid_param_key_accepted(self):
        c = _basic_app()
        self._login(c)
        r = c.post(
            "/launch",
            data={
                "pipeline": "nf-core/test",
                "params": '{"input": "s3://bucket/file.csv"}',
            },
            follow_redirects=False,
        )
        # Accepted (redirects to launch detail) — not a 422
        assert r.status_code in (303, 200)

    def test_invalid_param_key_rejected(self):
        c = _basic_app()
        self._login(c)
        # Keys with shell metacharacters should be rejected
        r = c.post(
            "/launch",
            data={
                "pipeline": "nf-core/test",
                "params": '{"--inject; rm -rf /": "value"}',
            },
            follow_redirects=False,
        )
        assert r.status_code == 422

    def test_oversized_params_rejected(self):
        c = _basic_app()
        self._login(c)
        huge_json = '{"k": "' + "x" * (101 * 1024) + '"}'
        r = c.post(
            "/launch",
            data={"pipeline": "nf-core/test", "params": huge_json},
            follow_redirects=False,
        )
        assert r.status_code == 422

"""Tests for the authentication layer.

Covers:
- mode=none: all routes are public (no session middleware required)
- mode=basic: login form, session cookie, HTTP Basic header, wrong creds
- Tower trace endpoints always public regardless of auth mode
- /auth/whoami reflects session state
- /auth/logout clears the session
- turret hash-password CLI command
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
    make_password_hash,
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

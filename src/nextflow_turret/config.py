"""Configuration loading for Nextflow Turret.

Config is read from the first file found in this order:

1. Path given via ``--config`` CLI flag
2. ``./turret.toml``  (project-level, next to the DB)
3. ``~/.config/turret/config.toml``  (user-level)

Values from the config file are used as defaults.  CLI flags always take
precedence over config-file values.

Example ``turret.toml``::

    [server]
    host    = "0.0.0.0"
    port    = 8000
    db      = "/data/turret.db"
    log_dir = "/data/turret-logs"

    [launcher]
    nextflow        = "/opt/nextflow/nextflow"
    work_dir        = "/scratch/nf-work"
    default_profile = "slurm"

    [auth]
    mode           = "basic"           # "none" | "basic" | "oidc"
    session_secret = "change-me"       # used to sign session cookies

    [auth.basic]
    username      = "admin"
    password_hash = "$2b$12$..."       # use `turret hash-password` to generate

    [auth.oidc]
    client_id     = "..."
    client_secret = "..."
    discovery_url = "https://accounts.google.com/.well-known/openid-configuration"
    redirect_uri  = "http://localhost:8000/auth/callback"  # optional; auto-detected
    scopes        = ["openid", "email", "profile"]
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .auth import AuthConfig, AuthMode, BasicAuthConfig, OIDCConfig

if sys.version_info >= (3, 11):  # noqa: E402  (after dataclass imports)
    import tomllib
else:
    try:
        import tomllib          # type: ignore[no-redef]
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError as exc:
            raise ImportError(
                "Python <3.11 requires the 'tomli' package to read TOML config files. "
                "Install it with: pip install tomli"
            ) from exc

_DEFAULT_CONFIG_PATHS = [
    Path("turret.toml"),
    Path.home() / ".config" / "turret" / "config.toml",
]


@dataclass
class TurretConfig:
    """Merged configuration (config file + CLI overrides)."""

    # [server]
    host:    str = "0.0.0.0"
    port:    int = 8000
    db:      str = "turret.db"
    log_dir: str = "turret-logs"

    # [launcher]
    nextflow:        str           = "nextflow"
    work_dir:        Optional[str] = None
    default_profile: Optional[str] = None

    # [auth]
    auth: AuthConfig = field(default_factory=AuthConfig)

    @property
    def tower_url(self) -> str:
        """URL Nextflow should use to reach this server."""
        host = self.host if self.host != "0.0.0.0" else "localhost"
        return f"http://{host}:{self.port}"


def _find_config_file(explicit: Optional[str] = None) -> Optional[Path]:
    """Return the first existing config file path, or None."""
    candidates = [Path(explicit)] if explicit else _DEFAULT_CONFIG_PATHS
    for p in candidates:
        if p.is_file():
            return p
    return None


def load_config(config_path: Optional[str] = None) -> tuple[TurretConfig, Optional[Path]]:
    """Load and return a :class:`TurretConfig` plus the resolved file path.

    Parameters
    ----------
    config_path:
        Explicit path supplied by the user (e.g. ``--config /path/to/turret.toml``).
        If *None*, the default search order is used.

    Returns
    -------
    (config, resolved_path)
        *resolved_path* is ``None`` when no config file was found.
    """
    cfg    = TurretConfig()
    fpath  = _find_config_file(config_path)

    if fpath is None:
        return cfg, None

    with fpath.open("rb") as fh:
        data = tomllib.load(fh)

    server   = data.get("server",   {})
    launcher = data.get("launcher", {})

    # server section
    if "host"    in server: cfg.host    = str(server["host"])
    if "port"    in server: cfg.port    = int(server["port"])
    if "db"      in server: cfg.db      = str(server["db"])
    if "log_dir" in server: cfg.log_dir = str(server["log_dir"])

    # launcher section
    if "nextflow"        in launcher: cfg.nextflow        = str(launcher["nextflow"])
    if "work_dir"        in launcher: cfg.work_dir        = str(launcher["work_dir"])
    if "default_profile" in launcher: cfg.default_profile = str(launcher["default_profile"])

    # auth section
    auth_raw  = data.get("auth", {})
    auth_mode = AuthMode(auth_raw.get("mode", "none"))
    auth_secret = str(auth_raw.get("session_secret", ""))

    basic_cfg: Optional[BasicAuthConfig] = None
    oidc_cfg:  Optional[OIDCConfig]      = None

    basic_raw = auth_raw.get("basic", {})
    if basic_raw:
        basic_cfg = BasicAuthConfig(
            username      = str(basic_raw.get("username", "admin")),
            password_hash = str(basic_raw.get("password_hash", "")),
        )

    oidc_raw = auth_raw.get("oidc", {})
    if oidc_raw:
        oidc_cfg = OIDCConfig(
            client_id     = str(oidc_raw.get("client_id", "")),
            client_secret = str(oidc_raw.get("client_secret", "")),
            discovery_url = str(oidc_raw.get("discovery_url", "")),
            redirect_uri  = oidc_raw.get("redirect_uri") or None,
            scopes        = list(oidc_raw.get("scopes", ["openid", "email", "profile"])),
        )

    cfg.auth = AuthConfig(
        mode           = auth_mode,
        session_secret = auth_secret,
        basic          = basic_cfg,
        oidc           = oidc_cfg,
    )

    return cfg, fpath

"""Entry point: ``python -m nextflow_turret.server``

Config is read from ``turret.toml`` (current dir) or
``~/.config/turret/config.toml`` (user dir).  CLI flags always override
config-file values.

Example
-------
::

    # Start with defaults (reads turret.toml if present)
    turret

    # Override specific values
    turret --host 127.0.0.1 --port 9000 --db /data/turret.db

    # Explicit config file
    turret --config /etc/turret/prod.toml

    # Generate a bcrypt password hash for [auth.basic]
    turret hash-password mysecretpassword

Then point Nextflow at it::

    nextflow run main.nf -with-tower http://localhost:8000 -name dispatcher_mybatch
"""
import argparse
import sys

import uvicorn

from ..config import load_config
from .app import create_app

_UNSET = object()  # sentinel to detect "user did not pass this flag"


def main() -> None:
    # --- Phase 0: handle utility sub-commands (no server needed) ----------
    if len(sys.argv) >= 2 and sys.argv[1] == "hash-password":
        _cmd_hash_password(sys.argv[2:])
        return

    # --- Phase 1: peek at --config so we can load the file first ----------
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", default=None)
    known, _ = pre.parse_known_args()

    cfg, cfg_path = load_config(known.config)

    # --- Phase 2: full parser with config-file values as defaults ---------
    parser = argparse.ArgumentParser(
        prog="turret",
        description="Nextflow Turret — self-hosted Seqera Platform replacement",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config",   default=None,          metavar="PATH",
                        help="Path to turret.toml config file")
    parser.add_argument("--host",     default=cfg.host,
                        help="Bind host")
    parser.add_argument("--port",     type=int, default=cfg.port,
                        help="Bind port")
    parser.add_argument("--db",       default=cfg.db,        metavar="PATH",
                        help="SQLite database path")
    parser.add_argument("--log-dir",  default=cfg.log_dir,   metavar="PATH",
                        help="Directory for per-launch log files")
    parser.add_argument("--nextflow", default=cfg.nextflow,  metavar="PATH",
                        help="Path to nextflow executable")
    parser.add_argument("--work-dir", default=cfg.work_dir,  metavar="PATH",
                        help="Default work directory for launched pipelines")
    parser.add_argument("--profile",  default=cfg.default_profile,
                        help="Default Nextflow profile for launched pipelines")
    args = parser.parse_args()

    if cfg_path:
        print(f"[turret] loaded config: {cfg_path}")

    if cfg.auth.mode.value != "none":
        print(f"[turret] auth mode: {cfg.auth.mode.value}")

    tower_url = f"http://{'localhost' if args.host == '0.0.0.0' else args.host}:{args.port}"
    app = create_app(
        db_path          = args.db,
        tower_url        = tower_url,
        log_dir          = args.log_dir,
        nextflow_bin     = args.nextflow,
        default_work_dir = args.work_dir,
        default_profile  = args.profile,
        auth_config      = cfg.auth,
    )
    uvicorn.run(app, host=args.host, port=args.port)


def _cmd_hash_password(argv: list[str]) -> None:
    """``turret hash-password`` — print a bcrypt hash.

    The password is always read interactively from the terminal to avoid
    exposing it in shell history or process listings.  Passing a positional
    argument is intentionally not supported for this reason.
    """
    parser = argparse.ArgumentParser(
        prog="turret hash-password",
        description="Generate a bcrypt password hash for use in [auth.basic] password_hash",
    )
    parser.parse_args(argv)  # accepts no positional args; prints help on --help

    import getpass
    password = getpass.getpass("Password: ")
    confirm  = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: passwords do not match", file=sys.stderr)
        sys.exit(1)

    from ..auth import make_password_hash
    print(make_password_hash(password))


if __name__ == "__main__":
    main()


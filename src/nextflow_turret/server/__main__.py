"""Entry point: ``python -m nextflow_turret.server``

Example
-------
::

    # Start on default port 8000 with a local SQLite database
    python -m nextflow_turret.server

    # Custom host/port/db
    python -m nextflow_turret.server --host 127.0.0.1 --port 9000 --db /data/turret.db

Then point Nextflow at it::

    nextflow run main.nf -with-tower http://localhost:8000 -name dispatcher_mybatch
"""
import argparse

import uvicorn

from .app import create_app


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="nextflow-turret",
        description="Nextflow Turret — self-hosted Seqera Platform replacement",
    )
    parser.add_argument("--host",       default="0.0.0.0",              help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port",       type=int, default=8000,          help="Bind port (default: 8000)")
    parser.add_argument("--db",         default="turret.db",             help="SQLite database path")
    parser.add_argument("--log-dir",    default="turret-logs",           help="Directory for launch log files")
    parser.add_argument("--nextflow",   default="nextflow",              help="Path to nextflow executable")
    args = parser.parse_args()

    tower_url = f"http://{args.host if args.host != '0.0.0.0' else 'localhost'}:{args.port}"
    app = create_app(
        db_path=args.db,
        tower_url=tower_url,
        log_dir=args.log_dir,
        nextflow_bin=args.nextflow,
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

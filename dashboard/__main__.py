"""python -m dashboard  (reads DATA_DIR / DASHBOARD_HOST / DASHBOARD_PORT from .env)"""

import argparse
import os
from pathlib import Path

import uvicorn
from dotenv import dotenv_values

from dashboard.app import create_app


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m dashboard")
    ap.add_argument("--env", default=os.environ.get("BOT_ENV_FILE", ".env"))
    ap.add_argument("--data-dir")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    a = ap.parse_args()
    env = {k: v for k, v in (dotenv_values(a.env) if Path(a.env).exists() else {}).items() if v is not None}
    env.update({k: v for k, v in os.environ.items()})
    data_dir = a.data_dir or env.get("DATA_DIR") or "./data"
    host = a.host or env.get("DASHBOARD_HOST") or "127.0.0.1"
    port = a.port or int(env.get("DASHBOARD_PORT") or 8050)
    print(f"dashboard: http://{host}:{port}  (data: {Path(data_dir).resolve()})")
    uvicorn.run(create_app(Path(data_dir)), host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()

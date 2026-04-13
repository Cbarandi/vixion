"""Arranque del servidor ASGI (uvicorn)."""

from __future__ import annotations

import os
from pathlib import Path


def _load_project_env() -> None:
    """Carga `.env` en la raíz del repo si existe (no sobreescribe variables ya definidas)."""
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    if not env_path.is_file():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        os.environ[key] = val


def main() -> None:
    _load_project_env()
    import uvicorn

    uvicorn.run(
        "vixion.api.main:app",
        host=os.environ.get("VIXION_API_HOST", "127.0.0.1"),
        port=int(os.environ.get("VIXION_API_PORT", "8001")),
        log_level=os.environ.get("UVICORN_LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()

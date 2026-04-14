#!/usr/bin/env python3
r"""
Outcome Engine — slice 1: contexto de mercado por corrida de narrativas.

Se ejecuta tras ``persist_narrative_history`` y usa el mismo ``run_id`` que el
snapshot de narrativas (derivado de ``saved_at`` del último ``narratives_*.json``).

Salida: ``data/outcomes/market_context/market_context_<run_id>.json`` (UTF-8, JSON atómico).

Precios spot BTC/ETH vs USD vía API pública CoinGecko (sin API key). Si la red
falla o el rate limit bloquea, ``btc_usd`` / ``eth_usd`` pueden ser ``null`` y
``fetch_status`` refleja el estado — el pipeline no debe abortar.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from vixion.utils.run_ids import run_id_from_saved_at

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_NARRATIVES_DIR = PROJECT_ROOT / "data" / "narratives"
OUT_DIR = PROJECT_ROOT / "data" / "outcomes" / "market_context"

COINGECKO_SIMPLE_PRICE = "https://api.coingecko.com/api/v3/simple/price"
FETCH_TIMEOUT_S = 15.0
ENV_SKIP = "VIXION_SKIP_MARKET_CONTEXT"


def find_latest_narratives_file() -> Path:
    files = sorted(
        DATA_NARRATIVES_DIR.glob("narratives_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No hay narratives_*.json en {DATA_NARRATIVES_DIR}. "
            "Ejecuta detect_narratives antes.",
        )
    return files[0]


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(text, encoding="utf-8", newline="\n")
        tmp.replace(path)
    except OSError:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
        raise


def fetch_btc_eth_usd_coingecko() -> tuple[float | None, float | None, str | None]:
    """
    Devuelve (btc_usd, eth_usd, error_message).
    error_message solo si ambos fallan o la respuesta es inválida.
    """
    try:
        res = requests.get(
            COINGECKO_SIMPLE_PRICE,
            params={"ids": "bitcoin,ethereum", "vs_currencies": "usd"},
            timeout=FETCH_TIMEOUT_S,
        )
        res.raise_for_status()
        data = res.json()
    except (OSError, requests.RequestException, ValueError) as exc:
        return None, None, str(exc)
    if not isinstance(data, dict):
        return None, None, "respuesta JSON inesperada"

    btc: float | None = None
    eth: float | None = None
    b = data.get("bitcoin")
    e = data.get("ethereum")
    if isinstance(b, dict):
        u = b.get("usd")
        if isinstance(u, (int, float)):
            btc = float(u)
    if isinstance(e, dict):
        u = e.get("usd")
        if isinstance(u, (int, float)):
            eth = float(u)

    err: str | None = None
    if btc is None and eth is None:
        err = "sin precios parseables en la respuesta"
    return btc, eth, err


def build_market_context_payload(
    *,
    run_id: str,
    narrative_saved_at: str,
    narratives_source_rel: str,
    btc_usd: float | None,
    eth_usd: float | None,
    fetch_error: str | None,
    price_provider: str,
) -> dict[str, Any]:
    if btc_usd is not None and eth_usd is not None:
        fetch_status = "ok"
    elif btc_usd is not None or eth_usd is not None:
        fetch_status = "partial"
    else:
        fetch_status = "unavailable"

    err_out: str | None = None
    if fetch_status == "unavailable":
        err_out = fetch_error or "unknown"

    return {
        "schema_version": 1,
        "run_id": run_id,
        "narrative_saved_at": narrative_saved_at,
        "narratives_source_file": narratives_source_rel,
        "market_context_saved_at": datetime.now(UTC).isoformat(),
        "price_provider": price_provider,
        "btc_usd": round(btc_usd, 8) if btc_usd is not None else None,
        "eth_usd": round(eth_usd, 8) if eth_usd is not None else None,
        "fetch_status": fetch_status,
        "fetch_error": err_out,
    }


def main() -> int:
    if (os.environ.get(ENV_SKIP) or "").strip().lower() in ("1", "true", "yes"):
        print(f"[persist_market_context] Omitido ({ENV_SKIP}=1).")
        return 0

    try:
        latest = find_latest_narratives_file()
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    try:
        payload = json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"[ERROR] No se pudo leer {latest}: {exc}", file=sys.stderr)
        return 1

    saved_at = payload.get("saved_at")
    if not isinstance(saved_at, str) or not saved_at.strip():
        print("[ERROR] Falta 'saved_at' en narrativas.", file=sys.stderr)
        return 1

    try:
        run_id = run_id_from_saved_at(saved_at)
    except (TypeError, ValueError) as exc:
        print(f"[ERROR] saved_at inválido: {exc}", file=sys.stderr)
        return 1

    out_path = OUT_DIR / f"market_context_{run_id}.json"
    if out_path.is_file():
        print(f"[persist_market_context] Ya existe {out_path.name}; sin cambios.")
        return 0

    try:
        latest_rel = str(latest.relative_to(PROJECT_ROOT))
    except ValueError:
        latest_rel = str(latest)

    btc, eth, err = fetch_btc_eth_usd_coingecko()
    if err is None and btc is None and eth is None:
        err = "sin precios parseables en la respuesta"

    mc = build_market_context_payload(
        run_id=run_id,
        narrative_saved_at=saved_at,
        narratives_source_rel=latest_rel,
        btc_usd=btc,
        eth_usd=eth,
        fetch_error=err,
        price_provider="coingecko_simple",
    )

    try:
        write_json_atomic(out_path, mc)
    except OSError as exc:
        print(f"[ERROR] No se pudo escribir {out_path}: {exc}", file=sys.stderr)
        return 1

    print(f"Market context: {out_path}")
    print(
        f"  BTC/USD={mc['btc_usd']}  ETH/USD={mc['eth_usd']}  "
        f"status={mc['fetch_status']}",
    )
    if mc["fetch_status"] == "unavailable":
        print(f"  [warn] {mc.get('fetch_error')}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

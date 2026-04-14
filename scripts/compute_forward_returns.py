#!/usr/bin/env python3
r"""
Outcome Engine — retornos forward desde snapshots de ``market_context``.

Solo lee JSON locales bajo ``data/outcomes/market_context/`` (sin APIs ni DB).
Los instantes se parsean con ``vixion.utils.run_ids.parse_saved_at_utc`` para
alinear criterios de tiempo con el resto del proyecto.

Lee todos los ``market_context_<run_id>.json``, ordena por ``narrative_saved_at``,
y para cada ancla toma el primer snapshot estrictamente posterior en el tiempo
con ``ts >= ancla + N`` días calendario (N ∈ {1,3,7}). Retorno simple:
(precio_objetivo / precio_ancla) - 1 por activo cuando hay precios.

Salida: ``data/outcomes/forward_returns/forward_returns_<run_id>.json`` por ancla.
Horizontes sin snapshot futuro → ``status: missing_future`` (exit 0).

Uso en pipeline tras ``persist_market_context`` o como script suelto (reprocesa todo).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from vixion.utils.run_ids import parse_saved_at_utc

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MARKET_CONTEXT_DIR = PROJECT_ROOT / "data" / "outcomes" / "market_context"
OUT_DIR = PROJECT_ROOT / "data" / "outcomes" / "forward_returns"

HORIZON_DAYS = (1, 3, 7)
HORIZON_KEYS = ("1d", "3d", "7d")

ENV_SKIP = "VIXION_SKIP_FORWARD_RETURNS"


def parse_iso_to_utc(iso_str: str) -> datetime:
    return parse_saved_at_utc(iso_str)


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


def _float_or_none(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_market_context_row(path: Path, payload: dict[str, Any]) -> dict[str, Any] | None:
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return None
    saved = payload.get("narrative_saved_at")
    if not isinstance(saved, str) or not saved.strip():
        return None
    try:
        ts = parse_iso_to_utc(saved)
    except (TypeError, ValueError):
        return None
    return {
        "run_id": run_id.strip(),
        "path": path,
        "ts": ts,
        "narrative_saved_at": saved.strip(),
        "btc_usd": _float_or_none(payload.get("btc_usd")),
        "eth_usd": _float_or_none(payload.get("eth_usd")),
        "raw": payload,
    }


def discover_snapshots(mc_dir: Path) -> list[dict[str, Any]]:
    if not mc_dir.is_dir():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(mc_dir.glob("market_context_*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        row = load_market_context_row(path, data)
        if row is not None:
            rows.append(row)
    rows.sort(key=lambda r: (r["ts"], r["run_id"]))
    return rows


def simple_return(anchor: float | None, target: float | None) -> tuple[float | None, str]:
    if anchor is None or target is None:
        return None, "missing_price"
    if anchor == 0.0:
        return None, "invalid_anchor_reference"
    return (target / anchor) - 1.0, "ok"


def pick_future_snapshot(
    sorted_rows: list[dict[str, Any]],
    anchor_idx: int,
    anchor_ts: datetime,
    horizon_days: int,
) -> dict[str, Any] | None:
    min_ts = anchor_ts + timedelta(days=horizon_days)
    for j in range(anchor_idx + 1, len(sorted_rows)):
        if sorted_rows[j]["ts"] >= min_ts:
            return sorted_rows[j]
    return None


def horizon_payload(
    horizon_key: str,
    horizon_days: int,
    anchor_btc: float | None,
    anchor_eth: float | None,
    future: dict[str, Any] | None,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "horizon_key": horizon_key,
        "horizon_calendar_days": horizon_days,
        "target_run_id": None,
        "target_narrative_saved_at": None,
        "target_btc_usd": None,
        "target_eth_usd": None,
        "btc_return": None,
        "eth_return": None,
        "status": "missing_future",
    }
    if future is None:
        return base

    tb = _float_or_none(future.get("btc_usd"))
    te = _float_or_none(future.get("eth_usd"))
    base["target_run_id"] = future["run_id"]
    base["target_narrative_saved_at"] = future["narrative_saved_at"]
    base["target_btc_usd"] = tb
    base["target_eth_usd"] = te

    br, st_b = simple_return(anchor_btc, tb)
    er, st_e = simple_return(anchor_eth, te)

    if br is not None:
        base["btc_return"] = round(br, 8)
    if er is not None:
        base["eth_return"] = round(er, 8)

    if br is not None and er is not None:
        base["status"] = "ok"
    elif br is None and er is None:
        if st_b == "invalid_anchor_reference" or st_e == "invalid_anchor_reference":
            base["status"] = "invalid_anchor_reference"
        else:
            base["status"] = "missing_price"
    else:
        base["status"] = "partial"

    return base


def build_forward_returns_document(
    anchor_row: dict[str, Any],
    sorted_rows: list[dict[str, Any]],
    anchor_idx: int,
    computed_at: str,
) -> dict[str, Any]:
    ab = anchor_row.get("btc_usd")
    ae = anchor_row.get("eth_usd")
    horizons_out: dict[str, Any] = {}
    for key, days in zip(HORIZON_KEYS, HORIZON_DAYS, strict=True):
        fut = pick_future_snapshot(sorted_rows, anchor_idx, anchor_row["ts"], days)
        horizons_out[key] = horizon_payload(key, days, ab, ae, fut)

    doc: dict[str, Any] = {
        "schema_version": 1,
        "anchor_run_id": anchor_row["run_id"],
        "anchor_narrative_saved_at": anchor_row["narrative_saved_at"],
        "anchor_btc_usd": ab,
        "anchor_eth_usd": ae,
        "computed_at": computed_at,
        "source": "market_context",
        "horizons": horizons_out,
    }
    # Atajos planos (misma información que ``horizons``)
    for hk in HORIZON_KEYS:
        h = horizons_out[hk]
        doc[f"btc_return_{hk}"] = h.get("btc_return")
        doc[f"eth_return_{hk}"] = h.get("eth_return")
    return doc


def compute_all_forward_returns(
    mc_dir: Path,
    out_dir: Path | None = None,
    *,
    now_iso: str | None = None,
) -> tuple[int, int]:
    """
    Genera un JSON por snapshot de mercado. Devuelve (escritos, omitidos_por_error).
    """
    dest = out_dir if out_dir is not None else OUT_DIR
    computed_at = now_iso or datetime.now(UTC).isoformat()
    rows = discover_snapshots(mc_dir)
    if not rows:
        return 0, 0

    written = 0
    skipped = 0
    for i, anchor in enumerate(rows):
        try:
            doc = build_forward_returns_document(anchor, rows, i, computed_at)
            out_path = dest / f"forward_returns_{anchor['run_id']}.json"
            write_json_atomic(out_path, doc)
            written += 1
        except OSError:
            skipped += 1
    return written, skipped


def main() -> int:
    if (os.environ.get(ENV_SKIP) or "").strip().lower() in ("1", "true", "yes"):
        print(f"[compute_forward_returns] Omitido ({ENV_SKIP}=1).")
        return 0

    w, sk = compute_all_forward_returns(MARKET_CONTEXT_DIR, OUT_DIR)
    print(f"Forward returns: {w} archivo(s) en {OUT_DIR}")
    if sk:
        print(f"  [warn] {sk} error(es) de escritura", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

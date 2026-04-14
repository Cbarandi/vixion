#!/usr/bin/env python3
r"""
Outcome Engine v2 — drawdown BTC v1 desde ``market_context``.

Misma línea temporal que ``compute_forward_returns``: snapshots ordenados por
``narrative_saved_at``. Para cada ancla y horizonte N días calendario, el
objetivo es el **mismo** snapshot futuro que forward (primer ``ts >= ancla + N``).

Drawdown v1 (solo BTC, respecto al precio ancla ``P0``):
- Ventana: todos los snapshots **estrictamente posteriores** al ancla hasta el
  objetivo del horizonte **inclusive** (``(ancla, target]`` en el tiempo).
- Por cada punto con ``btc_usd`` válido: retorno relativo al ancla ``(P/P0)-1``.
- ``btc_max_drawdown``: magnitud positiva del peor retroceso respecto al ancla,
  es decir ``max(0, -min(retornos))``. Si todo el trayecto está por encima o
  igual al ancla, el valor es ``0``.

Estados por horizonte:
- ``invalid_anchor_reference``: ``btc_usd`` del ancla ausente o cero.
- ``missing_future``: no hay snapshot futuro que cumpla el horizonte.
- ``missing_price``: hay objetivo pero ningún ``btc_usd`` válido en la ventana.
- ``ok``: al menos un precio válido en la ventana (el drawdown puede ser 0).

Salida: ``data/outcomes/drawdowns/drawdown_<run_id>.json`` por ancla.
Ejecutar tras ``compute_forward_returns`` en el pipeline. Omitir con
``VIXION_SKIP_COMPUTE_BTC_DRAWDOWN=1``.
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
OUT_DIR = PROJECT_ROOT / "data" / "outcomes" / "drawdowns"

HORIZON_DAYS = (1, 3, 7)
HORIZON_KEYS = ("1d", "3d", "7d")

ENV_SKIP = "VIXION_SKIP_COMPUTE_BTC_DRAWDOWN"


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


def window_rows_anchor_to_target(
    sorted_rows: list[dict[str, Any]],
    anchor_idx: int,
    target: dict[str, Any],
) -> list[dict[str, Any]]:
    """Snapshots con índice (anchor_idx, target_idx] (excluye ancla, incluye objetivo)."""
    target_idx = -1
    for j in range(anchor_idx + 1, len(sorted_rows)):
        if sorted_rows[j]["run_id"] == target["run_id"] and sorted_rows[j]["ts"] == target["ts"]:
            target_idx = j
            break
    if target_idx < 0:
        return []
    return sorted_rows[anchor_idx + 1 : target_idx + 1]


def btc_max_drawdown_vs_anchor(
    anchor_btc: float | None,
    window_rows: list[dict[str, Any]],
) -> tuple[float | None, str]:
    """
    Peor retroceso relativo al ancla en la ventana (magnitud >= 0).
    ``missing_price`` si no hay ningún ``btc_usd`` válido en la ventana.
    """
    if anchor_btc is None or anchor_btc == 0.0:
        return None, "invalid_anchor_reference"

    rels: list[float] = []
    for r in window_rows:
        p = r.get("btc_usd")
        if p is None:
            continue
        try:
            pv = float(p)
        except (TypeError, ValueError):
            continue
        if pv <= 0.0:
            continue
        rels.append((pv / anchor_btc) - 1.0)

    if not rels:
        return None, "missing_price"

    worst = min(rels)
    mdd = max(0.0, -worst)
    return round(mdd, 8), "ok"


def horizon_drawdown_payload(
    horizon_key: str,
    horizon_days: int,
    anchor_btc: float | None,
    sorted_rows: list[dict[str, Any]],
    anchor_idx: int,
    anchor_ts: datetime,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "horizon_key": horizon_key,
        "horizon_calendar_days": horizon_days,
        "target_run_id": None,
        "target_narrative_saved_at": None,
        "btc_max_drawdown": None,
        "status": "missing_future",
    }

    if anchor_btc is None or anchor_btc == 0.0:
        base["status"] = "invalid_anchor_reference"
        return base

    target = pick_future_snapshot(sorted_rows, anchor_idx, anchor_ts, horizon_days)
    if target is None:
        return base

    base["target_run_id"] = target["run_id"]
    base["target_narrative_saved_at"] = target.get("narrative_saved_at")

    window = window_rows_anchor_to_target(sorted_rows, anchor_idx, target)
    mdd, st = btc_max_drawdown_vs_anchor(anchor_btc, window)
    base["btc_max_drawdown"] = mdd
    base["status"] = st
    return base


def build_drawdown_document(
    anchor_row: dict[str, Any],
    sorted_rows: list[dict[str, Any]],
    anchor_idx: int,
    computed_at: str,
) -> dict[str, Any]:
    ab = anchor_row.get("btc_usd")
    horizons_out: dict[str, Any] = {}
    for key, days in zip(HORIZON_KEYS, HORIZON_DAYS, strict=True):
        horizons_out[key] = horizon_drawdown_payload(
            key,
            days,
            ab,
            sorted_rows,
            anchor_idx,
            anchor_row["ts"],
        )

    return {
        "schema_version": 1,
        "anchor_run_id": anchor_row["run_id"],
        "anchor_narrative_saved_at": anchor_row["narrative_saved_at"],
        "anchor_btc_usd": ab,
        "computed_at": computed_at,
        "source": "market_context",
        "horizons": horizons_out,
    }


def compute_all_drawdowns(
    mc_dir: Path,
    out_dir: Path | None = None,
    *,
    now_iso: str | None = None,
) -> tuple[int, int]:
    dest = out_dir if out_dir is not None else OUT_DIR
    computed_at = now_iso or datetime.now(UTC).isoformat()
    rows = discover_snapshots(mc_dir)
    if not rows:
        return 0, 0

    written = 0
    skipped = 0
    for i, anchor in enumerate(rows):
        try:
            doc = build_drawdown_document(anchor, rows, i, computed_at)
            out_path = dest / f"drawdown_{anchor['run_id']}.json"
            write_json_atomic(out_path, doc)
            written += 1
        except OSError:
            skipped += 1
    return written, skipped


def main() -> int:
    if (os.environ.get(ENV_SKIP) or "").strip().lower() in ("1", "true", "yes"):
        print(f"[compute_drawdowns] Omitido ({ENV_SKIP}=1).")
        return 0

    w, sk = compute_all_drawdowns(MARKET_CONTEXT_DIR, OUT_DIR)
    print(f"Drawdowns BTC: {w} archivo(s) en {OUT_DIR}")
    if sk:
        print(f"  [warn] {sk} error(es) de escritura", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

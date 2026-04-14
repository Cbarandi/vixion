#!/usr/bin/env python3
r"""
Outcome Engine — agregación de forward returns por narrativa (archivos locales).

Une snapshots de narrativas (``data/narrative_history/snapshots/<run_id>.json``)
con ``forward_returns_<run_id>.json``, opcionalmente ``drawdown_<run_id>.json`` y
``time_to_peak_<run_id>.json`` (métricas de mercado por corrida, compartidas por
todas las narrativas del snapshot), y ``lifecycle_<run_id>.json`` para
NEW/RISING por clave normalizada.

Salida: ``data/outcomes/narrative_aggregates/latest.json``
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SNAPSHOTS_DIR = PROJECT_ROOT / "data" / "narrative_history" / "snapshots"
FORWARD_DIR = PROJECT_ROOT / "data" / "outcomes" / "forward_returns"
DRAWDOWN_DIR = PROJECT_ROOT / "data" / "outcomes" / "drawdowns"
TIME_TO_PEAK_DIR = PROJECT_ROOT / "data" / "outcomes" / "time_to_peak"
LIFECYCLE_DIR = PROJECT_ROOT / "data" / "narrative_history" / "lifecycle"
OUT_FILE = PROJECT_ROOT / "data" / "outcomes" / "narrative_aggregates" / "latest.json"

ENV_SKIP = "VIXION_SKIP_NARRATIVE_AGGREGATES"

HORIZONS = ("1d", "3d", "7d")


def normalize_narrative_key(label: str) -> str:
    """Misma regla que persist_narrative_history: strip + espacios colapsados."""
    s = label.strip()
    return re.sub(r"\s+", " ", s)


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


def run_id_from_snapshot_path(path: Path) -> str | None:
    if path.suffix != ".json":
        return None
    stem = path.stem
    return stem if stem else None


def _path_for_report(p: Path) -> str:
    try:
        return str(p.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(p.resolve())


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def lifecycle_key_sets(lc: dict[str, Any]) -> tuple[set[str], set[str]]:
    new_k: set[str] = set()
    rising_k: set[str] = set()
    for item in lc.get("new") or []:
        if not isinstance(item, dict):
            continue
        k = item.get("narrative_key")
        if isinstance(k, str) and k.strip():
            new_k.add(normalize_narrative_key(k))
        else:
            n = item.get("narrative")
            if isinstance(n, str) and n.strip():
                new_k.add(normalize_narrative_key(n))
    for item in lc.get("rising") or []:
        if not isinstance(item, dict):
            continue
        k = item.get("narrative_key")
        if isinstance(k, str) and k.strip():
            rising_k.add(normalize_narrative_key(k))
        else:
            n = item.get("narrative")
            if isinstance(n, str) and n.strip():
                rising_k.add(normalize_narrative_key(n))
    return new_k, rising_k


def _accumulate(
    agg: dict[str, Any],
    field_sum: str,
    field_cnt: str,
    field_pos: str,
    value: float | None,
) -> None:
    if value is None:
        return
    agg[field_sum] = agg.get(field_sum, 0.0) + float(value)
    agg[field_cnt] = agg.get(field_cnt, 0) + 1
    if float(value) > 0.0:
        agg[field_pos] = agg.get(field_pos, 0) + 1


def _accumulate_scalar(agg: dict[str, Any], field_sum: str, field_cnt: str, value: float | None) -> None:
    """Suma / cuenta sin tasa positiva (drawdown, horas TTP, etc.)."""
    if value is None:
        return
    try:
        fv = float(value)
    except (TypeError, ValueError):
        return
    if not math.isfinite(fv):
        return
    agg[field_sum] = agg.get(field_sum, 0.0) + fv
    agg[field_cnt] = agg.get(field_cnt, 0) + 1


def _horizon_block(doc: dict[str, Any] | None, horizon: str) -> dict[str, Any] | None:
    if doc is None:
        return None
    hor = doc.get("horizons")
    if not isinstance(hor, dict):
        return None
    block = hor.get(horizon)
    return block if isinstance(block, dict) else None


def drawdown_max_for_horizon(doc: dict[str, Any] | None, horizon: str) -> float | None:
    bl = _horizon_block(doc, horizon)
    if bl is None or bl.get("status") != "ok":
        return None
    v = bl.get("btc_max_drawdown")
    if v is None:
        return None
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return None
    return fv if math.isfinite(fv) else None


def time_to_peak_hours_for_horizon(doc: dict[str, Any] | None, horizon: str) -> float | None:
    bl = _horizon_block(doc, horizon)
    if bl is None or bl.get("status") != "ok":
        return None
    v = bl.get("btc_time_to_peak_hours")
    if v is None:
        return None
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return None
    return fv if math.isfinite(fv) else None


def build_aggregate_payload(
    snapshots_dir: Path,
    forward_dir: Path,
    lifecycle_dir: Path,
    drawdown_dir: Path | None = None,
    time_to_peak_dir: Path | None = None,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    gen_at = generated_at or datetime.now(UTC).isoformat()
    per_key: dict[str, dict[str, Any]] = {}

    def ensure_key(k: str) -> dict[str, Any]:
        if k not in per_key:
            per_key[k] = {
                "narrative_key": k,
                "occurrences": 0,
                "runs_tagged_new": 0,
                "runs_tagged_rising": 0,
            }
            for h in HORIZONS:
                per_key[k][f"_sum_btc_{h}"] = 0.0
                per_key[k][f"_cnt_btc_{h}"] = 0
                per_key[k][f"_pos_btc_{h}"] = 0
                per_key[k][f"_sum_dd_{h}"] = 0.0
                per_key[k][f"_cnt_dd_{h}"] = 0
                per_key[k][f"_sum_ttp_{h}"] = 0.0
                per_key[k][f"_cnt_ttp_{h}"] = 0
        return per_key[k]

    dd_root = drawdown_dir if drawdown_dir is not None else DRAWDOWN_DIR
    ttp_root = time_to_peak_dir if time_to_peak_dir is not None else TIME_TO_PEAK_DIR

    runs_total = 0
    runs_with_forward = 0

    snap_paths = sorted(snapshots_dir.glob("*.json")) if snapshots_dir.is_dir() else []
    for spath in snap_paths:
        run_id = run_id_from_snapshot_path(spath)
        if not run_id:
            continue
        snap = load_json(spath)
        if snap is None:
            continue
        narratives = snap.get("narratives")
        if not isinstance(narratives, list):
            continue

        fr_path = forward_dir / f"forward_returns_{run_id}.json"
        fr = load_json(fr_path) if fr_path.is_file() else None
        if fr is not None:
            runs_with_forward += 1
        runs_total += 1

        dd_path = dd_root / f"drawdown_{run_id}.json"
        dd = load_json(dd_path) if dd_path.is_file() else None
        ttp_path = ttp_root / f"time_to_peak_{run_id}.json"
        ttp = load_json(ttp_path) if ttp_path.is_file() else None

        lc_path = lifecycle_dir / f"lifecycle_{run_id}.json"
        lc = load_json(lc_path) if lc_path.is_file() else None
        new_set, rising_set = lifecycle_key_sets(lc) if lc else (set(), set())

        keys_in_run: set[str] = set()
        for row in narratives:
            if not isinstance(row, dict):
                continue
            label = row.get("narrative")
            if not isinstance(label, str) or not label.strip():
                continue
            nk = normalize_narrative_key(label)
            if not nk:
                continue
            keys_in_run.add(nk)
            st = ensure_key(nk)
            st["occurrences"] += 1

            if fr is not None:
                for h in HORIZONS:
                    v = fr.get(f"btc_return_{h}")
                    if isinstance(v, bool):
                        continue
                    if v is None:
                        continue
                    try:
                        fv = float(v)
                    except (TypeError, ValueError):
                        continue
                    _accumulate(st, f"_sum_btc_{h}", f"_cnt_btc_{h}", f"_pos_btc_{h}", fv)

            if dd is not None:
                for h in HORIZONS:
                    dv = drawdown_max_for_horizon(dd, h)
                    _accumulate_scalar(st, f"_sum_dd_{h}", f"_cnt_dd_{h}", dv)

            if ttp is not None:
                for h in HORIZONS:
                    tv = time_to_peak_hours_for_horizon(ttp, h)
                    _accumulate_scalar(st, f"_sum_ttp_{h}", f"_cnt_ttp_{h}", tv)

        for nk in keys_in_run:
            if nk in new_set:
                ensure_key(nk)["runs_tagged_new"] += 1
            if nk in rising_set:
                ensure_key(nk)["runs_tagged_rising"] += 1

    narrative_rows: list[dict[str, Any]] = []
    for k, st in sorted(per_key.items(), key=lambda x: (-x[1]["occurrences"], x[0])):
        out: dict[str, Any] = {
            "narrative_key": k,
            "occurrences": st["occurrences"],
            "runs_tagged_new": st["runs_tagged_new"],
            "runs_tagged_rising": st["runs_tagged_rising"],
        }
        for h in HORIZONS:
            cnt = int(st.get(f"_cnt_btc_{h}", 0))
            ssum = float(st.get(f"_sum_btc_{h}", 0.0))
            pos = int(st.get(f"_pos_btc_{h}", 0))
            avg_key = f"avg_btc_return_{h}"
            rate_key = f"positive_rate_{h}"
            cnt_key = f"count_with_returns_{h}"
            out[cnt_key] = cnt
            out[avg_key] = round(ssum / cnt, 8) if cnt > 0 else None
            out[rate_key] = round(pos / cnt, 8) if cnt > 0 else None

            dd_cnt = int(st.get(f"_cnt_dd_{h}", 0))
            dd_sum = float(st.get(f"_sum_dd_{h}", 0.0))
            out[f"count_with_drawdown_{h}"] = dd_cnt
            out[f"avg_btc_max_drawdown_{h}"] = round(dd_sum / dd_cnt, 8) if dd_cnt > 0 else None

            ttp_cnt = int(st.get(f"_cnt_ttp_{h}", 0))
            ttp_sum = float(st.get(f"_sum_ttp_{h}", 0.0))
            out[f"count_with_time_to_peak_{h}"] = ttp_cnt
            out[f"avg_btc_time_to_peak_hours_{h}"] = round(ttp_sum / ttp_cnt, 8) if ttp_cnt > 0 else None

        narrative_rows.append(out)

    return {
        "schema_version": 1,
        "generated_at": gen_at,
        "source": {
            "snapshots_dir": _path_for_report(snapshots_dir),
            "forward_returns_dir": _path_for_report(forward_dir),
            "drawdowns_dir": _path_for_report(dd_root),
            "time_to_peak_dir": _path_for_report(ttp_root),
            "lifecycle_dir": _path_for_report(lifecycle_dir),
        },
        "runs_with_snapshots": runs_total,
        "runs_with_forward_returns": runs_with_forward,
        "narrative_count": len(narrative_rows),
        "narratives": narrative_rows,
    }


def main() -> int:
    if (os.environ.get(ENV_SKIP) or "").strip().lower() in ("1", "true", "yes"):
        print(f"[aggregate_narrative_outcomes] Omitido ({ENV_SKIP}=1).")
        return 0

    try:
        payload = build_aggregate_payload(SNAPSHOTS_DIR, FORWARD_DIR, LIFECYCLE_DIR)
        write_json_atomic(OUT_FILE, payload)
    except OSError as exc:
        print(f"[ERROR] No se pudo escribir {OUT_FILE}: {exc}", file=sys.stderr)
        return 1

    print(f"Agregado narrativo: {OUT_FILE}")
    print(
        f"  runs (snapshots): {payload['runs_with_snapshots']} · "
        f"con forward_returns: {payload['runs_with_forward_returns']} · "
        f"narrativas distintas: {payload['narrative_count']}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

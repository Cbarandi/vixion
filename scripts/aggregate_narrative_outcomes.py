#!/usr/bin/env python3
r"""
Outcome Engine — agregación de forward returns por narrativa (archivos locales).

Une snapshots de narrativas (``data/narrative_history/snapshots/<run_id>.json``)
con ``forward_returns_<run_id>.json`` y opcionalmente ``lifecycle_<run_id>.json``
para contar apariciones NEW/RISING por clave normalizada.

Salida: ``data/outcomes/narrative_aggregates/latest.json``
"""

from __future__ import annotations

import json
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


def build_aggregate_payload(
    snapshots_dir: Path,
    forward_dir: Path,
    lifecycle_dir: Path,
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
        return per_key[k]

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

            if fr is None:
                continue
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
        narrative_rows.append(out)

    return {
        "schema_version": 1,
        "generated_at": gen_at,
        "source": {
            "snapshots_dir": _path_for_report(snapshots_dir),
            "forward_returns_dir": _path_for_report(forward_dir),
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

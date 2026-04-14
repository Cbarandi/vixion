"""Series temporales compactas a partir de snapshots en ``data/narrative_history/snapshots``."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from vixion.ops.narrative_diff_movers import build_top_movers_from_diff


def normalize_narrative_key(label: str) -> str:
    """Misma regla que ``persist_narrative_history``: strip + espacios colapsados."""
    s = label.strip()
    return re.sub(r"\s+", " ", s)


def load_runs_index_entries(runs_jsonl: Path) -> list[dict[str, Any]]:
    if not runs_jsonl.is_file():
        return []
    try:
        raw = runs_jsonl.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    out: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def recent_runs_with_snapshots(
    project_root: Path,
    *,
    max_runs: int,
) -> list[dict[str, Any]]:
    """
    Últimas ``max_runs`` entradas del índice cuyo snapshot existe (orden cronológico
    creciente: más antigua → más reciente).
    """
    lim = max(1, min(int(max_runs), 20))
    idx = project_root / "data" / "narrative_history" / "runs.jsonl"
    entries = load_runs_index_entries(idx)
    valid: list[dict[str, Any]] = []
    for e in entries:
        rid = e.get("run_id")
        sp = e.get("snapshot_path")
        if not isinstance(rid, str) or not rid.strip():
            continue
        if not isinstance(sp, str) or not sp.strip():
            continue
        path = project_root / sp
        if path.is_file():
            valid.append(
                {
                    "run_id": rid.strip(),
                    "saved_at": e.get("saved_at") if isinstance(e.get("saved_at"), str) else None,
                    "snapshot_path": sp.strip(),
                }
            )
    if len(valid) <= lim:
        return valid
    return valid[-lim:]


def strength_map_from_snapshot(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Clave normalizada → display, strength, rank (1 = más fuerte en ese snapshot)."""
    narratives = payload.get("narratives")
    if not isinstance(narratives, list):
        return {}
    rows: list[tuple[str, str, float]] = []
    for r in narratives:
        if not isinstance(r, dict):
            continue
        lab = r.get("narrative")
        if not isinstance(lab, str) or not lab.strip():
            continue
        k = normalize_narrative_key(lab)
        if not k:
            continue
        try:
            s = float(r.get("narrative_strength") or 0.0)
        except (TypeError, ValueError):
            s = 0.0
        rows.append((k, lab.strip(), s))
    rows.sort(key=lambda t: (-t[2], t[0]))
    out: dict[str, dict[str, Any]] = {}
    rank = 0
    for k, lab, s in rows:
        if k in out:
            continue
        rank += 1
        out[k] = {
            "narrative": lab,
            "strength": round(s, 6),
            "rank": rank,
        }
    return out


def read_snapshot_maps(
    project_root: Path,
    runs: list[dict[str, Any]],
) -> list[dict[str, Any] | None]:
    """Un mapa por corrida (mismo orden que ``runs``); None si no legible."""
    maps: list[dict[str, Any] | None] = []
    for r in runs:
        sp = r.get("snapshot_path")
        if not isinstance(sp, str):
            maps.append(None)
            continue
        path = project_root / sp
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            maps.append(None)
            continue
        if not isinstance(data, dict):
            maps.append(None)
            continue
        maps.append(strength_map_from_snapshot(data))
    return maps


def pick_timeline_keys(
    movers_payload: dict[str, Any] | None,
    last_snapshot_map: dict[str, Any] | None,
    *,
    max_narratives: int,
) -> list[str]:
    keys: list[str] = []
    if movers_payload:
        for side in ("rising", "falling"):
            for row in movers_payload.get(side) or []:
                if not isinstance(row, dict):
                    continue
                k = row.get("narrative_key")
                if isinstance(k, str) and k.strip():
                    kk = k.strip()
                    if kk not in keys:
                        keys.append(kk)
                if len(keys) >= max_narratives:
                    return keys[:max_narratives]
    if last_snapshot_map:
        # Orden por strength desc (ya implícito en map iteration — re-sort)
        ranked = sorted(
            last_snapshot_map.items(),
            key=lambda kv: (-float(kv[1].get("strength") or 0.0), kv[0]),
        )
        for k, _ in ranked:
            if k not in keys:
                keys.append(k)
            if len(keys) >= max_narratives:
                break
    return keys[:max_narratives]


def build_snapshot_timelines_payload(
    project_root: Path,
    *,
    max_runs: int = 8,
    max_narratives: int = 6,
) -> dict[str, Any]:
    """
    Construye eje de corridas + una serie de puntos por narrativa relevante.

    Puntos alineados al eje: ``null`` si la narrativa no aparece en ese snapshot.
    """
    mr = max(3, min(int(max_runs), 15))
    mn = max(2, min(int(max_narratives), 10))

    runs = recent_runs_with_snapshots(project_root, max_runs=mr)
    if not runs:
        return {
            "runs": [],
            "timelines": [],
            "meta": {"empty_reason": "no_runs_index_or_snapshots"},
        }

    per_run_maps = read_snapshot_maps(project_root, runs)
    last_map = per_run_maps[-1] if per_run_maps else None

    diff_path = None
    diffs_dir = project_root / "data" / "narrative_history" / "diffs"
    if diffs_dir.is_dir():
        candidates = sorted(
            diffs_dir.glob("diff_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            diff_path = candidates[0]
    movers_inner: dict[str, Any] | None = None
    if diff_path and diff_path.is_file():
        try:
            raw_diff = json.loads(diff_path.read_text(encoding="utf-8"))
            if isinstance(raw_diff, dict):
                movers_inner = build_top_movers_from_diff(raw_diff, limit=5)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            pass

    keys = pick_timeline_keys(movers_inner, last_map, max_narratives=mn)

    axis = [
        {"run_id": r["run_id"], "saved_at": r.get("saved_at")}
        for r in runs
    ]

    timelines: list[dict[str, Any]] = []
    for k in keys:
        label = k
        points: list[dict[str, Any] | None] = []
        for m in per_run_maps:
            if m is None or k not in m:
                points.append(None)
                continue
            info = m[k]
            label = str(info.get("narrative") or k)
            points.append(
                {
                    "strength": info.get("strength"),
                    "rank": info.get("rank"),
                }
            )
        timelines.append(
            {
                "narrative_key": k,
                "narrative": label,
                "points": points,
            }
        )

    return {
        "runs": axis,
        "timelines": timelines,
        "meta": {
            "max_runs": mr,
            "max_narratives": mn,
            "run_count": len(axis),
            "diff_source": str(diff_path.relative_to(project_root))
            if diff_path
            else None,
        },
    }

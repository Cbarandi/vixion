"""Resumen compacto de Top Movers a partir del diff narrativo (artefacto JSON)."""

from __future__ import annotations

from typing import Any


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip())
        except ValueError:
            return None
    return None


def _as_int(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        try:
            return int(v.strip())
        except ValueError:
            return None
    return None


def _diff_counts(diff: dict[str, Any]) -> dict[str, int]:
    def n(key: str) -> int:
        v = diff.get(key)
        return len(v) if isinstance(v, list) else 0

    return {"added": n("added"), "removed": n("removed"), "changed": n("changed")}


def _normalize_changed_row(item: dict[str, Any]) -> dict[str, Any] | None:
    label = item.get("narrative")
    if not isinstance(label, str) or not label.strip():
        k = item.get("narrative_key")
        if isinstance(k, str) and k.strip():
            label = k.strip()
        else:
            return None

    nk = item.get("narrative_key")
    narrative_key = nk.strip() if isinstance(nk, str) and nk.strip() else label

    ds = _as_float(item.get("delta_strength"))
    if ds is None:
        return None

    cs = _as_float(item.get("current_strength"))
    ps = _as_float(item.get("previous_strength"))
    cr = _as_int(item.get("current_rank"))
    pr = _as_int(item.get("previous_rank"))

    return {
        "narrative": label.strip(),
        "narrative_key": narrative_key,
        "delta_strength": round(ds, 6),
        "current_strength": round(cs, 6) if cs is not None else None,
        "previous_strength": round(ps, 6) if ps is not None else None,
        "current_rank": cr,
        "previous_rank": pr,
    }


def build_top_movers_from_diff(diff: dict[str, Any], *, limit: int = 5) -> dict[str, Any]:
    """
    Extrae top subidas y top bajadas por ``delta_strength`` entre corridas (solo ``changed``).

    Entradas con Δ == 0 no aparecen en ninguna lista. Orden estable: |Δ|, luego ``narrative``.
    """
    lim = max(1, min(int(limit), 50))
    meta = {
        "diff_generated_at": diff.get("diff_generated_at"),
        "current_run_id": diff.get("current_run_id"),
        "previous_run_id": diff.get("previous_run_id"),
        "counts": _diff_counts(diff),
        "note": diff.get("note"),
    }

    changed = diff.get("changed")
    rows: list[dict[str, Any]] = []
    if isinstance(changed, list):
        for item in changed:
            if not isinstance(item, dict):
                continue
            row = _normalize_changed_row(item)
            if row is None:
                continue
            rows.append(row)

    rising_cand = [r for r in rows if r["delta_strength"] > 0]
    falling_cand = [r for r in rows if r["delta_strength"] < 0]

    rising_cand.sort(
        key=lambda r: (-float(r["delta_strength"]), str(r["narrative"])),
    )
    falling_cand.sort(
        key=lambda r: (float(r["delta_strength"]), str(r["narrative"])),
    )

    return {
        "meta": meta,
        "rising": rising_cand[:lim],
        "falling": falling_cand[:lim],
    }

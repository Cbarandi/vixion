#!/usr/bin/env python3
"""
Clasificación de ciclo de vida (NEW / RISING / FADING) a partir del último diff
de ``persist_narrative_history``. Sin DEAD en este slice.

Pipeline: tras ``persist_narrative_history``, antes de ``generate_alerts``.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DIFFS_DIR = PROJECT_ROOT / "data" / "narrative_history" / "diffs"
LIFECYCLE_DIR = PROJECT_ROOT / "data" / "narrative_history" / "lifecycle"

# Ajuste principal: variación mínima de strength para contar momentum (punto flotante).
# Sobrescribible con env sin tocar código.
DEFAULT_DELTA_STRENGTH_THRESHOLD = 2.0
_ENV_THRESHOLD = "VIXION_LIFECYCLE_DELTA_STRENGTH_THRESHOLD"


def lifecycle_threshold() -> float:
    raw = os.environ.get(_ENV_THRESHOLD)
    if raw is None or not str(raw).strip():
        return DEFAULT_DELTA_STRENGTH_THRESHOLD
    try:
        return float(raw)
    except ValueError:
        return DEFAULT_DELTA_STRENGTH_THRESHOLD


def classify_lifecycle_from_diff(
    diff: dict[str, Any],
    threshold: float,
) -> dict[str, Any]:
    """
    NEW ← ``added``.
    RISING ← ``changed`` con delta_strength > threshold.
    FADING ← ``changed`` con delta_strength < -threshold.
    Entradas en ``changed`` entre -threshold y +threshold (inclusive) no se listan.
    """
    new_items: list[dict[str, Any]] = []
    for a in diff.get("added") or []:
        if not isinstance(a, dict):
            continue
        new_items.append(
            {
                "narrative_key": a.get("narrative_key"),
                "narrative": a.get("narrative"),
                "narrative_strength": a.get("narrative_strength"),
                "rank": a.get("rank"),
                "type": a.get("type"),
            }
        )

    rising: list[dict[str, Any]] = []
    fading: list[dict[str, Any]] = []
    for c in diff.get("changed") or []:
        if not isinstance(c, dict):
            continue
        try:
            ds_f = float(c.get("delta_strength"))
        except (TypeError, ValueError):
            continue
        entry = {
            "narrative_key": c.get("narrative_key"),
            "narrative": c.get("narrative"),
            "delta_strength": ds_f,
            "current_strength": c.get("current_strength"),
            "previous_strength": c.get("previous_strength"),
            "current_rank": c.get("current_rank"),
            "previous_rank": c.get("previous_rank"),
        }
        if ds_f > threshold:
            rising.append(entry)
        elif ds_f < -threshold:
            fading.append(entry)

    return {
        "new": new_items,
        "rising": rising,
        "fading": fading,
    }


def find_latest_diff_file() -> Path:
    files = sorted(
        DIFFS_DIR.glob("diff_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No hay diff_*.json en {DIFFS_DIR}. Ejecuta persist_narrative_history antes."
        )
    return files[0]


def run_id_from_diff_path(path: Path) -> str:
    name = path.name
    if not name.startswith("diff_") or not name.endswith(".json"):
        raise ValueError(f"Nombre de diff inesperado: {path.name}")
    return name[len("diff_") : -len(".json")]


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


def main() -> int:
    threshold = lifecycle_threshold()
    try:
        diff_path = find_latest_diff_file()
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    try:
        diff = json.loads(diff_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"[ERROR] No se pudo leer {diff_path}: {exc}", file=sys.stderr)
        return 1

    if not isinstance(diff, dict):
        print("[ERROR] El diff no es un objeto JSON.", file=sys.stderr)
        return 1

    run_id = diff.get("current_run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        try:
            run_id = run_id_from_diff_path(diff_path)
        except ValueError:
            print("[ERROR] El diff no incluye current_run_id válido.", file=sys.stderr)
            return 1

    classified = classify_lifecycle_from_diff(diff, threshold)

    try:
        diff_rel = str(diff_path.relative_to(PROJECT_ROOT))
    except ValueError:
        diff_rel = str(diff_path)

    out: dict[str, Any] = {
        "run_id": run_id,
        "classified_at": datetime.now(UTC).isoformat(),
        "threshold_delta_strength": threshold,
        "threshold_env_var": _ENV_THRESHOLD,
        "threshold_default": DEFAULT_DELTA_STRENGTH_THRESHOLD,
        "source_diff": diff_rel,
        "is_first_snapshot": diff.get("previous_run_id") is None,
        **classified,
    }

    out_path = LIFECYCLE_DIR / f"lifecycle_{run_id}.json"
    try:
        write_json_atomic(out_path, out)
    except OSError as exc:
        print(f"[ERROR] No se pudo escribir {out_path}: {exc}", file=sys.stderr)
        return 1

    print(f"Lifecycle: {out_path}")
    print(
        f"  NEW={len(out['new'])}  RISING={len(out['rising'])}  "
        f"FADING={len(out['fading'])}  (umbral Δstrength=±{threshold})",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

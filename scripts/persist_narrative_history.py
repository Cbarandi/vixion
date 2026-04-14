#!/usr/bin/env python3
r"""
Persiste snapshots de narrativas y diff respecto a la corrida anterior.

Inserción en pipeline: justo después de ``detect_narratives``, antes de
``generate_alerts`` (ver ``run_pipeline.py``). No altera Telegram ni email.

Salidas (UTF-8, JSON):
  - ``data/narrative_history/snapshots/<run_id>.json`` — copia del payload de
    narrativas del run (mismo esquema que ``narratives_*.json``).
  - ``data/narrative_history/runs.jsonl`` — una línea JSON por corrida indexada:
    ``run_id``, ``saved_at``, rutas de fuente y snapshot.
  - ``data/narrative_history/diffs/diff_<run_id>.json`` — comparación última vs
    penúltima corrida **ya persistida** antes de añadir la nueva línea al índice.

Comportamiento primer run: no hay baseline en disco → ``previous_run_id`` null,
todas las narrativas en ``added``, ``note`` = ``first_snapshot_no_previous_run``.

Si el índice apunta a un snapshot previo ilegible o ausente, se difunde el mismo
estilo de diff "sin baseline" y ``note`` = ``previous_snapshot_unavailable``.

Identidad de narrativa: cadena exacta tras ``strip`` y colapso de espacios en
blanco (\s+ → un espacio); sin normalizar mayúsculas.

Orden de escritura: snapshot atómico → diff atómico → línea en ``runs.jsonl``
(solo si snapshot y diff se guardaron bien).
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_NARRATIVES_DIR = PROJECT_ROOT / "data" / "narratives"
SNAPSHOTS_DIR = PROJECT_ROOT / "data" / "narrative_history" / "snapshots"
DIFFS_DIR = PROJECT_ROOT / "data" / "narrative_history" / "diffs"
RUNS_INDEX = PROJECT_ROOT / "data" / "narrative_history" / "runs.jsonl"


def normalize_narrative_key(label: str) -> str:
    """Identidad: strip + espacios internos colapsados a un solo espacio."""
    s = label.strip()
    return re.sub(r"\s+", " ", s)


def run_id_from_saved_at(iso_str: str) -> str:
    """ID estable por instante UTC: YYYYMMDD_HHMMSS_microseconds."""
    raw = iso_str.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.strftime("%Y%m%d_%H%M%S_%f")


def find_latest_narratives_file() -> Path:
    files = sorted(
        DATA_NARRATIVES_DIR.glob("narratives_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No hay narratives_*.json en {DATA_NARRATIVES_DIR}. "
            "Ejecuta detect_narratives antes."
        )
    return files[0]


def read_json(path: Path) -> dict[str, Any]:
    """Lee JSON con UTF-8; fallos de lectura/parseo propagados al caller."""
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def load_runs_index() -> list[dict[str, Any]]:
    if not RUNS_INDEX.is_file():
        return []
    try:
        raw = RUNS_INDEX.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    entries: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


def append_run_index(entry: dict[str, Any]) -> None:
    RUNS_INDEX.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    with RUNS_INDEX.open("a", encoding="utf-8", newline="\n") as f:
        f.write(line)
    try:
        os.chmod(RUNS_INDEX, 0o644)
    except OSError:
        pass


def write_json_atomic(path: Path, payload: Any) -> None:
    """Escribe JSON UTF-8 en disco de forma atómica (tmp + replace en el mismo directorio)."""
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
    try:
        os.chmod(path, 0o644)
    except OSError:
        pass


def rank_by_strength(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Orden estable: strength desc, luego narrative asc."""
    sorted_rows = sorted(
        rows,
        key=lambda r: (
            -float(r.get("narrative_strength") or 0.0),
            str(r.get("narrative") or ""),
        ),
    )
    out: list[dict[str, Any]] = []
    for i, r in enumerate(sorted_rows, start=1):
        copy = dict(r)
        copy["_rank"] = i
        out.append(copy)
    return out


def rows_by_key(
    ranked: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for r in ranked:
        label = r.get("narrative")
        if not isinstance(label, str):
            continue
        key = normalize_narrative_key(label)
        if not key:
            continue
        by_key[key] = r
    return by_key


def build_diff(
    current_run_id: str,
    previous_run_id: str | None,
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]] | None,
    *,
    diff_generated_at: str | None = None,
    unavailable_note: str | None = None,
) -> dict[str, Any]:
    """
    ``previous_rows is None`` → no hay baseline (primer índice o snapshot previo
    ilegible/faltante). Usa ``unavailable_note`` para distinguir causas.

    ``previous_rows == []`` → baseline vacío explícito: el diff lista solo
    ``added`` / ``removed`` / ``changed`` respecto a lista vacía.
    """
    gen_at = diff_generated_at or datetime.now(UTC).isoformat()

    if previous_rows is None:
        ranked_curr = rank_by_strength(current_rows)
        added_out: list[dict[str, Any]] = []
        for r in ranked_curr:
            label = r.get("narrative")
            if not isinstance(label, str):
                continue
            k = normalize_narrative_key(label)
            if not k:
                continue
            added_out.append(
                {
                    "narrative": label,
                    "narrative_key": k,
                    "narrative_strength": float(r.get("narrative_strength") or 0.0),
                    "total_articles": int(r.get("total_articles") or 0),
                    "rank": int(r.get("_rank") or 0),
                    "type": r.get("type"),
                }
            )
        note = unavailable_note or "first_snapshot_no_previous_run"
        return {
            "diff_generated_at": gen_at,
            "current_run_id": current_run_id,
            "previous_run_id": None,
            "previous_snapshot_path": None,
            "added": added_out,
            "removed": [],
            "changed": [],
            "note": note,
        }

    ranked_curr = rank_by_strength(current_rows)
    ranked_prev = rank_by_strength(previous_rows)
    curr_map = rows_by_key(ranked_curr)
    prev_map = rows_by_key(ranked_prev)

    curr_keys = set(curr_map.keys())
    prev_keys = set(prev_map.keys())

    added: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []

    for k in sorted(curr_keys - prev_keys):
        r = curr_map[k]
        label = r.get("narrative")
        if not isinstance(label, str):
            label = k
        added.append(
            {
                "narrative": label,
                "narrative_key": k,
                "narrative_strength": float(r.get("narrative_strength") or 0.0),
                "total_articles": int(r.get("total_articles") or 0),
                "rank": int(r.get("_rank") or 0),
                "type": r.get("type"),
            }
        )

    for k in sorted(prev_keys - curr_keys):
        r = prev_map[k]
        label = r.get("narrative")
        if not isinstance(label, str):
            label = k
        removed.append(
            {
                "narrative": label,
                "narrative_key": k,
                "narrative_strength": float(r.get("narrative_strength") or 0.0),
                "total_articles": int(r.get("total_articles") or 0),
                "rank": int(r.get("_rank") or 0),
                "type": r.get("type"),
            }
        )

    for k in sorted(curr_keys & prev_keys):
        c = curr_map[k]
        p = prev_map[k]
        label_c = c.get("narrative")
        if not isinstance(label_c, str):
            label_c = k
        cs = float(c.get("narrative_strength") or 0.0)
        ps = float(p.get("narrative_strength") or 0.0)
        cr = int(c.get("_rank") or 0)
        pr = int(p.get("_rank") or 0)
        cta = int(c.get("total_articles") or 0)
        pta = int(p.get("total_articles") or 0)
        changed.append(
            {
                "narrative": label_c,
                "narrative_key": k,
                "current_strength": cs,
                "previous_strength": ps,
                "delta_strength": round(cs - ps, 6),
                "current_rank": cr,
                "previous_rank": pr,
                "delta_rank": cr - pr,
                "current_total_articles": cta,
                "previous_total_articles": pta,
            }
        )

    return {
        "diff_generated_at": gen_at,
        "current_run_id": current_run_id,
        "previous_run_id": previous_run_id,
        "previous_snapshot_path": None,
        "added": added,
        "removed": removed,
        "changed": changed,
    }


def main() -> int:
    try:
        latest = find_latest_narratives_file()
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    try:
        payload = read_json(latest)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"[ERROR] No se pudo leer {latest}: {exc}", file=sys.stderr)
        return 1

    saved_at = payload.get("saved_at")
    if not isinstance(saved_at, str) or not saved_at.strip():
        print("[ERROR] Falta 'saved_at' en el JSON de narrativas.", file=sys.stderr)
        return 1

    narratives = payload.get("narratives")
    if not isinstance(narratives, list):
        print("[ERROR] Falta lista 'narratives'.", file=sys.stderr)
        return 1

    try:
        run_id = run_id_from_saved_at(saved_at)
    except (TypeError, ValueError) as exc:
        print(f"[ERROR] saved_at inválido: {exc}", file=sys.stderr)
        return 1

    entries = load_runs_index()
    if entries and entries[-1].get("run_id") == run_id:
        print(f"[persist_narrative_history] Ya indexada corrida {run_id}; sin cambios.")
        return 0

    previous_run_id: str | None = None
    previous_rows: list[dict[str, Any]] | None = None
    previous_snapshot_fs: Path | None = None
    unavailable_note: str | None = None

    if entries:
        prev_entry = entries[-1]
        prev_id = prev_entry.get("run_id")
        prev_snap = prev_entry.get("snapshot_path")
        if isinstance(prev_id, str) and isinstance(prev_snap, str):
            previous_snapshot_fs = (PROJECT_ROOT / prev_snap).resolve()
            if previous_snapshot_fs.is_file():
                try:
                    prev_payload = read_json(previous_snapshot_fs)
                    pr = prev_payload.get("narratives")
                    if isinstance(pr, list):
                        previous_run_id = prev_id
                        previous_rows = pr
                    else:
                        unavailable_note = "previous_snapshot_unavailable"
                except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                    unavailable_note = "previous_snapshot_unavailable"
                    print(
                        f"[warn] Snapshot previo ilegible {previous_snapshot_fs}: {exc}",
                        file=sys.stderr,
                    )
            else:
                unavailable_note = "previous_snapshot_missing"
                print(
                    f"[warn] Snapshot previo no encontrado: {previous_snapshot_fs}",
                    file=sys.stderr,
                )
        else:
            unavailable_note = "previous_snapshot_unavailable"

    snapshot_path = SNAPSHOTS_DIR / f"{run_id}.json"
    try:
        write_json_atomic(snapshot_path, payload)
    except OSError as exc:
        print(f"[ERROR] No se pudo escribir snapshot: {exc}", file=sys.stderr)
        return 1

    diff = build_diff(
        run_id,
        previous_run_id,
        narratives,
        previous_rows,
        unavailable_note=unavailable_note,
    )
    if previous_snapshot_fs is not None:
        try:
            diff["previous_snapshot_path"] = str(
                previous_snapshot_fs.relative_to(PROJECT_ROOT)
            )
        except ValueError:
            diff["previous_snapshot_path"] = str(previous_snapshot_fs)

    diff_path = DIFFS_DIR / f"diff_{run_id}.json"
    try:
        write_json_atomic(diff_path, diff)
    except OSError as exc:
        print(f"[ERROR] No se pudo escribir diff: {exc}", file=sys.stderr)
        return 1

    try:
        latest_rel = str(latest.relative_to(PROJECT_ROOT))
    except ValueError:
        latest_rel = str(latest)
    try:
        snapshot_rel = str(snapshot_path.relative_to(PROJECT_ROOT))
    except ValueError:
        snapshot_rel = str(snapshot_path)

    try:
        append_run_index(
            {
                "run_id": run_id,
                "saved_at": saved_at,
                "source_narratives_file": latest_rel,
                "snapshot_path": snapshot_rel,
            }
        )
    except OSError as exc:
        print(
            f"[ERROR] Snapshot y diff escritos pero índice falló (corrige manualmente): {exc}",
            file=sys.stderr,
        )
        return 1

    print(f"Snapshot: {snapshot_path}")
    print(f"Índice:   {RUNS_INDEX} (append)")
    print(f"Diff:     {diff_path}")
    if previous_run_id:
        print(f"Diff vs:  {previous_run_id}")
    else:
        print("Diff vs:  (sin corrida anterior)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

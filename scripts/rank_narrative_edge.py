#!/usr/bin/env python3
"""
Narrative Edge Ranking v1 — ranking explicable sobre agregados de outcomes.

Fórmula (no pretende ser estadísticamente óptima; es heurística operativa):

1) Se consideran solo horizontes con count_with_returns_h > 0.
2) raw_positive_edge = media ponderada de positive_rate_1d/3d/7d
   (pesos 0.40 / 0.35 / 0.25; si falta un horizonte, se renormalizan los pesos).
3) Eligibilidad: occurrences >= MIN_OCCURRENCES y al menos un horizonte con retornos.
4) Shrinkage hacia 0.5 (neutral): edge_score = 0.5 + (raw_positive_edge - 0.5)
   * (occurrences / (occurrences + SHRINKAGE_K))
   Así las muestras pequeñas no obtienen puntuaciones extremas aunque pasen el mínimo.
5) Orden estable: edge_score desc, occurrences desc, narrative_key asc.

Los avg_btc_return_* se incluyen en la salida como contexto; no entran en el score v1
para mantener una sola señal principal (tasas positivas) y evitar doble conteo con las tasas.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

ENV_SKIP = "VIXION_SKIP_RANK_NARRATIVE_EDGE"
ENV_MIN_OCC = "VIXION_EDGE_MIN_OCCURRENCES"
ENV_SHRINK = "VIXION_EDGE_SHRINKAGE_K"

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_IN = os.path.join(REPO_ROOT, "data/outcomes/narrative_aggregates/latest.json")
OUT_FILE = os.path.join(REPO_ROOT, "data/outcomes/narrative_edge/latest.json")

HORIZONS = ("1d", "3d", "7d")
WEIGHTS: dict[str, float] = {"1d": 0.40, "3d": 0.35, "7d": 0.25}


def _path_for_report(p: str) -> str:
    try:
        return os.path.relpath(p, REPO_ROOT)
    except ValueError:
        return p


def weighted_positive_rate(row: dict[str, Any]) -> float | None:
    """Media ponderada de positive_rate por horizontes con datos; None si no hay ninguno."""
    num = 0.0
    den = 0.0
    for h in HORIZONS:
        cnt_key = f"count_with_returns_{h}"
        pr_key = f"positive_rate_{h}"
        cnt = row.get(cnt_key)
        if not isinstance(cnt, int) or cnt <= 0:
            continue
        pr = row.get(pr_key)
        if pr is None:
            continue
        try:
            pv = float(pr)
        except (TypeError, ValueError):
            continue
        w = WEIGHTS[h]
        num += pv * w
        den += w
    if den <= 0:
        return None
    return num / den


def shrink_toward_neutral(raw: float, occurrences: int, k: float) -> float:
    """Encoge hacia 0.5; raw en [0,1] típicamente."""
    if occurrences <= 0:
        return 0.5
    factor = float(occurrences) / (float(occurrences) + float(k))
    return 0.5 + (raw - 0.5) * factor


def is_eligible(row: dict[str, Any], min_occurrences: int) -> bool:
    occ = row.get("occurrences")
    if not isinstance(occ, int) or occ < min_occurrences:
        return False
    for h in HORIZONS:
        cnt = row.get(f"count_with_returns_{h}")
        if isinstance(cnt, int) and cnt > 0:
            return True
    return False


def stable_sort_key(item: dict[str, Any]) -> tuple[float, int, str]:
    return (
        -float(item["edge_score"]),
        -int(item["occurrences"]),
        str(item["narrative_key"]),
    )


def build_ranking_payload(
    aggregate: dict[str, Any],
    *,
    min_occurrences: int,
    shrinkage_k: float,
    source_path: str,
) -> dict[str, Any]:
    narratives = aggregate.get("narratives")
    if not isinstance(narratives, list):
        narratives = []

    rows: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []

    for row in narratives:
        if not isinstance(row, dict):
            continue
        nk = row.get("narrative_key")
        if not isinstance(nk, str) or not nk.strip():
            continue

        raw = weighted_positive_rate(row)
        occ = row.get("occurrences")
        occ_i = int(occ) if isinstance(occ, int) else 0

        base: dict[str, Any] = {
            "narrative_key": nk,
            "occurrences": occ_i,
            "runs_tagged_new": row.get("runs_tagged_new"),
            "runs_tagged_rising": row.get("runs_tagged_rising"),
            "raw_positive_edge": round(raw, 8) if raw is not None else None,
            "positive_rate_1d": row.get("positive_rate_1d"),
            "positive_rate_3d": row.get("positive_rate_3d"),
            "positive_rate_7d": row.get("positive_rate_7d"),
            "avg_btc_return_1d": row.get("avg_btc_return_1d"),
            "avg_btc_return_3d": row.get("avg_btc_return_3d"),
            "avg_btc_return_7d": row.get("avg_btc_return_7d"),
            "count_with_returns_1d": row.get("count_with_returns_1d"),
            "count_with_returns_3d": row.get("count_with_returns_3d"),
            "count_with_returns_7d": row.get("count_with_returns_7d"),
        }

        if not is_eligible(row, min_occurrences):
            reason = (
                "below_min_occurrences"
                if occ_i < min_occurrences
                else "no_forward_returns"
            )
            excluded.append({"narrative_key": nk, "reason": reason, "occurrences": occ_i})
            base["eligible"] = False
            base["edge_score"] = None
            base["exclude_reason"] = reason
            rows.append(base)
            continue

        if raw is None:
            excluded.append(
                {"narrative_key": nk, "reason": "no_positive_rates", "occurrences": occ_i}
            )
            base["eligible"] = False
            base["edge_score"] = None
            base["exclude_reason"] = "no_positive_rates"
            rows.append(base)
            continue

        edge = shrink_toward_neutral(raw, occ_i, shrinkage_k)
        base["eligible"] = True
        base["edge_score"] = round(edge, 8)
        base["exclude_reason"] = None
        rows.append(base)

    ranked_only = [r for r in rows if r.get("eligible") is True]
    ranked_only.sort(key=stable_sort_key)
    for i, r in enumerate(ranked_only, start=1):
        r["rank"] = i

    rank_by_key = {r["narrative_key"]: r["rank"] for r in ranked_only}
    for r in rows:
        if r.get("eligible"):
            r["rank"] = rank_by_key[r["narrative_key"]]
        else:
            r["rank"] = None

    gen_at = datetime.now(UTC).isoformat()
    return {
        "schema_version": 1,
        "generated_at": gen_at,
        "ranking_id": "narrative_edge_v1",
        "formula": {
            "description": "weighted positive_rate by horizon (0.4/0.35/0.25), "
            "shrink toward 0.5 by occurrences/(occurrences+k)",
            "weights_by_horizon": dict(WEIGHTS),
            "shrinkage_k": shrinkage_k,
            "min_occurrences": min_occurrences,
        },
        "source_aggregate": _path_for_report(source_path),
        "aggregate_generated_at": aggregate.get("generated_at"),
        "ranked": ranked_only,
        "all_narratives": rows,
        "excluded_summary": {
            "count": len(excluded),
            "items": excluded[:50],
        },
    }


def write_json_atomic(path: str, data: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def main() -> int:
    if (os.environ.get(ENV_SKIP) or "").strip().lower() in ("1", "true", "yes"):
        print(f"[rank_narrative_edge] Omitido ({ENV_SKIP}=1).")
        return 0

    in_path = os.environ.get("VIXION_NARRATIVE_AGGREGATES_JSON") or DEFAULT_IN
    min_occ = int((os.environ.get(ENV_MIN_OCC) or "3").strip() or "3")
    shrink_k = float((os.environ.get(ENV_SHRINK) or "2.0").strip() or "2.0")

    try:
        with open(in_path, encoding="utf-8") as f:
            aggregate = json.load(f)
    except OSError as exc:
        print(f"[ERROR] No se pudo leer {in_path}: {exc}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as exc:
        print(f"[ERROR] JSON inválido en {in_path}: {exc}", file=sys.stderr)
        return 1

    payload = build_ranking_payload(
        aggregate,
        min_occurrences=min_occ,
        shrinkage_k=shrink_k,
        source_path=in_path,
    )

    try:
        write_json_atomic(OUT_FILE, payload)
    except OSError as exc:
        print(f"[ERROR] No se pudo escribir {OUT_FILE}: {exc}", file=sys.stderr)
        return 1

    n = len(payload["ranked"])
    print(f"Narrative edge ranking: {OUT_FILE}")
    print(f"  elegibles rankeados: {n} · min_occurrences={min_occ} · shrinkage_k={shrink_k}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

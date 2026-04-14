#!/usr/bin/env python3
"""
Narrative Edge Ranking — v1 (señal base) + v2 (ajuste riesgo/timing).

v1 (sin cambios de definición):
1) Horizontes con count_with_returns_h > 0.
2) raw_positive_edge = media ponderada de positive_rate_1d/3d/7d
   (pesos 0.40 / 0.35 / 0.25; si falta un horizonte, se renormalizan los pesos).
3) Eligibilidad: occurrences >= MIN_OCCURRENCES y al menos un horizonte con retornos.
4) edge_score (v1) = 0.5 + (raw_positive_edge - 0.5) * (occ / (occ + SHRINKAGE_K)).

v2 (multiplicativo, determinista):
   Misma media ponderada y mismos pesos que v1, aplicados a:
   - avg_btc_max_drawdown_* donde count_with_drawdown_* > 0
   - avg_btc_time_to_peak_hours_* donde count_with_time_to_peak_* > 0

   p_dd = min(PENALTY_MAX, (weighted_avg_dd / DD_REF) * PENALTY_MAX)
   p_ttp = min(PENALTY_MAX, (weighted_avg_ttp_h / TTP_REF_HOURS) * PENALTY_MAX)

   Si no hay datos de DD (o TTP) en ningún horizonte, esa penalización es 0.

   edge_score_v2 = edge_score * (1 - p_dd) * (1 - p_ttp)

5) Orden del ranking: edge_score_v2 desc, edge_score desc, occurrences desc, narrative_key asc.

Los avg_btc_return_* siguen solo como contexto en la salida (no entran en v1 ni v2).
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
ENV_V2_DD_REF = "VIXION_EDGE_V2_DD_REF"
ENV_V2_TTP_REF = "VIXION_EDGE_V2_TTP_REF_HOURS"
ENV_V2_PENALTY_MAX = "VIXION_EDGE_V2_PENALTY_MAX"

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_IN = os.path.join(REPO_ROOT, "data/outcomes/narrative_aggregates/latest.json")
OUT_FILE = os.path.join(REPO_ROOT, "data/outcomes/narrative_edge/latest.json")

HORIZONS = ("1d", "3d", "7d")
WEIGHTS: dict[str, float] = {"1d": 0.40, "3d": 0.35, "7d": 0.25}

# v2: referencias lineales para penalización (sin ajuste estadístico).
DEFAULT_V2_DD_REF = 0.15
DEFAULT_V2_TTP_REF_HOURS = 48.0
DEFAULT_V2_PENALTY_MAX = 0.5


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


def weighted_horizon_metric(
    row: dict[str, Any],
    value_prefix: str,
    count_prefix: str,
) -> float | None:
    """Media ponderada (mismos pesos que positive_rate) solo en horizontes con count > 0."""
    num = 0.0
    den = 0.0
    for h in HORIZONS:
        cnt_key = f"{count_prefix}_{h}"
        val_key = f"{value_prefix}_{h}"
        cnt = row.get(cnt_key)
        if not isinstance(cnt, int) or cnt <= 0:
            continue
        raw = row.get(val_key)
        if raw is None:
            continue
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        w = WEIGHTS[h]
        num += v * w
        den += w
    if den <= 0:
        return None
    return num / den


def capped_linear_penalty(
    weighted_value: float | None,
    ref: float,
    penalty_max: float,
) -> float:
    """
    Penalización en [0, penalty_max]: en ref alcanza penalty_max; lineal por debajo;
    plateau por encima de ref.
    """
    if weighted_value is None or ref <= 0 or penalty_max <= 0:
        return 0.0
    v = max(0.0, float(weighted_value))
    return min(float(penalty_max), (v / float(ref)) * float(penalty_max))


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


def stable_sort_key(item: dict[str, Any]) -> tuple[float, float, int, str]:
    v2 = item.get("edge_score_v2")
    v1 = item.get("edge_score")
    v2f = -float(v2) if isinstance(v2, (int, float)) else 0.0
    v1f = -float(v1) if isinstance(v1, (int, float)) else 0.0
    return (
        v2f,
        v1f,
        -int(item["occurrences"]),
        str(item["narrative_key"]),
    )


def build_ranking_payload(
    aggregate: dict[str, Any],
    *,
    min_occurrences: int,
    shrinkage_k: float,
    source_path: str,
    v2_dd_ref: float = DEFAULT_V2_DD_REF,
    v2_ttp_ref_hours: float = DEFAULT_V2_TTP_REF_HOURS,
    v2_penalty_max: float = DEFAULT_V2_PENALTY_MAX,
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
            "avg_btc_max_drawdown_1d": row.get("avg_btc_max_drawdown_1d"),
            "avg_btc_max_drawdown_3d": row.get("avg_btc_max_drawdown_3d"),
            "avg_btc_max_drawdown_7d": row.get("avg_btc_max_drawdown_7d"),
            "count_with_drawdown_1d": row.get("count_with_drawdown_1d"),
            "count_with_drawdown_3d": row.get("count_with_drawdown_3d"),
            "count_with_drawdown_7d": row.get("count_with_drawdown_7d"),
            "avg_btc_time_to_peak_hours_1d": row.get("avg_btc_time_to_peak_hours_1d"),
            "avg_btc_time_to_peak_hours_3d": row.get("avg_btc_time_to_peak_hours_3d"),
            "avg_btc_time_to_peak_hours_7d": row.get("avg_btc_time_to_peak_hours_7d"),
            "count_with_time_to_peak_1d": row.get("count_with_time_to_peak_1d"),
            "count_with_time_to_peak_3d": row.get("count_with_time_to_peak_3d"),
            "count_with_time_to_peak_7d": row.get("count_with_time_to_peak_7d"),
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
            base["edge_score_v2"] = None
            base["weighted_avg_max_drawdown"] = None
            base["weighted_avg_time_to_peak_hours"] = None
            base["drawdown_penalty"] = None
            base["time_to_peak_penalty"] = None
            base["exclude_reason"] = reason
            rows.append(base)
            continue

        if raw is None:
            excluded.append(
                {"narrative_key": nk, "reason": "no_positive_rates", "occurrences": occ_i}
            )
            base["eligible"] = False
            base["edge_score"] = None
            base["edge_score_v2"] = None
            base["weighted_avg_max_drawdown"] = None
            base["weighted_avg_time_to_peak_hours"] = None
            base["drawdown_penalty"] = None
            base["time_to_peak_penalty"] = None
            base["exclude_reason"] = "no_positive_rates"
            rows.append(base)
            continue

        edge = shrink_toward_neutral(raw, occ_i, shrinkage_k)
        w_dd = weighted_horizon_metric(row, "avg_btc_max_drawdown", "count_with_drawdown")
        w_ttp = weighted_horizon_metric(
            row, "avg_btc_time_to_peak_hours", "count_with_time_to_peak"
        )
        p_dd = capped_linear_penalty(w_dd, v2_dd_ref, v2_penalty_max)
        p_ttp = capped_linear_penalty(w_ttp, v2_ttp_ref_hours, v2_penalty_max)
        edge_v2 = edge * (1.0 - p_dd) * (1.0 - p_ttp)

        base["eligible"] = True
        base["edge_score"] = round(edge, 8)
        base["weighted_avg_max_drawdown"] = round(w_dd, 8) if w_dd is not None else None
        base["weighted_avg_time_to_peak_hours"] = round(w_ttp, 8) if w_ttp is not None else None
        base["drawdown_penalty"] = round(p_dd, 8)
        base["time_to_peak_penalty"] = round(p_ttp, 8)
        base["edge_score_v2"] = round(edge_v2, 8)
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
        "schema_version": 2,
        "generated_at": gen_at,
        "ranking_id": "narrative_edge_v2",
        "formula": {
            "edge_score_v1": "0.5 + (raw_positive_edge - 0.5) * (occurrences / (occurrences + k))",
            "edge_score_v2": "edge_score * (1 - drawdown_penalty) * (1 - time_to_peak_penalty)",
            "drawdown_penalty": "min(penalty_max, (weighted_avg_max_drawdown / dd_ref) * penalty_max); "
            "weighted avg same weights as v1, horizons with count_with_drawdown_h > 0 only",
            "time_to_peak_penalty": "min(penalty_max, (weighted_avg_time_to_peak_hours / ttp_ref_hours) "
            "* penalty_max); horizons with count_with_time_to_peak_h > 0 only",
            "weights_by_horizon": dict(WEIGHTS),
            "shrinkage_k": shrinkage_k,
            "min_occurrences": min_occurrences,
            "v2_dd_ref": v2_dd_ref,
            "v2_ttp_ref_hours": v2_ttp_ref_hours,
            "v2_penalty_max": v2_penalty_max,
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
    v2_dd = float((os.environ.get(ENV_V2_DD_REF) or str(DEFAULT_V2_DD_REF)).strip() or str(DEFAULT_V2_DD_REF))
    v2_ttp = float(
        (os.environ.get(ENV_V2_TTP_REF) or str(DEFAULT_V2_TTP_REF_HOURS)).strip()
        or str(DEFAULT_V2_TTP_REF_HOURS)
    )
    v2_pmax = float(
        (os.environ.get(ENV_V2_PENALTY_MAX) or str(DEFAULT_V2_PENALTY_MAX)).strip()
        or str(DEFAULT_V2_PENALTY_MAX)
    )

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
        v2_dd_ref=v2_dd,
        v2_ttp_ref_hours=v2_ttp,
        v2_penalty_max=v2_pmax,
    )

    try:
        write_json_atomic(OUT_FILE, payload)
    except OSError as exc:
        print(f"[ERROR] No se pudo escribir {OUT_FILE}: {exc}", file=sys.stderr)
        return 1

    n = len(payload["ranked"])
    print(f"Narrative edge ranking: {OUT_FILE}")
    print(
        f"  elegibles rankeados: {n} · min_occurrences={min_occ} · shrinkage_k={shrink_k} · "
        f"v2 dd_ref={v2_dd} ttp_ref_h={v2_ttp} penalty_max={v2_pmax}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

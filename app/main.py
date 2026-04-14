"""API mínima FastAPI: último JSON scored y narrativas detectadas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_SCORED_DIR = PROJECT_ROOT / "data" / "scored"
DATA_NARRATIVES_DIR = PROJECT_ROOT / "data" / "narratives"
DATA_ALERTS_DIR = PROJECT_ROOT / "data" / "alerts"
DATA_NH_DIFFS = PROJECT_ROOT / "data" / "narrative_history" / "diffs"
DATA_NH_LIFECYCLE = PROJECT_ROOT / "data" / "narrative_history" / "lifecycle"

SORT_FIELDS = frozenset({"priority_score", "signal_score", "risk_score"})

app = FastAPI(
    title="VIXION Scored Articles API",
    version="0.1.0",
    description="Sirve scored, narrativas, alerts y artefactos narrative_history para panel/admin.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:3000",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:5174",
        "http://localhost:5174",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def find_latest_scored_json(scored_dir: Path) -> Path | None:
    """Ruta del rss_scored_*.json más reciente (por mtime), o None si no hay."""
    candidates = sorted(
        scored_dir.glob("rss_scored_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_latest_payload() -> dict[str, Any]:
    """Carga y parsea el último JSON scored."""
    path = find_latest_scored_json(DATA_SCORED_DIR)
    if path is None:
        raise FileNotFoundError(
            f"No hay archivos rss_scored_*.json en {DATA_SCORED_DIR}. "
            "Ejecuta antes: python scripts/score_articles.py",
        )
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"No se pudo leer o parsear {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("El JSON scored no es un objeto en la raíz.")
    data["_source_path"] = str(path.resolve())
    return data


def find_latest_narratives_json(narratives_dir: Path) -> Path | None:
    """Ruta del narratives_*.json más reciente (por mtime), o None si no hay."""
    candidates = sorted(
        narratives_dir.glob("narratives_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_latest_narratives_payload() -> dict[str, Any]:
    """Carga y parsea el último JSON de narrativas."""
    path = find_latest_narratives_json(DATA_NARRATIVES_DIR)
    if path is None:
        raise FileNotFoundError(
            f"No hay archivos narratives_*.json en {DATA_NARRATIVES_DIR}. "
            "Ejecuta antes: python scripts/detect_narratives.py",
        )
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"No se pudo leer o parsear {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("El JSON de narrativas no es un objeto en la raíz.")
    data["_source_path"] = str(path.resolve())
    return data


def find_latest_alerts_json(alerts_dir: Path) -> Path | None:
    candidates = sorted(
        alerts_dir.glob("alerts_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_latest_alerts_payload() -> dict[str, Any]:
    path = find_latest_alerts_json(DATA_ALERTS_DIR)
    if path is None:
        raise FileNotFoundError(
            f"No hay alerts_*.json en {DATA_ALERTS_DIR}. "
            "Ejecuta: python scripts/generate_alerts.py",
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"No se pudo leer {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("El JSON de alerts no es un objeto.")
    data["_source_path"] = str(path.resolve())
    return data


def find_latest_glob_file(directory: Path, pattern: str) -> Path | None:
    """Último archivo por mtime que coincide con pattern, o None."""
    if not directory.is_dir():
        return None
    candidates = sorted(
        directory.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


@app.get("/narrative-history/latest")
def narrative_history_latest() -> dict[str, Any]:
    """
    Últimos JSON de diff y lifecycle generados por el pipeline (sin 404 si faltan).
    """
    lifecycle_path = find_latest_glob_file(DATA_NH_LIFECYCLE, "lifecycle_*.json")
    diff_path = find_latest_glob_file(DATA_NH_DIFFS, "diff_*.json")

    out: dict[str, Any] = {
        "lifecycle": None,
        "diff_meta": None,
        "lifecycle_source": None,
        "diff_source": None,
    }

    if lifecycle_path and lifecycle_path.is_file():
        try:
            data = json.loads(lifecycle_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                out["lifecycle"] = data
                try:
                    out["lifecycle_source"] = str(lifecycle_path.relative_to(PROJECT_ROOT))
                except ValueError:
                    out["lifecycle_source"] = str(lifecycle_path)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            pass

    if diff_path and diff_path.is_file():
        try:
            d = json.loads(diff_path.read_text(encoding="utf-8"))
            if isinstance(d, dict):
                added = d.get("added")
                removed = d.get("removed")
                changed = d.get("changed")
                out["diff_meta"] = {
                    "current_run_id": d.get("current_run_id"),
                    "previous_run_id": d.get("previous_run_id"),
                    "diff_generated_at": d.get("diff_generated_at"),
                    "counts": {
                        "added": len(added) if isinstance(added, list) else 0,
                        "removed": len(removed) if isinstance(removed, list) else 0,
                        "changed": len(changed) if isinstance(changed, list) else 0,
                    },
                }
                try:
                    out["diff_source"] = str(diff_path.relative_to(PROJECT_ROOT))
                except ValueError:
                    out["diff_source"] = str(diff_path)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            pass

    return out


def find_recent_alerts_json_paths(alerts_dir: Path, limit: int) -> list[Path]:
    """Últimos `limit` archivos alerts_*.json por mtime (más reciente primero)."""
    candidates = sorted(
        alerts_dir.glob("alerts_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[:limit]


def load_merged_recent_alerts(limit: int) -> tuple[list[dict[str, Any]], Any, list[Path]]:
    """
    Concatena alertas de los últimos `limit` ficheros.
    Orden: archivos de más antiguo a más reciente dentro del rango, para que
    entradas repetidas/narrativa queden representadas por la corrida más nueva.
    """
    paths = find_recent_alerts_json_paths(DATA_ALERTS_DIR, limit)
    if not paths:
        raise FileNotFoundError(
            f"No hay alerts_*.json en {DATA_ALERTS_DIR}. "
            "Ejecuta: python scripts/generate_alerts.py",
        )

    merged: list[dict[str, Any]] = []
    for path in reversed(paths):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        alerts = data.get("alerts")
        if not isinstance(alerts, list):
            continue
        for item in alerts:
            if isinstance(item, dict):
                merged.append(item)

    newest_saved_at: Any = None
    try:
        tip = json.loads(paths[0].read_text(encoding="utf-8"))
        if isinstance(tip, dict):
            newest_saved_at = tip.get("saved_at")
    except (OSError, UnicodeError, json.JSONDecodeError):
        pass

    return merged, newest_saved_at, paths


def _articles_or_404() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        payload = load_latest_payload()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    articles = payload.get("articles")
    if not isinstance(articles, list):
        raise HTTPException(status_code=500, detail="Payload sin lista 'articles'.")
    return payload, articles


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/articles/latest")
def articles_latest() -> dict[str, Any]:
    payload, articles = _articles_or_404()
    return {
        "saved_at": payload.get("saved_at"),
        "article_count": payload.get("article_count", len(articles)),
        "articles": articles,
        "source_file": payload.get("_source_path"),
    }


@app.get("/articles/top")
def articles_top(
    sort_by: str = Query("priority_score", description="Campo de ordenación"),
    limit: int = Query(20, ge=1, le=200, description="Máximo de artículos"),
) -> dict[str, Any]:
    if sort_by not in SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"sort_by debe ser uno de: {', '.join(sorted(SORT_FIELDS))}",
        )
    payload, articles = _articles_or_404()
    scored: list[dict[str, Any]] = [a for a in articles if isinstance(a, dict)]

    def key_fn(item: dict[str, Any]) -> float:
        v = item.get(sort_by)
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    ranked = sorted(scored, key=key_fn, reverse=True)[:limit]
    return {
        "sort_by": sort_by,
        "limit": limit,
        "count": len(ranked),
        "articles": ranked,
        "source_file": payload.get("_source_path"),
    }


@app.get("/articles/filters")
def articles_filters() -> dict[str, Any]:
    _, articles = _articles_or_404()
    buckets: set[str] = set()
    topic_tags: set[str] = set()
    narratives: set[str] = set()

    for item in articles:
        if not isinstance(item, dict):
            continue
        b = item.get("score_bucket")
        if isinstance(b, str) and b.strip():
            buckets.add(b.strip())
        tags = item.get("topic_tags")
        if isinstance(tags, list):
            for t in tags:
                if isinstance(t, str) and t.strip():
                    topic_tags.add(t.strip())
        narrs = item.get("narrative_candidates")
        if isinstance(narrs, list):
            for n in narrs:
                if isinstance(n, str) and n.strip():
                    narratives.add(n.strip())

    return {
        "score_buckets": sorted(buckets),
        "topic_tags": sorted(topic_tags),
        "narrative_candidates": sorted(narratives),
    }


@app.get("/narratives/latest")
def narratives_latest() -> dict[str, Any]:
    try:
        payload = load_latest_narratives_payload()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    narratives = payload.get("narratives")
    if not isinstance(narratives, list):
        raise HTTPException(status_code=500, detail="Payload sin lista 'narratives'.")
    return {
        "saved_at": payload.get("saved_at"),
        "narrative_count": payload.get("narrative_count", len(narratives)),
        "narratives": narratives,
        "source_file": payload.get("_source_path"),
    }


@app.get("/alerts/latest")
def alerts_latest() -> dict[str, Any]:
    try:
        payload = load_latest_alerts_payload()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    alerts = payload.get("alerts")
    if not isinstance(alerts, list):
        raise HTTPException(status_code=500, detail="Payload sin lista 'alerts'.")

    return {
        "saved_at": payload.get("saved_at"),
        "alert_count": payload.get("alert_count", len(alerts)),
        "alerts": alerts,
        "source_file": payload.get("_source_path"),
    }


@app.get("/alerts/recent")
def alerts_recent(
    limit: int = Query(
        5,
        ge=1,
        le=50,
        description="Número de ficheros alerts_*.json recientes a combinar (mtime)",
    ),
) -> dict[str, Any]:
    try:
        merged, newest_saved_at, paths = load_merged_recent_alerts(limit)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "saved_at": newest_saved_at,
        "alert_count": len(merged),
        "alerts": merged,
        "source_files": [str(p.resolve()) for p in paths],
        "files_merged": len(paths),
    }

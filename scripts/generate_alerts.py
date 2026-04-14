#!/usr/bin/env python3
"""Genera alertas desde narratives_*.json: early, momentum y SURGE (v2)."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_NARRATIVES_DIR = PROJECT_ROOT / "data" / "narratives"
DATA_ALERTS_DIR = PROJECT_ROOT / "data" / "alerts"
DATA_LIFECYCLE_DIR = PROJECT_ROOT / "data" / "narrative_history" / "lifecycle"

# Umbrales (alineados con panel admin)
THRESHOLD_EARLY_OPPORTUNITY = 20.0
THRESHOLD_CONFIRMED_MOMENTUM = 40.0
# SURGE: crecimiento relativo narrative_strength entre dos corridas de detect_narratives
SURGE_GROWTH_THRESHOLD = 0.5


def surge_growth_bucket(growth: float) -> str:
    """
    Magnitud del salto (dedup por evento, no solo por narrativa).
    - 50%–<100%  → surge_50
    - 100%–<200% → surge_100
    - ≥200%      → surge_200
    """
    if growth >= 2.0:
        return "surge_200"
    if growth >= 1.0:
        return "surge_100"
    return "surge_50"


def _infer_surge_bucket_from_alert(item: dict[str, Any]) -> str:
    """Compatibilidad: alertas viejas sin surge_bucket se infieren desde growth."""
    raw = item.get("surge_bucket")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    g = item.get("growth")
    try:
        return surge_growth_bucket(float(g))
    except (TypeError, ValueError):
        return "surge_50"


def _dedup_key(alert: dict[str, Any]) -> tuple[str, str, str]:
    """Clave de deduplicación: surge usa (narrative, type, bucket); el resto bucket ''."""
    n = alert["narrative"]
    t = alert["type"]
    if t == "surge":
        b = alert.get("surge_bucket")
        if isinstance(b, str) and b.strip():
            return (n, t, b.strip())
        try:
            return (n, t, surge_growth_bucket(float(alert["growth"])))
        except (TypeError, ValueError, KeyError):
            return (n, t, "surge_50")
    return (n, t, "")


def find_latest_narratives_json() -> Path:
    candidates = sorted(
        DATA_NARRATIVES_DIR.glob("narratives_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No hay narratives_*.json en {DATA_NARRATIVES_DIR}")
    return candidates[0]


def normalize_narrative_key(label: str) -> str:
    """Misma regla que persist_narrative_history: strip + colapso de espacios."""
    return re.sub(r"\s+", " ", (label or "").strip())


def find_latest_lifecycle_json() -> Path | None:
    if not DATA_LIFECYCLE_DIR.is_dir():
        return None
    files = sorted(
        DATA_LIFECYCLE_DIR.glob("lifecycle_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def lifecycle_key_sets_from_payload(lc: dict[str, Any]) -> tuple[set[str], set[str]]:
    """
    Claves de narrativa para fase NEW (lista `new`) y RISING (`rising`).
    Solo estas dos fases enriquecen alertas en este slice.
    """
    new_keys: set[str] = set()
    rising_keys: set[str] = set()

    for item in lc.get("new") or []:
        if not isinstance(item, dict):
            continue
        raw = item.get("narrative_key")
        if isinstance(raw, str) and raw.strip():
            new_keys.add(normalize_narrative_key(raw))
        else:
            n = item.get("narrative")
            if isinstance(n, str) and n.strip():
                new_keys.add(normalize_narrative_key(n))

    for item in lc.get("rising") or []:
        if not isinstance(item, dict):
            continue
        raw = item.get("narrative_key")
        if isinstance(raw, str) and raw.strip():
            rising_keys.add(normalize_narrative_key(raw))
        else:
            n = item.get("narrative")
            if isinstance(n, str) and n.strip():
                rising_keys.add(normalize_narrative_key(n))

    return new_keys, rising_keys


def load_latest_lifecycle_key_sets() -> tuple[set[str], set[str]] | None:
    path = find_latest_lifecycle_json()
    if path is None or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return lifecycle_key_sets_from_payload(data)


def enrich_alerts_with_lifecycle(
    alerts: list[dict[str, Any]],
    *,
    new_keys: set[str] | None = None,
    rising_keys: set[str] | None = None,
) -> int:
    """
    Añade opcionalmente ``lifecycle: {"phase": "new"|"rising"}`` por narrativa.
    Retorna cuántas alertas se enriquecieron.
    Si no se pasan sets, intenta leer el último lifecycle_*.json.
    """
    if new_keys is None and rising_keys is None:
        loaded = load_latest_lifecycle_key_sets()
        if loaded is None:
            return 0
        new_keys, rising_keys = loaded
    else:
        new_keys = new_keys if new_keys is not None else set()
        rising_keys = rising_keys if rising_keys is not None else set()

    n = 0
    for a in alerts:
        if not isinstance(a, dict):
            continue
        name = (a.get("narrative") or "").strip()
        if not name:
            continue
        k = normalize_narrative_key(name)
        if not k:
            continue
        if k in new_keys:
            a["lifecycle"] = {"phase": "new"}
            n += 1
        elif k in rising_keys:
            a["lifecycle"] = {"phase": "rising"}
            n += 1
    return n


def find_previous_narratives_json() -> Path | None:
    """Segundo narratives_*.json más reciente (para comparar SURGE), o None."""
    candidates = sorted(
        DATA_NARRATIVES_DIR.glob("narratives_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if len(candidates) < 2:
        return None
    return candidates[1]


def load_previous_dedup_keys() -> set[tuple[str, str, str]]:
    """
    Unión de claves en todos los alerts_*.json.
    early/momentum: (narrative, type, '').
    surge: (narrative, 'surge', bucket) — permite varios surges por narrativa en buckets distintos.
    """
    if not DATA_ALERTS_DIR.is_dir():
        return set()
    keys: set[tuple[str, str, str]] = set()
    for path in DATA_ALERTS_DIR.glob("alerts_*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        alerts = payload.get("alerts")
        if not isinstance(alerts, list):
            continue
        for item in alerts:
            if not isinstance(item, dict):
                continue
            nar = item.get("narrative")
            at = item.get("type")
            if not isinstance(nar, str) or not isinstance(at, str):
                continue
            ns, ts = nar.strip(), at.strip()
            if ts == "surge":
                keys.add((ns, ts, _infer_surge_bucket_from_alert(item)))
            else:
                keys.add((ns, ts, ""))
    return keys


def _strength(n: dict[str, Any]) -> float:
    v = n.get("narrative_strength")
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def narratives_list_to_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """nombre narrativa -> fila (última gana si hay duplicados)."""
    m: dict[str, dict[str, Any]] = {}
    for n in rows:
        if not isinstance(n, dict):
            continue
        name = (n.get("narrative") or "").strip()
        if name:
            m[name] = n
    return m


def build_surge_candidates(
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
    created_at: str,
) -> list[dict[str, Any]]:
    """
    Compara narrative_strength entre dos snapshots.
    growth = (current - previous) / previous; alerta si growth >= SURGE_GROWTH_THRESHOLD.
    Requiere previous_strength > 0.
    """
    current_map = narratives_list_to_map(current_rows)
    previous_map = narratives_list_to_map(previous_rows)
    out: list[dict[str, Any]] = []

    for name, cur_row in current_map.items():
        if name not in previous_map:
            continue
        prev_strength = _strength(previous_map[name])
        cur_strength = _strength(cur_row)
        if prev_strength <= 0:
            continue
        growth = (cur_strength - prev_strength) / prev_strength
        if growth < SURGE_GROWTH_THRESHOLD:
            continue

        bucket = surge_growth_bucket(growth)
        alert_id = hashlib.sha256(
            f"surge|{name}|{bucket}|{created_at}|{round(cur_strength, 4)}|{round(prev_strength, 4)}".encode(
                "utf-8",
            ),
        ).hexdigest()
        out.append(
            {
                "alert_id": alert_id,
                "type": "surge",
                "narrative": name,
                "surge_bucket": bucket,
                "previous_strength": round(prev_strength, 4),
                "current_strength": round(cur_strength, 4),
                "growth": round(growth, 4),
                "created_at": created_at,
            },
        )
    return out


def build_candidate_alerts(narratives: list[dict[str, Any]], created_at: str) -> list[dict[str, Any]]:
    """Alertas early_opportunity / confirmed_momentum sin filtrar duplicados."""
    out: list[dict[str, Any]] = []
    for n in narratives:
        if not isinstance(n, dict):
            continue
        nt = (n.get("type") or "").strip().lower()
        strength = _strength(n)
        narrative_name = (n.get("narrative") or "").strip()
        if not narrative_name:
            continue

        alert_type: str | None = None
        if nt == "early" and strength >= THRESHOLD_EARLY_OPPORTUNITY:
            alert_type = "early_opportunity"
        elif nt == "confirmed" and strength >= THRESHOLD_CONFIRMED_MOMENTUM:
            alert_type = "confirmed_momentum"

        if alert_type is None:
            continue

        alert_id = hashlib.sha256(
            f"{alert_type}|{narrative_name}|{created_at}".encode("utf-8"),
        ).hexdigest()
        out.append(
            {
                "alert_id": alert_id,
                "type": alert_type,
                "narrative": narrative_name,
                "narrative_strength": round(strength, 4),
                "total_articles": int(n.get("total_articles") or 0),
                "rss_count": int(n.get("rss_count") or 0),
                "reddit_count": int(n.get("reddit_count") or 0),
                "created_at": created_at,
            },
        )
    return out


def filter_new_alerts(
    candidates: list[dict[str, Any]],
    seen: set[tuple[str, str, str]],
) -> list[dict[str, Any]]:
    new_list: list[dict[str, Any]] = []
    for a in candidates:
        key = _dedup_key(a)
        if key in seen:
            continue
        new_list.append(a)
        seen.add(key)
    return new_list


def save_alerts_json(alerts: list[dict[str, Any]]) -> Path:
    DATA_ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    out_path = DATA_ALERTS_DIR / f"alerts_{stamp}.json"
    payload = {
        "saved_at": datetime.now(UTC).isoformat(),
        "alert_count": len(alerts),
        "alerts": alerts,
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return out_path


def print_alerts(alerts: list[dict[str, Any]]) -> None:
    for a in alerts:
        t = a["type"]
        if t == "surge":
            print("🔥 SURGE DETECTED")
            print(f"   {a['narrative']}")
            if isinstance(a.get("lifecycle"), dict) and a["lifecycle"].get("phase"):
                print(f"   lifecycle: {a['lifecycle']['phase']}")
            pct = float(a["growth"]) * 100
            print(
                f"   {a['previous_strength']} → {a['current_strength']} "
                f"(+{pct:.0f}%)",
            )
            print()
            continue
        if t == "early_opportunity":
            print("🚀 EARLY OPPORTUNITY")
        elif t == "confirmed_momentum":
            print("⚠️ CONFIRMED MOMENTUM")
        else:
            print(f"• {t}")
        print(f"   {a['narrative']}")
        if isinstance(a.get("lifecycle"), dict) and a["lifecycle"].get("phase"):
            print(f"   lifecycle: {a['lifecycle']['phase']}")
        print(f"   strength: {a['narrative_strength']}")
        print(
            f"   posts: {a['total_articles']} "
            f"(rss: {a['rss_count']}, reddit: {a['reddit_count']})",
        )
        print()


TELEGRAM_API_TIMEOUT_S = 20.0
TELEGRAM_TEXT_MAX = 4096
# Panel público por defecto (producción). Override local: export VIXION_DASHBOARD_URL=http://127.0.0.1:8080
_DEFAULT_DASHBOARD_URL = "https://xolid.ai/vixion"

RESEND_API_TIMEOUT_S = 25.0


def _resolve_dashboard_url() -> str:
    """
    URL del panel para Telegram / email.
    Si VIXION_DASHBOARD_URL está definida y no vacía → se usa tal cual (trim).
    Si no está definida o está vacía → https://xolid.ai/vixion
    """
    raw = os.environ.get("VIXION_DASHBOARD_URL")
    if raw is None:
        return _DEFAULT_DASHBOARD_URL
    s = raw.strip()
    return s if s else _DEFAULT_DASHBOARD_URL


def _telegram_header_block() -> str:
    """Líneas previas al cuerpo: hora UTC y enlace al panel (si aplica)."""
    now = datetime.now(UTC)
    lines: list[str] = [f"🕒 {now.strftime('%H:%M')} UTC"]

    dashboard = _resolve_dashboard_url()
    lines.append("")
    lines.append("🔗 View in dashboard:")
    lines.append(dashboard)

    return "\n".join(lines)


def format_lifecycle_line(alert: dict[str, Any]) -> str:
    """Línea opcional para digest (Telegram / email), sin romper formato legacy."""
    lc = alert.get("lifecycle")
    if not isinstance(lc, dict):
        return ""
    ph = lc.get("phase")
    if ph == "new":
        return "\nlifecycle: NEW"
    if ph == "rising":
        return "\nlifecycle: RISING"
    return ""


def format_alert_for_telegram(alert: dict[str, Any]) -> str:
    """Texto plano para sendMessage (sin parse_mode, evita romper con _ * en narrativas)."""
    t = alert.get("type")
    nar = (alert.get("narrative") or "").strip()
    suf = format_lifecycle_line(alert)

    if t == "early_opportunity":
        s = alert.get("narrative_strength")
        return f"🚀 EARLY\n{nar}\nstrength: {s}{suf}"

    if t == "confirmed_momentum":
        s = alert.get("narrative_strength")
        return f"⚠️ MOMENTUM\n{nar}\nstrength: {s}{suf}"

    if t == "surge":
        prev = alert.get("previous_strength")
        cur = alert.get("current_strength")
        try:
            g = float(alert.get("growth") or 0.0)
        except (TypeError, ValueError):
            g = 0.0
        pct = round(g * 100)
        return f"🔥 SURGE\n{nar}\n{prev} → {cur}\n(+{pct}%){suf}"

    return f"VIXION\n{t}\n{nar}{suf}"


def _send_telegram_message(token: str, chat_id: str, text: str) -> None:
    if len(text) > TELEGRAM_TEXT_MAX:
        text = text[: TELEGRAM_TEXT_MAX - 3] + "..."
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    res = requests.post(
        url,
        json={"chat_id": chat_id, "text": text},
        timeout=TELEGRAM_API_TIMEOUT_S,
    )
    res.raise_for_status()
    body = res.json()
    if not body.get("ok"):
        raise ValueError(body.get("description") or "Telegram API ok=false")


def send_new_alerts_to_telegram(alerts: list[dict[str, Any]]) -> None:
    """
    Un solo mensaje por ejecución (agrupa varias alertas).
    Arriba: 🕒 HH:MM UTC y enlace al panel (VIXION_DASHBOARD_URL o default xolid.ai).
    Si solo hay una alerta, sin cabecera «VIXION ALERTS» en el cuerpo.
    Requiere TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID; errores → [WARN] en stderr.
    """
    if not alerts:
        return

    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        return

    blocks: list[str] = []
    for a in alerts:
        if isinstance(a, dict):
            blocks.append(format_alert_for_telegram(a))

    if not blocks:
        return

    if len(blocks) == 1:
        body = blocks[0]
    else:
        body = "VIXION ALERTS\n\n" + "\n\n".join(blocks)

    text = _telegram_header_block() + "\n\n" + body

    try:
        _send_telegram_message(token, chat_id, text)
    except (OSError, requests.RequestException, ValueError) as exc:
        print(f"[WARN] Telegram: no se envió el digest: {exc}", file=sys.stderr)


def build_email_digest_text(alerts: list[dict[str, Any]]) -> str:
    """Cuerpo texto plano: mismo bloque por alerta que Telegram + enlace opcional."""
    blocks: list[str] = ["VIXION ALERTS", ""]
    for a in alerts:
        if isinstance(a, dict):
            blocks.append(format_alert_for_telegram(a))
            blocks.append("")
    while blocks and blocks[-1] == "":
        blocks.pop()
    dash = _resolve_dashboard_url()
    blocks.extend(["", "View dashboard:", dash])
    return "\n".join(blocks)


def send_new_alerts_via_resend(alerts: list[dict[str, Any]]) -> None:
    """
    Digest por email (Resend) si RESEND_API_KEY, ALERTS_EMAIL_FROM y ALERTS_EMAIL_TO están definidos.
    Sin variables → no-op. Errores → [WARN] en stderr.
    """
    if not alerts:
        return

    api_key = (os.environ.get("RESEND_API_KEY") or "").strip()
    from_addr = (os.environ.get("ALERTS_EMAIL_FROM") or "").strip()
    to_addr = (os.environ.get("ALERTS_EMAIL_TO") or "").strip()
    if not api_key or not from_addr or not to_addr:
        return

    n = len(alerts)
    subject = f"VIXION Alerts — {n} new signal" + ("s" if n != 1 else "")
    text = build_email_digest_text(alerts)

    url = "https://api.resend.com/emails"
    try:
        res = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": from_addr,
                "to": [to_addr],
                "subject": subject,
                "text": text,
            },
            timeout=RESEND_API_TIMEOUT_S,
        )
        res.raise_for_status()
    except (OSError, requests.RequestException, ValueError) as exc:
        print(f"[WARN] Resend: no se envió el email: {exc}", file=sys.stderr)


def main() -> None:
    try:
        nar_path = find_latest_narratives_json()
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    prev_nar_path = find_previous_narratives_json()

    try:
        payload = json.loads(nar_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        print(f"[ERROR] No se pudo leer {nar_path}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    narratives = payload.get("narratives")
    if not isinstance(narratives, list):
        print("[ERROR] El JSON no contiene lista 'narratives'.", file=sys.stderr)
        raise SystemExit(1)

    created_at = datetime.now(UTC).isoformat()
    candidates = build_candidate_alerts(narratives, created_at)

    surge_count = 0
    if prev_nar_path is not None:
        try:
            prev_payload = json.loads(prev_nar_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            print(f"[WARN] No se pudo leer narrativo anterior {prev_nar_path}: {exc}", file=sys.stderr)
        else:
            prev_list = prev_payload.get("narratives")
            if isinstance(prev_list, list):
                surge_cands = build_surge_candidates(narratives, prev_list, created_at)
                surge_count = len(surge_cands)
                candidates.extend(surge_cands)
    else:
        print(
            "[INFO] Solo hay un narratives_*.json; SURGE requiere al menos dos corridas de detect_narratives.",
            file=sys.stderr,
        )

    prev_keys = load_previous_dedup_keys()
    new_alerts = filter_new_alerts(candidates, set(prev_keys))

    enriched_n = enrich_alerts_with_lifecycle(new_alerts)

    print(f"Narrativas actuales: {nar_path}")
    if prev_nar_path:
        print(f"Narrativas anteriores (SURGE): {prev_nar_path}")
    print(
        f"Candidatos: {len(candidates)} "
        f"(incl. surge: {surge_count}) · Nuevos (sin duplicar): {len(new_alerts)}",
    )
    if enriched_n:
        print(
            f"Lifecycle: {enriched_n} alerta(s) etiquetadas (NEW / RISING) desde último lifecycle.",
        )
    print()

    if not new_alerts:
        print("No hay alertas nuevas (sin candidatos o ya vistas en data/alerts/).")
    else:
        print_alerts(new_alerts)

    try:
        out_path = save_alerts_json(new_alerts)
    except OSError as exc:
        print(f"[ERROR] No se pudo guardar: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Archivo generado: {out_path}")

    send_new_alerts_to_telegram(new_alerts)
    send_new_alerts_via_resend(new_alerts)


if __name__ == "__main__":
    main()

"""Gate de idioma: corpus operativo Phase 1 = English only."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LocaleDecision:
    accepted: bool
    language_code: str  # 'en' | 'und'
    reason: str


def assess_locale(text: str) -> LocaleDecision:
    """
    Detecta idioma con langdetect si está disponible; si no, heurística ASCII conservadora.
    No-EN con confianza suficiente => rechazo.
    """
    sample = (text or "").strip()
    if not sample:
        return LocaleDecision(True, "en", "empty_defaults_en")

    try:
        from langdetect import LangDetectException, detect_langs

        try:
            scores = detect_langs(sample[:5000])
        except LangDetectException:
            return LocaleDecision(True, "und", "langdetect_failed_default_accept")

        if not scores:
            return LocaleDecision(True, "und", "no_scores")

        top = scores[0]
        if top.lang == "en" and top.prob >= 0.85:
            return LocaleDecision(True, "en", f"langdetect_en_{top.prob:.2f}")
        if top.lang != "en" and top.prob >= 0.90:
            return LocaleDecision(False, top.lang, f"langdetect_reject_{top.lang}_{top.prob:.2f}")
        # Zona gris: conservador para no inglés débil
        if top.lang == "en":
            return LocaleDecision(True, "en", f"langdetect_en_weak_{top.prob:.2f}")
        return LocaleDecision(False, top.lang, f"langdetect_reject_weak_{top.lang}_{top.prob:.2f}")
    except ImportError:
        return _ascii_heuristic(sample)


def _ascii_heuristic(sample: str) -> LocaleDecision:
    letters = [c for c in sample if c.isalpha()]
    if not letters:
        return LocaleDecision(True, "und", "no_letters")
    non_ascii = sum(1 for c in letters if ord(c) > 127)
    ratio = non_ascii / len(letters)
    if ratio > 0.08:
        return LocaleDecision(False, "und", f"non_ascii_ratio_{ratio:.2f}")
    return LocaleDecision(True, "en", f"ascii_heuristic_ratio_{ratio:.2f}")

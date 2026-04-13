"""Lógica de rachas operativas (sin DB)."""

from __future__ import annotations

import pytest

from vixion.ops.observability import leading_non_success_streak, thresholds


def test_leading_non_success_streak():
    runs = [
        {"status": "failed"},
        {"status": "partial"},
        {"status": "success"},
        {"status": "failed"},
    ]
    assert leading_non_success_streak(runs) == 2
    assert leading_non_success_streak([{"status": "success"}]) == 0
    assert leading_non_success_streak([{"status": "failed"}, {"status": "failed"}]) == 2


def test_thresholds_defaults(monkeypatch: pytest.MonkeyPatch):
    for k in (
        "VIXION_OPS_PENDING_WARN",
        "VIXION_OPS_FAILED_WARN",
        "VIXION_OPS_DEAD_WARN",
        "VIXION_OPS_INGEST_BAD_STREAK",
    ):
        monkeypatch.delenv(k, raising=False)
    t = thresholds()
    assert t["pending_jobs"] == 200
    assert t["ingest_non_success_streak"] == 3

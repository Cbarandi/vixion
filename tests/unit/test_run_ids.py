from __future__ import annotations

from datetime import UTC

from vixion.utils.run_ids import parse_saved_at_utc, run_id_from_saved_at


def test_run_id_from_saved_at_with_offset() -> None:
    assert run_id_from_saved_at("2026-04-14T12:15:40.702755+00:00") == "20260414_121540_702755"


def test_run_id_from_saved_at_with_z_suffix() -> None:
    assert run_id_from_saved_at("2026-04-14T12:15:40.702755Z") == "20260414_121540_702755"


def test_parse_saved_at_utc_from_naive() -> None:
    dt = parse_saved_at_utc("2026-04-14T12:15:40.702755")
    assert dt.tzinfo == UTC
    assert dt.strftime("%Y%m%d_%H%M%S_%f") == "20260414_121540_702755"

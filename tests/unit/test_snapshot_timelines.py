"""Tests para series de strength desde snapshots indexados."""

from __future__ import annotations

import json
from pathlib import Path

from vixion.ops.snapshot_timelines import build_snapshot_timelines_payload


def _write_snapshot(path: Path, narratives: list[dict]) -> None:
    path.write_text(
        json.dumps({"saved_at": "t", "narratives": narratives}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_build_timelines_from_runs_and_snapshots(tmp_path: Path) -> None:
    root = tmp_path
    snap_dir = root / "data/narrative_history/snapshots"
    snap_dir.mkdir(parents=True)
    idx_path = root / "data/narrative_history" / "runs.jsonl"
    idx_path.parent.mkdir(parents=True, exist_ok=True)

    _write_snapshot(
        snap_dir / "run_a.json",
        [
            {"narrative": "Alpha theme", "narrative_strength": 10.0},
            {"narrative": "Beta theme", "narrative_strength": 5.0},
        ],
    )
    _write_snapshot(
        snap_dir / "run_b.json",
        [
            {"narrative": "Alpha theme", "narrative_strength": 8.0},
            {"narrative": "Beta theme", "narrative_strength": 7.0},
        ],
    )
    _write_snapshot(
        snap_dir / "run_c.json",
        [
            {"narrative": "Alpha theme", "narrative_strength": 12.0},
            {"narrative": "Beta theme", "narrative_strength": 4.0},
        ],
    )

    lines = [
        json.dumps(
            {
                "run_id": "run_a",
                "saved_at": "2026-01-01T00:00:00Z",
                "snapshot_path": "data/narrative_history/snapshots/run_a.json",
            },
            ensure_ascii=False,
        ),
        json.dumps(
            {
                "run_id": "run_b",
                "saved_at": "2026-01-02T00:00:00Z",
                "snapshot_path": "data/narrative_history/snapshots/run_b.json",
            },
            ensure_ascii=False,
        ),
        json.dumps(
            {
                "run_id": "run_c",
                "saved_at": "2026-01-03T00:00:00Z",
                "snapshot_path": "data/narrative_history/snapshots/run_c.json",
            },
            ensure_ascii=False,
        ),
    ]
    idx_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    out = build_snapshot_timelines_payload(root, max_runs=8, max_narratives=6)
    assert len(out["runs"]) == 3
    assert {r["run_id"] for r in out["runs"]} == {"run_a", "run_b", "run_c"}

    by_key = {t["narrative_key"]: t for t in out["timelines"]}
    assert "Alpha theme" in by_key
    alpha_pts = by_key["Alpha theme"]["points"]
    assert [p["strength"] for p in alpha_pts if p is not None] == [10.0, 8.0, 12.0]


def test_sparse_missing_narrative_in_one_run(tmp_path: Path) -> None:
    root = tmp_path
    snap_dir = root / "data/narrative_history/snapshots"
    snap_dir.mkdir(parents=True)
    idx_path = root / "data/narrative_history/runs.jsonl"
    idx_path.parent.mkdir(parents=True, exist_ok=True)

    _write_snapshot(snap_dir / "r1.json", [{"narrative": "Gamma", "narrative_strength": 3.0}])
    _write_snapshot(snap_dir / "r2.json", [{"narrative": "Other", "narrative_strength": 9.0}])
    _write_snapshot(snap_dir / "r3.json", [{"narrative": "Gamma", "narrative_strength": 4.0}])

    idx_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "run_id": "r1",
                        "saved_at": "t",
                        "snapshot_path": "data/narrative_history/snapshots/r1.json",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "run_id": "r2",
                        "saved_at": "t",
                        "snapshot_path": "data/narrative_history/snapshots/r2.json",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "run_id": "r3",
                        "saved_at": "t",
                        "snapshot_path": "data/narrative_history/snapshots/r3.json",
                    },
                    ensure_ascii=False,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = build_snapshot_timelines_payload(root, max_runs=8, max_narratives=6)
    gamma = next(t for t in out["timelines"] if t["narrative_key"] == "Gamma")
    assert gamma["points"][0]["strength"] == 3.0
    assert gamma["points"][1] is None
    assert gamma["points"][2]["strength"] == 4.0


def test_no_index_returns_empty(tmp_path: Path) -> None:
    out = build_snapshot_timelines_payload(tmp_path, max_runs=8, max_narratives=6)
    assert out["runs"] == []
    assert out["timelines"] == []
    assert out["meta"].get("empty_reason") == "no_runs_index_or_snapshots"

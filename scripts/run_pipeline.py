#!/usr/bin/env python3
"""
Pipeline completo VIXION: ingest RSS/Reddit → normalize → classify → merge → score →
narrativas → alertas.

Uso:
  python scripts/run_pipeline.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = PROJECT_ROOT / "scripts"
PY = sys.executable


def _cmd(script: str, *args: str) -> list[str]:
    return [PY, str(SCRIPTS / script), *args]


# Orden fijo del pipeline
STEPS: list[tuple[str, list[str]]] = [
    ("rss_ingest", _cmd("rss_ingest.py")),
    ("reddit_ingest", _cmd("reddit_ingest.py")),
    ("normalize_rss", _cmd("normalize_rss.py")),
    ("normalize_reddit", _cmd("normalize_reddit.py")),
    ("classify_narratives (rss)", _cmd("classify_narratives.py", "--source", "rss")),
    ("classify_narratives (reddit)", _cmd("classify_narratives.py", "--source", "reddit")),
    ("merge_sources", _cmd("merge_sources.py")),
    ("score_merged", _cmd("score_merged.py")),
    ("detect_narratives", _cmd("detect_narratives.py")),
    ("persist_narrative_history", _cmd("persist_narrative_history.py")),
    ("persist_market_context", _cmd("persist_market_context.py")),
    ("compute_forward_returns", _cmd("compute_forward_returns.py")),
    ("classify_narrative_lifecycle", _cmd("classify_narrative_lifecycle.py")),
    ("aggregate_narrative_outcomes", _cmd("aggregate_narrative_outcomes.py")),
    ("rank_narrative_edge", _cmd("rank_narrative_edge.py")),
    ("generate_alerts", _cmd("generate_alerts.py")),
]


def main() -> int:
    print(f"VIXION pipeline · raíz: {PROJECT_ROOT}")
    print("=" * 60)

    t0_total = time.perf_counter()
    durations: list[tuple[str, float]] = []

    for i, (name, cmd) in enumerate(STEPS, start=1):
        print(f"\n[{i}/{len(STEPS)}] ▶ {name}")
        print(f"    $ {' '.join(cmd)}")
        sys.stdout.flush()

        t0 = time.perf_counter()
        try:
            subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            elapsed = time.perf_counter() - t0
            print()
            print("=" * 60)
            print(f"✖ PIPELINE DETENIDO en paso {i}/{len(STEPS)}: {name}")
            print(f"  Código de salida: {exc.returncode}")
            print(f"  Duración de este paso: {elapsed:.2f}s")
            print("=" * 60)
            return int(exc.returncode) if exc.returncode else 1
        except OSError as exc:
            elapsed = time.perf_counter() - t0
            print()
            print("=" * 60)
            print(f"✖ No se pudo ejecutar el paso {i}/{len(STEPS)}: {name}")
            print(f"  {exc}")
            print(f"  Duración de este paso: {elapsed:.2f}s")
            print("=" * 60)
            return 1

        elapsed = time.perf_counter() - t0
        durations.append((name, elapsed))
        print(f"    ✓ OK ({elapsed:.2f}s)")

    total = time.perf_counter() - t0_total
    print()
    print("=" * 60)
    print("✓ PIPELINE COMPLETADO")
    print(f"  Tiempo total: {total:.2f}s")
    print("  Por paso:")
    for name, sec in durations:
        print(f"    · {name}: {sec:.2f}s")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

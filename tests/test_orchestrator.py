from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orchestrator import run_once


def test_orchestrator_runs_and_logs(tmp_path: Path) -> None:
    src = ROOT

    for folder in ["config", "data", "engines", "execution", "risk", "state", "logs"]:
        (tmp_path / folder).mkdir(parents=True, exist_ok=True)

    shutil.copy(src / "config" / "config.yaml", tmp_path / "config" / "config.yaml")

    result = run_once(tmp_path)
    assert "timestamp" in result

    decisions = tmp_path / "logs" / "decisions.log"
    pnl = tmp_path / "logs" / "pnl.log"
    system_state = tmp_path / "state" / "system_state.json"

    assert decisions.exists()
    assert pnl.exists()
    assert system_state.exists()

    state = json.loads(system_state.read_text(encoding="utf-8"))
    assert "violation_streak" in state

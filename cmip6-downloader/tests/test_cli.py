"""Executable wrapper smoke tests."""

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("script", ["compare_catalogs.py", "run_download.py"])
def test_cli_help(script: str) -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / script), "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "usage:" in result.stdout

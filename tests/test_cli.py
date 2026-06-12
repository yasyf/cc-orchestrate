from __future__ import annotations

import pytest
from click.testing import CliRunner

from cc_orchestrate.cli import main


def test_help_exits_cleanly() -> None:
    result = CliRunner().invoke(main, ["--help"])
    assert result.exit_code == 0
    assert result.output.startswith("Usage: main")


def test_backends_reports_availability(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "cc_orchestrate.cli.shutil.which",
        lambda name: "/usr/local/bin/cmux" if name == "cmux" else None,
    )
    result = CliRunner().invoke(main, ["backends"])
    assert result.exit_code == 0
    assert result.output == "cmux\tavailable\nsuperset\tnot found\n"

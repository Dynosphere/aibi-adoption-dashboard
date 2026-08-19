"""Test: required bundle variables exist with correct defaults."""
from pathlib import Path
import yaml


def _load_vars(repo_root: Path) -> dict:
    return yaml.safe_load(
        (repo_root / "deployment_resources" / "variables.yml").read_text()
    )["variables"]


def test_lookback_days_default_365(repo_root: Path) -> None:
    vars_ = _load_vars(repo_root)
    assert "lookback_days" in vars_, "lookback_days must be declared"
    assert int(vars_["lookback_days"]["default"]) == 365

"""Test: required V3 bundle variables exist with correct defaults."""

from pathlib import Path

import yaml


def _load_vars(repo_root: Path) -> dict:
    return yaml.safe_load(
        (repo_root / "deployment_resources" / "variables.yml").read_text()
    )["variables"]


def test_enable_genie_deep_dive_default_false(repo_root: Path) -> None:
    vars_ = _load_vars(repo_root)
    assert "enable_genie_deep_dive" in vars_, "enable_genie_deep_dive must be declared"
    assert vars_["enable_genie_deep_dive"]["default"] in (False, "false")

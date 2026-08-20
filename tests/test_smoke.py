"""Smoke test: confirms pytest + project layout work."""

from pathlib import Path


def test_repo_root_has_databricks_yml(repo_root: Path) -> None:
    assert (repo_root / "databricks.yml").is_file()

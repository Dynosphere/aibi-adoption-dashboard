"""Shared pytest fixtures for the aibi-adoption-dashboard test suite."""

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def repo_root() -> Path:
    """Absolute path to the repo root."""
    return REPO_ROOT


@pytest.fixture
def in_workspace() -> bool:
    """True when running in a Databricks workspace runtime (used to gate integration tests)."""
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

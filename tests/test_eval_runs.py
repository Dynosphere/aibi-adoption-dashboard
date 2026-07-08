"""Test: eval_runs.fetch_for_space returns flat dicts from the SDK fan-out."""
from unittest.mock import MagicMock
from src.includes.eval_runs import fetch_for_space


def test_fetch_returns_one_dict_per_run() -> None:
    w = MagicMock()
    run = MagicMock()
    run.eval_run_id     = "r-123"
    run.created_at      = "2026-06-10T09:00:00Z"
    run.created_by      = "alice@example.com"
    run.accuracy        = 0.82
    run.eval_run_status = "COMPLETED"
    w.genie.genie_list_eval_runs.return_value = [run]

    out = fetch_for_space(w, "space-abc")
    assert out == [{
        "eval_run_id": "r-123",
        "space_id":    "space-abc",
        "created_at":  "2026-06-10T09:00:00Z",
        "created_by":  "alice@example.com",
        "accuracy":    0.82,
        "status":      "COMPLETED",
    }]


def test_fetch_returns_empty_on_no_benchmarks() -> None:
    w = MagicMock()
    w.genie.genie_list_eval_runs.return_value = []
    assert fetch_for_space(w, "space-empty") == []

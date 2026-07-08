"""Snapshot Genie benchmark eval-run summaries via SDK fan-out.

Per-question detail expires upstream at 7 days, so we MUST snapshot
into a Delta dim if we want a > 7d accuracy trend. Per-run summaries
(timestamp, accuracy %, status) persist longer in the Evaluations tab
but are only API-accessible per-space.
"""

from __future__ import annotations


def fetch_for_space(w, space_id: str) -> list[dict]:
    """Return one flat dict per eval run on `space_id`."""
    out: list[dict] = []
    try:
        for run in w.genie.genie_list_eval_runs(space_id=space_id):
            out.append({
                "eval_run_id": run.eval_run_id,
                "space_id":   space_id,
                "created_at": run.created_at,
                "created_by": getattr(run, "created_by", None),
                "accuracy":   getattr(run, "accuracy", None),
                "status":     getattr(run, "eval_run_status", None),
            })
    except Exception as exc:  # noqa: BLE001 — public Beta API, tolerate per-space errors
        print(f"eval_runs.fetch_for_space({space_id}): {exc}")
    return out

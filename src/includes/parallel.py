"""Bounded ThreadPoolExecutor wrapper for SDK fan-out.

Honours the public Databricks rate limits:
  - Permissions API GET: 100/s/workspace
  - Lakeview list:       safe at 5-8 concurrent
  - Genie list:          5-8 concurrent (per public API guidance)

Per-item errors are collected so a single 403/404 does not abort the batch.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Sequence


def run(
    fn: Callable,
    items: Sequence,
    max_workers: int = 5,
    return_errors: bool = False,
):
    """Apply ``fn`` to every item in ``items`` concurrently.

    Each invocation that returns an iterable (list/tuple/generator) is
    flattened into the result; scalar returns are appended as-is.

    Parameters
    ----------
    fn:
        Callable applied to each item. May return a scalar, list, or tuple.
    items:
        Sequence of inputs to fan out over.
    max_workers:
        Thread pool size. Defaults to 5 (safe for Genie rate limits).
        Use 8 for Lakeview / serving_endpoints / apps.
    return_errors:
        When True, returns a ``(results, errors)`` tuple instead of
        printing a summary and returning only results.
    """
    results: list = []
    errors: list = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fn, item): item for item in items}
        for fut in as_completed(futures):
            try:
                out = fut.result()
                if isinstance(out, (list, tuple)):
                    results.extend(out)
                elif out is not None:
                    results.append(out)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
    if return_errors:
        return results, errors
    if errors:
        print(
            f"parallel.run: {len(errors)} item(s) failed (continuing). "
            f"First error: {errors[0]}"
        )
    return results

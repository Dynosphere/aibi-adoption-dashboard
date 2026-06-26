"""Test: parallel.run executes concurrently and surfaces per-item errors."""

import time

from src.includes.parallel import run


def test_executes_concurrently() -> None:
    def slow(x: int) -> int:
        time.sleep(0.1)
        return x * 2

    start = time.monotonic()
    out = run(slow, list(range(10)), max_workers=5)
    duration = time.monotonic() - start
    assert sorted(out) == [x * 2 for x in range(10)]
    # 10 items @ 0.1s sequential = 1.0s; with 5 workers, well under 0.5s.
    assert duration < 0.5, f"expected concurrent run, took {duration:.2f}s"


def test_collects_per_item_errors() -> None:
    def maybe_fail(x: int) -> int:
        if x == 2:
            raise ValueError(f"boom {x}")
        return x

    results, errors = run(maybe_fail, [1, 2, 3], max_workers=2, return_errors=True)
    assert sorted(results) == [1, 3]
    assert len(errors) == 1
    assert "boom 2" in str(errors[0])

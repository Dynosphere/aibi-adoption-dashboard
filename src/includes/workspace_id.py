"""Resolve the current workspace_id once, from system.access.workspaces_latest.

The result is used as a partition key on every `adb_*` and `mvFact*` table so
two deployments writing to a shared catalog don't collide.
"""

from __future__ import annotations

from urllib.parse import urlparse


def resolve(spark, w) -> tuple[int, str, str]:
    """Return ``(workspace_id, workspace_name, workspace_url)``.

    Primary source: ``system.access.workspaces_latest`` joined on ``workspace_url``.
    Fallback: the host on ``w.config.host`` (workspace_id = 0, name derived from host).
    """
    host = (w.config.host or "").rstrip("/")
    try:
        row = spark.sql(
            f"""
            SELECT workspace_id, workspace_name, workspace_url
            FROM system.access.workspaces_latest
            WHERE workspace_url = '{host}'
            LIMIT 1
            """
        ).first()
        if row is not None:
            return int(row.workspace_id), row.workspace_name, row.workspace_url
    except Exception:  # noqa: BLE001 - fallback is intentional
        pass
    # Fallback path — uniquely identifies the workspace by host even if the
    # system table is unavailable in this UC region.
    netloc = urlparse(host).netloc or host
    return 0, netloc, host

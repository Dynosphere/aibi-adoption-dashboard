"""V3 pre-flight: DESCRIBE EXTENDED every system table referenced in the spec.

Writes the consolidated output to `docs/v3-system-table-validation.md`.

Usage:
    DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com \
    DATABRICKS_TOKEN=... \
    python scripts/validate_system_tables.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent

from databricks.sdk import WorkspaceClient

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = REPO_ROOT / "docs" / "v3-system-table-validation.md"

REQUIRED_TABLES = [
    "system.access.audit",
    "system.access.workspaces_latest",
    "system.access.assistant_events",
    "system.access.table_lineage",
    "system.query.history",
    "system.compute.warehouse_events",
    "system.billing.usage",
    "system.billing.list_prices",
    "system.serving.served_entities",
    "system.serving.endpoint_usage",
    "system.ai_gateway.usage",
    "system.lakeflow.jobs",
    "system.lakeflow.job_run_timeline",
    "system.information_schema.tables",
]


def describe(w: WorkspaceClient, warehouse_id: str, table: str) -> tuple[str, list[dict]]:
    """Return (status, rows) for a DESCRIBE EXTENDED on `table`."""
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE EXTENDED {table}",
            wait_timeout="30s",
        )
        rows = [
            dict(zip([c.name for c in result.manifest.schema.columns], r))
            for r in (result.result.data_array or [])
        ]
        return "OK", rows
    except Exception as exc:  # noqa: BLE001 - we want every failure recorded
        return f"ERROR: {exc.__class__.__name__}: {exc}", []


def sample_billing_origin_products(w: WorkspaceClient, warehouse_id: str) -> list[str]:
    """Distinct values of billing_origin_product in the last 30 days."""
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=dedent(
                """
                SELECT DISTINCT billing_origin_product
                FROM system.billing.usage
                WHERE usage_date >= current_date() - INTERVAL 30 DAYS
                ORDER BY 1
                """
            ).strip(),
            wait_timeout="30s",
        )
        return sorted({(r[0] or "<null>") for r in (result.result.data_array or [])})
    except Exception as exc:  # noqa: BLE001
        return [f"ERROR: {exc}"]


def main() -> int:
    w = WorkspaceClient()

    # Pick a warehouse: prefer one named via DATABRICKS_WAREHOUSE_ID env var, then
    # any "Serverless Starter Warehouse", then any RUNNING warehouse.
    import os

    warehouses = list(w.warehouses.list())
    starter = None

    env_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if env_id:
        starter = next((wh for wh in warehouses if wh.id == env_id), None)

    if starter is None:
        starter = next(
            (wh for wh in warehouses if wh.name == "Serverless Starter Warehouse"),
            None,
        )

    if starter is None:
        # Fall back to any RUNNING warehouse so the validation can proceed.
        starter = next(
            (wh for wh in warehouses if str(wh.state) == "State.RUNNING"),
            None,
        )

    if starter is None:
        print(
            "ERROR: no usable warehouse found. Set DATABRICKS_WAREHOUSE_ID or "
            "start at least one warehouse in the workspace.",
            file=sys.stderr,
        )
        return 1
    warehouse_id = starter.id

    lines: list[str] = []
    lines.append("# V3 system-table validation\n")
    lines.append(f"**Workspace:** {w.config.host}\n")
    lines.append(f"**Warehouse:** {starter.name} (`{warehouse_id}`)\n")
    lines.append(f"**Generated:** by `scripts/validate_system_tables.py`\n\n")
    lines.append("This document is the **source of truth** for column names/types ")
    lines.append("when V3 MERGE statements reference system tables. Update it whenever ")
    lines.append("the spec adds a new system-table dependency.\n\n")

    for table in REQUIRED_TABLES:
        status, rows = describe(w, warehouse_id, table)
        lines.append(f"## `{table}` — {status}\n\n")
        if rows:
            lines.append("| col_name | data_type | comment |\n|---|---|---|\n")
            for r in rows:
                col = (r.get("col_name") or "").strip()
                if not col or col.startswith("#"):
                    continue
                dt = (r.get("data_type") or "").strip()
                cm = (r.get("comment") or "").strip().replace("|", "\\|")
                lines.append(f"| `{col}` | `{dt}` | {cm} |\n")
            lines.append("\n")

    # billing_origin_product probe
    lines.append("## Observed `billing_origin_product` values (last 30 days)\n\n")
    for v in sample_billing_origin_products(w, warehouse_id):
        lines.append(f"- `{v}`\n")
    lines.append("\n")

    OUTPUT.write_text("".join(lines))
    print(f"Wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

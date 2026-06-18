"""Helpers for workspace-scoped Delta MERGEs.

Replaces the V2 pattern `df.write.mode("overwrite").saveAsTable(...)`, which
clobbered other workspaces' rows when two deployments shared a UC catalog.
"""

from __future__ import annotations


def build_merge_sql(target: str, source_view: str, merge_keys: list[str]) -> str:
    """Return the MERGE statement for `target` reading from `source_view`.

    Join always includes ``workspace_id``; callers MUST ensure the DataFrame
    they registered as `source_view` has a `workspace_id` column.
    """
    if not merge_keys:
        raise ValueError("merge_keys must be non-empty")
    join_clauses = " AND ".join(f"tgt.{k} = src.{k}" for k in merge_keys)
    join_clauses += " AND tgt.workspace_id = src.workspace_id"
    return (
        f"MERGE INTO {target} tgt\n"
        f"USING {source_view} src\n"
        f"ON {join_clauses}\n"
        "WHEN MATCHED THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )


def merge_workspace_scoped(
    spark,
    df,
    table_fqn: str,
    merge_keys: list[str],
) -> None:
    """Idempotently MERGE `df` into `table_fqn`.

    Creates the target table from `df`'s schema if it does not yet exist
    (first deploy bootstrap), then MERGEs by ``merge_keys + ["workspace_id"]``.
    """
    if "workspace_id" not in df.columns:
        raise ValueError(
            "df must contain workspace_id column "
            "(use includes.workspace_id.resolve())"
        )
    view = f"_src_{table_fqn.replace('.', '_')}"
    df.createOrReplaceTempView(view)
    columns = ", ".join(f"{c} {df.schema[c].dataType.simpleString()}" for c in df.columns)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_fqn} ({columns}) USING DELTA"
    )
    spark.sql(build_merge_sql(table_fqn, view, merge_keys))

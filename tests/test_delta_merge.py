"""Test: merge_workspace_scoped builds the MERGE statement we expect."""

from unittest.mock import MagicMock

from src.includes.delta_merge import build_merge_sql, merge_workspace_scoped


def test_build_merge_sql_includes_workspace_id_in_join() -> None:
    sql = build_merge_sql(
        target="cat.sch.adb_genie_spaces",
        source_view="src",
        merge_keys=["space_id"],
    )
    assert "MERGE INTO cat.sch.adb_genie_spaces" in sql
    assert "tgt.space_id = src.space_id" in sql
    assert "tgt.workspace_id = src.workspace_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_merge_workspace_scoped_creates_then_merges() -> None:
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["space_id", "workspace_id", "workspace_name", "title"]

    merge_workspace_scoped(
        spark=spark,
        df=df,
        table_fqn="cat.sch.adb_genie_spaces",
        merge_keys=["space_id"],
    )
    # Created if not exists (idempotent first run) and then MERGE
    create_calls = [c for c in spark.sql.call_args_list if "CREATE TABLE IF NOT EXISTS" in c.args[0]]
    merge_calls = [c for c in spark.sql.call_args_list if "MERGE INTO" in c.args[0]]
    assert len(create_calls) == 1
    assert len(merge_calls) == 1


def test_merge_workspace_scoped_coerces_void_columns_to_string() -> None:
    """All-NULL (Spark `void`) columns must be cast to string before CREATE/MERGE.

    Delta MERGE rejects `void` columns with DELTA_MERGE_ADD_VOID_COLUMN
    (SQLSTATE 42K09). The helper must coerce them to `string` first.
    """
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["app_id", "workspace_id", "app_status"]

    # Mock df.schema[c].dataType.simpleString() per-column:
    #   app_status -> "void", others -> "string"
    schema = {}
    for c in df.columns:
        field = MagicMock()
        field.dataType.simpleString.return_value = "void" if c == "app_status" else "string"
        schema[c] = field
    df.schema.__getitem__.side_effect = lambda c: schema[c]

    df.withColumn.return_value = df  # chained calls all return the same mock

    merge_workspace_scoped(
        spark=spark,
        df=df,
        table_fqn="cat.sch.adb_apps",
        merge_keys=["app_id"],
    )
    # We must have called withColumn("app_status", ...) exactly once to cast it.
    cast_calls = [c for c in df.withColumn.call_args_list if c.args and c.args[0] == "app_status"]
    assert len(cast_calls) == 1, "void column must be coerced to string"

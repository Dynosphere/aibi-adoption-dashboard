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

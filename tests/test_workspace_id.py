"""Test: resolve() returns workspace_id from system.access.workspaces_latest."""

from unittest.mock import MagicMock

from src.includes.workspace_id import resolve


def test_returns_row_from_workspaces_latest() -> None:
    spark = MagicMock()
    row = MagicMock()
    row.workspace_id = 1444828305810485
    row.workspace_name = "e2-demo-field-eng"
    row.workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"
    spark.sql.return_value.first.return_value = row

    w = MagicMock()
    w.config.host = "https://e2-demo-field-eng.cloud.databricks.com"

    wid, name, url = resolve(spark, w)
    assert wid == 1444828305810485
    assert name == "e2-demo-field-eng"
    assert url == "https://e2-demo-field-eng.cloud.databricks.com"


def test_falls_back_to_host_when_system_table_unreadable() -> None:
    spark = MagicMock()
    spark.sql.side_effect = Exception("Table not found")

    w = MagicMock()
    w.config.host = "https://fallback.cloud.databricks.com"

    wid, name, url = resolve(spark, w)
    assert url == "https://fallback.cloud.databricks.com"
    # workspace_id and name are synthesised when fallback fires
    assert wid == 0
    assert "fallback" in name.lower()

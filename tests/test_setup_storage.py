"""Test: ensure_storage creates the catalog/schema or raises with remediation."""

from unittest.mock import MagicMock

import pytest

from src.includes.setup_storage import ensure_storage


def test_creates_catalog_and_schema_idempotently() -> None:
    spark = MagicMock()
    ensure_storage(spark, "my_cat", "my_schema")
    statements = [c.args[0] for c in spark.sql.call_args_list]
    assert any("CREATE CATALOG IF NOT EXISTS `my_cat`" in s for s in statements)
    assert any(
        "CREATE SCHEMA IF NOT EXISTS `my_cat`.`my_schema`" in s for s in statements
    )


def test_raises_permissionerror_with_remediation_text() -> None:
    spark = MagicMock()
    spark.sql.side_effect = Exception("PERMISSION_DENIED: missing CREATE CATALOG")
    with pytest.raises(PermissionError) as exc:
        ensure_storage(spark, "no_perm_cat", "any_schema")
    msg = str(exc.value)
    assert "no_perm_cat" in msg
    assert "CREATE CATALOG" in msg  # remediation guidance

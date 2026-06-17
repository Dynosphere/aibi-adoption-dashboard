"""Idempotent catalog/schema bootstrap for the V3 pipeline.

Resolves GitHub issue #11: pipeline failed silently when the configured
catalog or schema did not exist. We now `CREATE ... IF NOT EXISTS` and
fail loudly with a remediation hint if the executing identity lacks
permission.
"""

from __future__ import annotations


def _quote_ident(name: str) -> str:
    """Quote an identifier with backticks (Spark SQL safe)."""
    if "`" in name:
        # Spark identifiers don't allow backticks; surface a clean error.
        raise ValueError(f"Identifier may not contain backticks: {name!r}")
    return f"`{name}`"


def ensure_storage(spark, catalog: str, schema: str) -> None:
    """Create `<catalog>.<schema>` if it does not exist.

    Raises:
        PermissionError: if the executing identity lacks CREATE CATALOG /
            CREATE SCHEMA. Message includes the missing identifier and
            the grants the user should ask their UC admin for.
    """
    c = _quote_ident(catalog)
    s = _quote_ident(schema)
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {c}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{s}")
    except Exception as exc:  # noqa: BLE001 - surface clean error
        raise PermissionError(
            (
                f"Could not create catalog/schema {catalog}.{schema}. "
                "The deploy identity needs CREATE CATALOG (account-admin grant) "
                "and CREATE SCHEMA on the target catalog. "
                f"Original error: {exc}"
            )
        ) from exc

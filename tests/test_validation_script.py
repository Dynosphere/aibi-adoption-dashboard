"""Validation script structural test (does not run against a workspace)."""

from pathlib import Path


def test_script_exists(repo_root: Path) -> None:
    script = repo_root / "scripts" / "validate_system_tables.py"
    assert script.is_file(), "validate_system_tables.py must exist"


def test_script_references_all_required_tables(repo_root: Path) -> None:
    """The script must touch every system table the V3 spec depends on."""
    required_tables = [
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
    body = (repo_root / "scripts" / "validate_system_tables.py").read_text()
    missing = [t for t in required_tables if t not in body]
    assert not missing, f"validation script missing: {missing}"

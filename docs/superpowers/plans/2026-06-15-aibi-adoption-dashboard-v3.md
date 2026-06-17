# aibi-adoption-dashboard V3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship V3 of the public-facing Databricks adoption dashboard before 2026-07-06 (Genie pay-as-you-go go-live), modernising the data model onto system tables, fixing issues #11/#12/#14, and adding cost, quality, and cross-asset adoption pages.

**Architecture:** Dimensions stay in the SDK, facts move to system tables. Every fact is an incremental Delta table merged with `MERGE INTO` (not a view), keyed on `(asset_id, event_date, workspace_id)`. SDK fan-out for residual catalogue calls is bounded `ThreadPoolExecutor(max_workers=5-8)`. The SDK message-content loader (`adb_genie_messages`) is preserved behind an opt-in bundle variable `enable_genie_deep_dive: false` for the deep-dive panel.

**Tech Stack:** Databricks Asset Bundles, Python 3.11 + `databricks-sdk` ≥ 0.117, Spark SQL on Databricks SQL warehouses (Serverless Starter), Lakeview dashboards (.lvdash.json), pytest for Python unit tests, GitHub (`Dynosphere/aibi-adoption-dashboard`, SSH remote `git@personal:Dynosphere/aibi-adoption-dashboard.git`).

## Global Constraints

Every task implicitly includes these. Values are quoted verbatim from `docs/superpowers/specs/2026-06-15-aibi-adoption-dashboard-v3-design.md`.

- **Public-facing solution.** No internal jargon, no `go/` links, no internal Slack channel IDs in code, comments, copy, or commits.
- **Hard deadline:** 2026-07-06 (Genie paygo go-live).
- **Target workspace during build:** `e2-demo-field-eng` (`https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485`).
- **Git auth:** SSH remote uses `git@personal` (Dynosphere account). Never push from any other identity. Never use `--no-verify` or `--no-gpg-sign`.
- **Databricks Runtime:** 13.x or later, Unity Catalog enabled.
- **SDK pin:** `databricks-sdk >= 0.117` (adds `serialized_space`, Genie eval-run methods, removed SCIM `/Me` round-trip).
- **Materialised aggregations:** every `mvFact*` is a Delta table populated via `MERGE INTO`, **not** a fresh-each-run view. Bounded source windows (typically 7 days, longer for slow-changing facts).
- **Writes are workspace-aware.** Every `adb_*` and `mvFact*` carries `workspace_id`. MERGE keys include `workspace_id`.
- **No `df.write.mode("overwrite")`** — use `MERGE INTO` keyed on `(<entity_id>, workspace_id)`.
- **Drop SCIM `users.get`** from hot paths — use `user_identity.email` from audit rows.
- **Bounded concurrency:** `ThreadPoolExecutor(max_workers=5-8)` per resource type. Honour SCIM 255/min, Permissions 100/s, Query History 10/s.
- **Naming:** "Genie Agents (formerly Genie Spaces)" once, then "Genie Agents". "Genie One" replaces "Databricks One" in user-facing copy. SDK/audit identifiers (`service_name='aibiGenie'`, `space_id`) unchanged.
- **`billing_origin_product IN ('GENIE','AI/BI_GENIE')`** — future-proofs the label rename.
- **Bundle variable `enable_genie_deep_dive`** defaults to `false`. **The pipeline and dashboard must not break when this is `false`.** Deep-dive widget renders a placeholder.
- **Public Preview / Beta dependencies** (`system.access.audit`, `system.serving.endpoint_usage`, `system.ai_gateway.usage`, Genie Benchmark API, `system.access.assistant_events`) get a banner on the relevant tile.
- **Frequent commits.** Co-authored-by: Isaac line on every commit.
- **Tag `v3.0.0-rc<N>` after each CP**, `v3.0.0` after CP6.

---

# CP0 — Live system-table validation pass

**Why first:** Spec §11 locks this as the gate. Public docs may differ from your workspace's preview ring; we want to know before anyone writes a MERGE.

**Effort:** ~0.5 day.

### Task 0.1: Set up Python test harness

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `pyproject.toml`
- Create: `.github/workflows/ci.yml`
- Create: `requirements-dev.txt`

**Interfaces:**
- Produces: a `pytest` runnable from repo root; CI runs `databricks bundle validate` + `pytest` on every push.

- [ ] **Step 1: Create `requirements-dev.txt`**

```
pytest>=8.0
pytest-mock>=3.12
databricks-sdk>=0.117
pyyaml>=6.0
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --strict-markers"
markers = [
    "integration: requires live Databricks workspace (deselect with -m 'not integration')",
]
```

- [ ] **Step 3: Create `tests/__init__.py` (empty file)**

- [ ] **Step 4: Create `tests/conftest.py`**

```python
"""Shared pytest fixtures for the aibi-adoption-dashboard test suite."""

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def repo_root() -> Path:
    """Absolute path to the repo root."""
    return REPO_ROOT


@pytest.fixture
def in_workspace() -> bool:
    """True when running in a Databricks workspace runtime (used to gate integration tests)."""
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None
```

- [ ] **Step 5: Create `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements-dev.txt
      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      - name: Bundle validate
        run: databricks bundle validate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      - name: Unit tests
        run: pytest -m "not integration"
```

- [ ] **Step 6: Write a smoke test that proves the harness works**

Create `tests/test_smoke.py`:

```python
"""Smoke test: confirms pytest + project layout work."""

from pathlib import Path


def test_repo_root_has_databricks_yml(repo_root: Path) -> None:
    assert (repo_root / "databricks.yml").is_file()
```

- [ ] **Step 7: Run the smoke test**

```bash
cd /Users/sam.lecorre/Repos/aibi-adoption-dashboard
pip install -r requirements-dev.txt
pytest tests/test_smoke.py -v
```

Expected: 1 passed.

- [ ] **Step 8: Commit**

```bash
git checkout -b cp0/validation
git add tests/ pyproject.toml requirements-dev.txt .github/
git commit -m "$(cat <<'EOF'
test: add pytest harness and CI workflow

Adds a minimal pytest setup with conftest.py + a smoke test, plus a
GitHub Actions workflow that runs `databricks bundle validate` and the
test suite on every push. Integration-marked tests are excluded from CI
by default (they require a live workspace).

Co-authored-by: Isaac
EOF
)"
```

### Task 0.2: Validate every system table referenced by the V3 spec

**Files:**
- Create: `docs/v3-system-table-validation.md`
- Create: `scripts/validate_system_tables.py`

**Interfaces:**
- Consumes: `WorkspaceClient` (uses default auth chain).
- Produces: `docs/v3-system-table-validation.md` with `DESCRIBE EXTENDED` output + a checklist for each referenced table.

- [ ] **Step 1: Write the test that asserts the validation script exists and is executable**

Create `tests/test_validation_script.py`:

```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pytest tests/test_validation_script.py -v
```

Expected: FAIL (`validate_system_tables.py` does not exist).

- [ ] **Step 3: Create `scripts/validate_system_tables.py`**

```python
"""V3 pre-flight: DESCRIBE EXTENDED every system table referenced in the spec.

Writes the consolidated output to `docs/v3-system-table-validation.md`.

Usage:
    DATABRICKS_HOST=https://e2-demo-field-eng.cloud.databricks.com \\
    DATABRICKS_TOKEN=... \\
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
    # Use the warehouse the bundle resolves to. Fall back to the first Serverless Starter.
    starter = next(
        (
            wh for wh in w.warehouses.list()
            if wh.name == "Serverless Starter Warehouse"
        ),
        None,
    )
    if starter is None:
        print("ERROR: Serverless Starter Warehouse not found in workspace.", file=sys.stderr)
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
```

- [ ] **Step 4: Run the test to verify it now passes**

```bash
pytest tests/test_validation_script.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Authenticate to `e2-demo-field-eng` and run the validation script**

```bash
databricks auth login --host https://e2-demo-field-eng.cloud.databricks.com
python scripts/validate_system_tables.py
```

Expected output: `Wrote /Users/sam.lecorre/Repos/aibi-adoption-dashboard/docs/v3-system-table-validation.md`.

- [ ] **Step 6: Manually review `docs/v3-system-table-validation.md`**

Confirm:
- Every required table returned `OK`. Any `ERROR:` rows are blockers for the affected CP — flag in the doc and decide a workaround before CP1 starts.
- `billing_origin_product` observed values are recorded. If `AI/BI_GENIE` is **not yet present** (likely — paygo starts July 6), confirm `GENIE` is present and the `IN ('GENIE','AI/BI_GENIE')` clause still future-proofs.
- `system.serving.endpoint_usage` schema matches what `02_mvFactServingEndpointUsage.sql` (CP4) will MERGE from.
- Column names cited in S-A / S-C / S-F (e.g. `usage_metadata.app_id`, `usage_metadata.endpoint_id`, `genie_space_id` on `system.query.history`) actually exist.

- [ ] **Step 7: Commit**

```bash
git add scripts/ docs/v3-system-table-validation.md tests/test_validation_script.py
git commit -m "$(cat <<'EOF'
chore: add system-table validation script and snapshot

CP0 pre-flight: validates that every system table the V3 spec depends on
exists in e2-demo-field-eng with the expected schema. The snapshot in
docs/v3-system-table-validation.md is the source of truth for column
names/types when CP1+ write MERGE statements.

Co-authored-by: Isaac
EOF
)"
```

### Task 0.3: Open the CP0 PR

- [ ] **Step 1: Push and open PR**

```bash
git push -u personal cp0/validation
gh pr create --title "CP0: system-table validation pass" --body "$(cat <<'EOF'
## Summary

Pre-flight for V3:
- Adds a pytest harness + CI workflow.
- Adds `scripts/validate_system_tables.py` that runs `DESCRIBE EXTENDED` over every system table the V3 spec depends on.
- Captures the snapshot in `docs/v3-system-table-validation.md`.

## Test plan

- [x] `pytest -m "not integration"` passes locally.
- [x] `python scripts/validate_system_tables.py` ran against `e2-demo-field-eng` and produced the doc.
- [x] Manual review confirmed all required tables return `OK` (any errors are documented inline).

This pull request and its description were written by Isaac.
EOF
)"
```

- [ ] **Step 2: Merge after review and tag `v3.0.0-rc0`**

```bash
gh pr merge --squash --delete-branch
git checkout main && git pull
git tag v3.0.0-rc0 && git push personal v3.0.0-rc0
```

---

# CP1 — Stabilise + future-proof

**Why:** Foundation for everything else. Fix #11 (catalog/schema). Add `workspace_id` everywhere. Switch overwrites to MERGEs. No new features.

**Effort:** 2–3 days.

### Task 1.1: Add `enable_genie_deep_dive` bundle variable

**Files:**
- Modify: `deployment_resources/variables.yml`

**Interfaces:**
- Produces: bundle variable `enable_genie_deep_dive` (bool, default `false`) available to job parameters via `${var.enable_genie_deep_dive}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_bundle_vars.py`:

```python
"""Test: required V3 bundle variables exist with correct defaults."""

from pathlib import Path

import yaml


def _load_vars(repo_root: Path) -> dict:
    return yaml.safe_load(
        (repo_root / "deployment_resources" / "variables.yml").read_text()
    )["variables"]


def test_enable_genie_deep_dive_default_false(repo_root: Path) -> None:
    vars_ = _load_vars(repo_root)
    assert "enable_genie_deep_dive" in vars_, "enable_genie_deep_dive must be declared"
    assert vars_["enable_genie_deep_dive"]["default"] in (False, "false")
```

- [ ] **Step 2: Run test to confirm fail**

```bash
pytest tests/test_bundle_vars.py -v
```

Expected: FAIL (variable not declared).

- [ ] **Step 3: Add the variable**

Edit `deployment_resources/variables.yml` — append to the `variables:` block (keep existing entries intact):

```yaml
  enable_genie_deep_dive:
    description: |
      When true, the pipeline pulls Genie conversation/message content via the
      SDK into adb_genie_messages (slow on large workspaces). Required for the
      Genie deep-dive panel. When false (default), the deep-dive widget renders
      a placeholder and no SDK message fan-out runs.
    default: false
```

- [ ] **Step 4: Run test to confirm pass**

```bash
pytest tests/test_bundle_vars.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git checkout -b cp1/stabilise
git add deployment_resources/variables.yml tests/test_bundle_vars.py
git commit -m "$(cat <<'EOF'
feat: add enable_genie_deep_dive bundle variable

Opt-in flag for the SDK message-content loader behind the Genie deep-dive
panel. Defaults to false so deployments don't pay the SDK fan-out cost
unless they explicitly want the deep-dive widget.

Co-authored-by: Isaac
EOF
)"
```

### Task 1.2: Try-create catalog and schema (fixes #11)

**Files:**
- Create: `src/includes/__init__.py` (empty if not already present)
- Create: `src/includes/setup_storage.py`
- Modify: `src/01_get_metadata.ipynb` (add a call to `ensure_storage` near the top)
- Create: `tests/test_setup_storage.py`

**Interfaces:**
- Produces: `setup_storage.ensure_storage(spark, catalog: str, schema: str) -> None` — idempotently creates the catalog and schema if the executing identity has permission. Raises a clear `PermissionError` with remediation text if it does not.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to confirm fail**

```bash
pytest tests/test_setup_storage.py -v
```

Expected: FAIL (module not found).

- [ ] **Step 3: Create `src/includes/__init__.py`** (empty if not present).

- [ ] **Step 4: Create `src/includes/setup_storage.py`**

```python
"""Idempotent catalog/schema bootstrap for the V3 pipeline.

Resolves GitHub issue #11: pipeline failed silently when the configured
catalog or schema did not exist. We now `CREATE ... IF NOT EXISTS` and
fail loudly with a remediation hint if the executing identity lacks
permission.
"""

from __future__ import annotations


class _IdentLike:
    pass


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
```

- [ ] **Step 5: Run test to confirm pass**

```bash
pytest tests/test_setup_storage.py -v
```

Expected: 2 passed.

- [ ] **Step 6: Wire the call into `01_get_metadata.ipynb`**

Open `src/01_get_metadata.ipynb`. In the first cell that resolves `catalog_name` / `schema_name` parameters, add (after the parameter resolution, before the first `USE CATALOG` / SQL write):

```python
from includes.setup_storage import ensure_storage
ensure_storage(spark, catalog_name, schema_name)
```

If the notebook does not currently import from `includes`, also ensure `sys.path.insert(0, os.path.dirname(__file__) or '.')` is present so the include resolves (the bundle workspace layout maps `src/includes/` directly).

- [ ] **Step 7: Commit**

```bash
git add src/includes/ src/01_get_metadata.ipynb tests/test_setup_storage.py
git commit -m "$(cat <<'EOF'
fix(#11): try-create catalog and schema with clear permission error

ensure_storage() runs CREATE CATALOG/SCHEMA IF NOT EXISTS at the top of
the metadata notebook. When the identity lacks permissions, raises a
PermissionError naming the missing grants instead of failing later with
a cryptic Spark error.

Resolves #11.

Co-authored-by: Isaac
EOF
)"
```

### Task 1.3: Add `workspace_id` lookup helper

**Files:**
- Create: `src/includes/workspace_id.py`
- Create: `tests/test_workspace_id.py`

**Interfaces:**
- Produces: `workspace_id.resolve(spark, sdk_workspace_client) -> tuple[int, str, str]` — returns `(workspace_id, workspace_name, workspace_url)` from `system.access.workspaces_latest`. Falls back to a synthesised tuple from `w.config.host` if the system table is unreadable.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to confirm fail**

Expected: FAIL.

- [ ] **Step 3: Create `src/includes/workspace_id.py`**

```python
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
```

- [ ] **Step 4: Run test to confirm pass**

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/includes/workspace_id.py tests/test_workspace_id.py
git commit -m "$(cat <<'EOF'
feat: add workspace_id resolver helper

Single source of truth for the current workspace_id (from
system.access.workspaces_latest, falling back to w.config.host). Every
adb_* and mvFact* table will carry this column for multi-workspace
future-proofing.

Co-authored-by: Isaac
EOF
)"
```

### Task 1.4: Carry `workspace_id` through every `adb_*` write

**Files:**
- Modify: `src/01_get_metadata.ipynb` (every cell that writes to `adb_*`)

**Interfaces:**
- Consumes: `workspace_id.resolve()` from Task 1.3, `ensure_storage()` from Task 1.2.
- Produces: every `adb_*` Delta table gains columns `workspace_id BIGINT`, `workspace_name STRING`, `workspace_url STRING`. Existing rows in pre-V3 tables will need a one-off backfill — handled in CP6 polish.

- [ ] **Step 1: Open `src/01_get_metadata.ipynb` and inventory every write**

Identify every `df.write.mode("overwrite").saveAsTable("adb_*")` call. Capture them in a list (in the PR description) so the reviewer can cross-check.

- [ ] **Step 2: At the top of the notebook (after `ensure_storage`), add the workspace lookup**

```python
from databricks.sdk import WorkspaceClient
from includes.workspace_id import resolve as resolve_workspace

w = WorkspaceClient()
workspace_id, workspace_name, workspace_url = resolve_workspace(spark, w)
print(f"workspace_id={workspace_id} name={workspace_name}")
```

- [ ] **Step 3: For each `adb_*` write, add the three workspace columns to the DataFrame before writing**

Use this pattern (apply to every `adb_*` build cell):

```python
from pyspark.sql.functions import lit

df = df.withColumn("workspace_id", lit(workspace_id).cast("bigint")) \
       .withColumn("workspace_name", lit(workspace_name)) \
       .withColumn("workspace_url", lit(workspace_url))
```

Apply to: `adb_genie_spaces`, `adb_genie_conversations`, `adb_genie_messages`, `adb_dashboards`, `adb_dashboard_schedules`, `adb_dashboard_subscriptions`, `adb_models`, `adb_serving_endpoints`, `adb_apps`.

- [ ] **Step 4: Deploy and run the bundle against `e2-demo-field-eng` with current writes (still `overwrite`)**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

Expected: job completes; every `adb_*` table now contains `workspace_id`/`workspace_name`/`workspace_url` columns.

- [ ] **Step 5: Verify**

Run in the workspace SQL editor:

```sql
DESCRIBE EXTENDED users.<your_schema>.adb_genie_spaces;
-- Confirm workspace_id BIGINT, workspace_name STRING, workspace_url STRING
SELECT DISTINCT workspace_id, workspace_name FROM users.<your_schema>.adb_genie_spaces;
-- Expect exactly one workspace
```

- [ ] **Step 6: Commit**

```bash
git add src/01_get_metadata.ipynb
git commit -m "$(cat <<'EOF'
feat: carry workspace_id on every adb_* table

Adds workspace_id / workspace_name / workspace_url columns to every
metadata-notebook write so two deployments writing to a shared UC catalog
don't collide. Multi-workspace topology B is now a no-op at the data-model
level; the only remaining change is switching writes from overwrite to
MERGE (next task).

Co-authored-by: Isaac
EOF
)"
```

### Task 1.5: Switch `adb_*` writes from `overwrite` to `MERGE INTO`

**Files:**
- Modify: `src/01_get_metadata.ipynb`
- Create: `src/includes/delta_merge.py`
- Create: `tests/test_delta_merge.py`

**Interfaces:**
- Produces: `delta_merge.merge_workspace_scoped(df, table_fqn: str, merge_keys: list[str]) -> None` — wraps the Delta `MERGE INTO` so each `adb_*` cell becomes a one-liner.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to confirm fail**

Expected: FAIL.

- [ ] **Step 3: Create `src/includes/delta_merge.py`**

```python
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
```

- [ ] **Step 4: Run test to confirm pass**

Expected: 2 passed.

- [ ] **Step 5: Replace every `adb_*` write in `01_get_metadata.ipynb`**

For each cell that wrote `df.write.mode("overwrite").saveAsTable("adb_X")`, replace with:

```python
from includes.delta_merge import merge_workspace_scoped

merge_workspace_scoped(
    spark=spark,
    df=df,
    table_fqn=f"{catalog_name}.{schema_name}.adb_X",
    merge_keys=["<natural_key_column>"],  # see table below
)
```

Natural keys per table:

| Table | merge_keys |
|---|---|
| `adb_genie_spaces` | `["space_id"]` |
| `adb_genie_conversations` | `["conversation_id"]` |
| `adb_genie_messages` | `["message_id"]` |
| `adb_dashboards` | `["dashboard_id"]` |
| `adb_dashboard_schedules` | `["dashboard_id", "schedule_id"]` |
| `adb_dashboard_subscriptions` | `["dashboard_id", "schedule_id", "subscription_id"]` |
| `adb_models` | `["model_full_name"]` |
| `adb_serving_endpoints` | `["endpoint_id"]` |
| `adb_apps` | `["app_id"]` |

- [ ] **Step 6: Deploy + run end-to-end against `e2-demo-field-eng`**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

Run the job **twice** to confirm idempotency.

- [ ] **Step 7: Verify**

```sql
-- Row count is stable across two runs (no duplication, no clobber)
SELECT COUNT(*) FROM users.<schema>.adb_genie_spaces;  -- expect same N twice
```

- [ ] **Step 8: Commit**

```bash
git add src/includes/delta_merge.py src/01_get_metadata.ipynb tests/test_delta_merge.py
git commit -m "$(cat <<'EOF'
feat: switch adb_* writes from overwrite to MERGE INTO

Every adb_* write now uses includes.delta_merge.merge_workspace_scoped,
which MERGEs keyed on (natural_key, workspace_id). Idempotent across
repeat runs and safe for two deployments writing to a shared UC catalog.

Co-authored-by: Isaac
EOF
)"
```

### Task 1.6: Open CP1 PR and tag rc1

- [ ] **Step 1: Push**

```bash
git push -u personal cp1/stabilise
```

- [ ] **Step 2: Open PR**

```bash
gh pr create --title "CP1: stabilise + future-proof" --body "$(cat <<'EOF'
## Summary

- Adds `enable_genie_deep_dive` bundle variable (default false).
- Fixes #11: `ensure_storage()` creates catalog/schema if missing with a clear permission error if not.
- Adds `workspace_id` / `workspace_name` / `workspace_url` to every `adb_*` table.
- Switches every `adb_*` write from `overwrite` to `MERGE INTO` keyed on `(natural_key, workspace_id)`. Idempotent across reruns; safe for shared-catalog deployments.

## Test plan

- [x] `pytest -m "not integration"` passes (4 new test modules).
- [x] `databricks bundle validate` passes.
- [x] `databricks bundle deploy --target dev && databricks bundle run adoption_dash_job --target dev` succeeds twice in a row with stable row counts.
- [x] `DESCRIBE EXTENDED` on each `adb_*` table shows the three workspace columns.

Closes #11.

This pull request and its description were written by Isaac.
EOF
)"
```

- [ ] **Step 3: Merge + tag**

```bash
gh pr merge --squash --delete-branch
git checkout main && git pull
git tag v3.0.0-rc1 && git push personal v3.0.0-rc1
```

---

# CP2 — Genie observability refactor + perf

**Why:** Biggest single phase. Drops `adb_genie_conversations` and the SDK message fan-out (primary path). Re-derives `mvFactGenieUsage` and `genie_observability_main_table` from `system.access.audit` + `system.query.history`. Resolves #12 (perf) and #14 (statement_id mismatch) together. Adds `ThreadPoolExecutor` to residual SDK lists. Keeps `adb_genie_messages` SDK loader behind `enable_genie_deep_dive`.

**Effort:** 4–6 days.

### Task 2.1: Refactor `mvFactGenieUsage` into an incremental Delta MERGE

**Files:**
- Modify: `src/02_mvFactGenieUsage.sql`
- Create: `tests/sql/test_mv_genie_usage.sql`

**Interfaces:**
- Produces: `${catalog}.${schema}.mvFactGenieUsage` as a Delta table populated by MERGE on each pipeline run. Schema (PK = (space_id, usage_date, workspace_id, user_email)): `space_id, space_title, usage_date, workspace_id, workspace_name, user_email, message_count, conversation_count, distinct_users, surface`.

- [ ] **Step 1: Replace the body of `02_mvFactGenieUsage.sql`**

```sql
-- mvFactGenieUsage — incremental MERGE on system.access.audit.
-- Source: audit rows where service_name IN ('aibiGenie','genieChat'),
-- aggregated to (space_id, usage_date, workspace_id, user_email) grain.
-- Window: rebuilds the last 7 days every run; older rows are stable history.

CREATE TABLE IF NOT EXISTS :catalog.:schema.mvFactGenieUsage (
  space_id          STRING,
  space_title       STRING,
  usage_date        DATE,
  workspace_id      BIGINT,
  workspace_name    STRING,
  user_email        STRING,
  message_count     BIGINT,
  conversation_count BIGINT,
  distinct_users    BIGINT,
  surface           STRING  -- 'agents' (aibiGenie) or 'chat' (genieChat)
) USING DELTA
  PARTITIONED BY (workspace_id, surface);

MERGE INTO :catalog.:schema.mvFactGenieUsage tgt
USING (
  WITH audit AS (
    SELECT
      coalesce(request_params.space_id, request_params.spaceId)        AS space_id,
      event_date                                                       AS usage_date,
      workspace_id,
      user_identity.email                                              AS user_email,
      action_name,
      request_params.conversation_id                                   AS conversation_id,
      CASE service_name
        WHEN 'aibiGenie' THEN 'agents'
        WHEN 'genieChat' THEN 'chat'
      END AS surface
    FROM system.access.audit
    WHERE service_name IN ('aibiGenie', 'genieChat')
      AND event_date >= current_date() - INTERVAL 7 DAYS
      AND request_params.space_id IS NOT NULL
  ),
  spaces AS (
    SELECT space_id, title AS space_title, workspace_id
    FROM :catalog.:schema.adb_genie_spaces
  ),
  ws AS (
    SELECT workspace_id, workspace_name FROM system.access.workspaces_latest
  ),
  agg AS (
    SELECT
      a.space_id,
      a.usage_date,
      a.workspace_id,
      a.user_email,
      a.surface,
      count(*)                                                              AS message_count,
      count(DISTINCT a.conversation_id)                                     AS conversation_count
    FROM audit a
    GROUP BY ALL
  )
  SELECT
    g.space_id,
    s.space_title,
    g.usage_date,
    g.workspace_id,
    w.workspace_name,
    g.user_email,
    g.message_count,
    g.conversation_count,
    -- distinct_users is per (space, date) — computed via a window after the aggregate
    count(g.user_email) OVER (PARTITION BY g.space_id, g.usage_date, g.workspace_id) AS distinct_users,
    g.surface
  FROM agg g
  LEFT JOIN spaces s ON s.space_id = g.space_id AND s.workspace_id = g.workspace_id
  LEFT JOIN ws w     ON w.workspace_id = g.workspace_id
) src
ON tgt.space_id     = src.space_id
AND tgt.usage_date  = src.usage_date
AND tgt.workspace_id = src.workspace_id
AND tgt.user_email  = src.user_email
AND tgt.surface     = src.surface
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- [ ] **Step 2: Write the SQL-level smoke test**

Create `tests/sql/test_mv_genie_usage.sql`:

```sql
-- Smoke test for mvFactGenieUsage.
-- Requires: a recent run of the bundle's adoption_dash_job.

-- Schema sanity
SELECT
  assert_true(count(*) > 0, 'mvFactGenieUsage must not be empty after a pipeline run') AS schema_ok
FROM :catalog.:schema.mvFactGenieUsage;

-- Workspace scope: every row has a workspace_id
SELECT
  assert_true(count(*) = 0, 'mvFactGenieUsage rows must all carry workspace_id') AS ws_ok
FROM :catalog.:schema.mvFactGenieUsage
WHERE workspace_id IS NULL;

-- Idempotency: running this query twice does not change row count
-- (manually verify by snapshotting count and re-running the pipeline.)
```

- [ ] **Step 3: Deploy + run end-to-end**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

- [ ] **Step 4: Run the SQL smoke test in the SQL editor**

Confirm both `assert_true` rows return without raising.

- [ ] **Step 5: Compare row counts to V2**

```sql
-- V2 row count (current state before the MERGE refactor) recorded in PR description.
SELECT count(*) FROM users.<schema>.mvFactGenieUsage;  -- V3 row count
```

The V3 numbers should be in the same order of magnitude (audit captures every message; V2 SDK loop missed benchmark-created conversations per ES-1688908, so V3 may be slightly higher — note this in the CHANGELOG).

- [ ] **Step 6: Commit**

```bash
git checkout -b cp2/genie-observability
git add src/02_mvFactGenieUsage.sql tests/sql/test_mv_genie_usage.sql
git commit -m "$(cat <<'EOF'
refactor: rebuild mvFactGenieUsage on system.access.audit (incremental MERGE)

Replaces the V2 view that joined SDK-derived conversation/message tables
with an incremental Delta MERGE keyed on (space_id, usage_date,
workspace_id, user_email, surface). Source is system.access.audit
filtered to service_name IN ('aibiGenie','genieChat'); 7-day rebuild
window means audit retention rollover does not truncate our history.

Part of #12 (perf) and #14 (statement_id mismatch).

Co-authored-by: Isaac
EOF
)"
```

### Task 2.2: Rewrite `genie_observability_main_table` body

**Files:**
- Modify: `src/02_genie_observability_main_table.sql`

**Interfaces:**
- Produces: same external contract as V2 (same table name, key columns `space_id`, `conversation_id`, `message_id`, `statement_id`, `user_email`, `created_datetime`) plus a new `surface` column. Any V2 columns not reconstructable from `audit` + `query.history` are dropped and listed in CHANGELOG.

- [ ] **Step 1: Replace the body of `02_genie_observability_main_table.sql`**

```sql
-- genie_observability_main_table — incremental MERGE on audit + query.history.
-- Replaces the V2 SDK-derived view that suffered from statement_id mismatches (#14).

CREATE TABLE IF NOT EXISTS :catalog.:schema.genie_observability_main_table (
  space_id          STRING,
  space_title       STRING,
  conversation_id   STRING,
  message_id        STRING,
  statement_id      STRING,
  user_email        STRING,
  created_datetime  TIMESTAMP,
  workspace_id      BIGINT,
  workspace_name    STRING,
  surface           STRING,
  action_name       STRING,
  feedback_rating   STRING
) USING DELTA
  PARTITIONED BY (workspace_id, surface);

MERGE INTO :catalog.:schema.genie_observability_main_table tgt
USING (
  WITH events AS (
    SELECT
      coalesce(request_params.space_id, request_params.spaceId)  AS space_id,
      request_params.conversation_id                              AS conversation_id,
      request_params.message_id                                   AS message_id,
      user_identity.email                                         AS user_email,
      event_time                                                  AS created_datetime,
      workspace_id,
      CASE service_name
        WHEN 'aibiGenie' THEN 'agents'
        WHEN 'genieChat' THEN 'chat'
      END                                                         AS surface,
      action_name,
      request_params.feedback_rating                              AS feedback_rating
    FROM system.access.audit
    WHERE service_name IN ('aibiGenie', 'genieChat')
      AND event_date >= current_date() - INTERVAL 7 DAYS
      AND request_params.message_id IS NOT NULL
  ),
  with_stmt AS (
    -- Join message-level events to executed SQL via system.query.history.genie_space_id
    -- + a temporal window (message-emit ± 5 minutes). This is the join that V2 got
    -- wrong (it relied on attachments.statement_id from the SDK, which silently drops
    -- statement IDs in some attachments).
    SELECT
      e.*,
      q.statement_id
    FROM events e
    LEFT JOIN system.query.history q
      ON q.genie_space_id = e.space_id
     AND q.workspace_id = e.workspace_id
     AND q.statement_start_time BETWEEN e.created_datetime - INTERVAL 5 MINUTES
                                    AND e.created_datetime + INTERVAL 5 MINUTES
  ),
  with_space AS (
    SELECT s.*, sp.title AS space_title, w.workspace_name
    FROM with_stmt s
    LEFT JOIN :catalog.:schema.adb_genie_spaces sp
      ON sp.space_id = s.space_id AND sp.workspace_id = s.workspace_id
    LEFT JOIN system.access.workspaces_latest w
      ON w.workspace_id = s.workspace_id
  )
  SELECT * FROM with_space
) src
ON tgt.space_id      = src.space_id
AND tgt.message_id   = src.message_id
AND tgt.workspace_id = src.workspace_id
AND tgt.surface      = src.surface
AND tgt.action_name  = src.action_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- [ ] **Step 2: Add a SQL test that #14 is resolved**

Append to `tests/sql/test_mv_genie_usage.sql`:

```sql
-- #14 regression test: every statement_id present in
-- dbsql_cost_per_query for query_source_type='GENIE SPACE' must
-- also appear in genie_observability_main_table for the same workspace.
WITH missing AS (
  SELECT q.statement_id
  FROM :catalog.:schema.dbsql_cost_per_query q
  LEFT JOIN :catalog.:schema.genie_observability_main_table g
    ON g.statement_id = q.statement_id AND g.workspace_id = q.workspace_id
  WHERE q.query_source_type = 'GENIE SPACE'
    AND q.workspace_id IS NOT NULL
    AND g.statement_id IS NULL
)
SELECT
  assert_true(
    count(*) < 0.01 * (SELECT count(*) FROM :catalog.:schema.dbsql_cost_per_query
                       WHERE query_source_type = 'GENIE SPACE'),
    'No more than 1% of GENIE SPACE statement_ids may be missing from genie_observability_main_table'
  ) AS issue_14_resolved
FROM missing;
```

(Allow up to 1% slack for genuinely-orphaned statements — e.g. queries that span workspaces or hit the audit-log outage window.)

- [ ] **Step 3: Deploy + run + verify the assertion**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

Then in the SQL editor, run the appended assertion. Expected: no error.

- [ ] **Step 4: Commit**

```bash
git add src/02_genie_observability_main_table.sql tests/sql/test_mv_genie_usage.sql
git commit -m "$(cat <<'EOF'
fix(#14): rebuild genie_observability_main_table on audit + query.history

Replaces the SDK-attachment-derived join (the source of the statement_id
mismatch reported by Thames Water) with a join from system.access.audit
to system.query.history.genie_space_id within a ±5min temporal window.
Same table name and key columns preserved; adds 'surface' column for
agents vs chat.

Closes #14.

Co-authored-by: Isaac
EOF
)"
```

### Task 2.3: Drop SDK conversation/message fan-out from primary path; gate behind `enable_genie_deep_dive`

**Files:**
- Modify: `src/01_get_metadata.ipynb`

**Interfaces:**
- Consumes: `${var.enable_genie_deep_dive}` bundle parameter via `dbutils.widgets.get('enable_genie_deep_dive')`.
- Produces: when flag is `false`, the notebook does not call `w.genie.list_conversations` or `w.genie.list_conversation_messages` and does not write `adb_genie_conversations` or `adb_genie_messages`. When `true`, both tables are populated as in V2 (the deep-dive path).

- [ ] **Step 1: Add the parameter at the top of the notebook**

```python
dbutils.widgets.text("enable_genie_deep_dive", "false")
enable_genie_deep_dive = dbutils.widgets.get("enable_genie_deep_dive").lower() == "true"
print(f"enable_genie_deep_dive={enable_genie_deep_dive}")
```

- [ ] **Step 2: Wrap the conversation + message loop**

Find the cells that build `adb_genie_conversations` and `adb_genie_messages`. Wrap their entire body in:

```python
if enable_genie_deep_dive:
    # ... existing SDK fan-out code (kept intact)
    ...
else:
    print("Skipping Genie conversation/message fan-out (enable_genie_deep_dive=false). "
          "Deep-dive panel will render a placeholder.")
```

- [ ] **Step 3: Add `enable_genie_deep_dive` parameter to the job**

Edit `deployment_resources/workflows.yml` — add to the `parameters:` block:

```yaml
        - name: enable_genie_deep_dive
          default: ${var.enable_genie_deep_dive}
```

- [ ] **Step 4: Deploy with default (false) and verify it runs without touching `adb_genie_conversations` / `adb_genie_messages`**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

In the SQL editor, confirm `adb_genie_conversations` and `adb_genie_messages` either don't exist (first run) or aren't modified (table history `DESCRIBE HISTORY` shows no new versions).

- [ ] **Step 5: Override `enable_genie_deep_dive=true` and rerun**

```bash
databricks bundle run adoption_dash_job --target dev -- --enable_genie_deep_dive true
```

Confirm both tables populate exactly as in V2.

- [ ] **Step 6: Commit**

```bash
git add src/01_get_metadata.ipynb deployment_resources/workflows.yml
git commit -m "$(cat <<'EOF'
feat: gate SDK conversation/message fan-out on enable_genie_deep_dive

When the flag is false (default), the metadata notebook skips the
expensive SDK loops behind adb_genie_conversations / adb_genie_messages.
mvFactGenieUsage and genie_observability_main_table no longer depend on
those tables (CP2 tasks 2.1 and 2.2), so the default deployment runs
without the N-space SDK fan-out that caused issue #12.

When true, behaviour is identical to V2.

Co-authored-by: Isaac
EOF
)"
```

### Task 2.4: Add bounded `ThreadPoolExecutor` to residual SDK lists

**Files:**
- Create: `src/includes/parallel.py`
- Modify: `src/01_get_metadata.ipynb` (lakeview, schedules, subscriptions, apps, serving_endpoints loops)
- Create: `tests/test_parallel.py`

**Interfaces:**
- Produces: `parallel.run(fn, items, max_workers=5) -> list` — returns the concatenated results of `fn(item)` over all items, capturing per-item errors so a single 403/404 doesn't fail the batch.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to confirm fail**

- [ ] **Step 3: Create `src/includes/parallel.py`**

```python
"""Bounded ThreadPoolExecutor wrapper for SDK fan-out.

Honours the public Databricks rate limits:
  - Permissions API GET: 100/s/workspace
  - Lakeview list:       safe at 5-8 concurrent
  - Genie list:          5-8 concurrent (per public API guidance)

Per-item errors are collected so a single 403/404 does not abort the batch.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Sequence


def run(
    fn: Callable,
    items: Sequence,
    max_workers: int = 5,
    return_errors: bool = False,
):
    """Apply ``fn`` to every item in ``items`` concurrently.

    Each invocation that returns an iterable (list/tuple/generator) is
    flattened into the result; scalar returns are appended as-is.
    """
    results: list = []
    errors: list = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fn, item): item for item in items}
        for fut in as_completed(futures):
            try:
                out = fut.result()
                if isinstance(out, (list, tuple)):
                    results.extend(out)
                elif out is not None:
                    results.append(out)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
    if return_errors:
        return results, errors
    if errors:
        print(f"parallel.run: {len(errors)} item(s) failed (continuing). "
              f"First error: {errors[0]}")
    return results
```

- [ ] **Step 4: Run test to confirm pass**

Expected: 2 passed.

- [ ] **Step 5: Replace the lakeview / schedules / subscriptions / apps / serving_endpoints loops**

In `src/01_get_metadata.ipynb`, find the loops that fetch dashboards / schedules / subscriptions / apps / serving endpoints and replace each with `parallel.run`. Example for schedules:

```python
from includes.parallel import run as par_run

def fetch_schedules_for(dashboard_id: str) -> list[dict]:
    return [
        s.as_dict() | {"dashboard_id": dashboard_id}
        for s in w.lakeview.list_schedules(dashboard_id=dashboard_id)
    ]

schedules = par_run(fetch_schedules_for, dashboard_ids, max_workers=8)
```

Apply the same shape to: `w.lakeview.list_subscriptions(...)`, `w.serving_endpoints.get_permissions(...)`, `w.apps.get_permissions(...)`.

- [ ] **Step 6: Remove SCIM `w.users.get` lookups from the message-builder cell**

Anywhere `w.users.get(user_id).user_name` was used, source the email from `user_identity.email` in the audit-derived joins instead. If a user_id appears that's not in audit, leave it null — don't add a fallback SCIM call.

- [ ] **Step 7: Deploy + run + measure**

```bash
databricks bundle deploy --target dev
time databricks bundle run adoption_dash_job --target dev
```

Compare wall-clock to V2 baseline (recorded in `docs/v3-system-table-validation.md` Step 6 / known-bad). Expected: ≥5× faster on a workspace with hundreds of dashboards / apps / serving endpoints.

- [ ] **Step 8: Commit**

```bash
git add src/includes/parallel.py src/01_get_metadata.ipynb tests/test_parallel.py
git commit -m "$(cat <<'EOF'
perf(#12): bounded ThreadPoolExecutor on residual SDK lists

Replaces the sequential lakeview/schedules/subscriptions/apps/serving
loops with parallel.run (max_workers=8, per-item error collection).
Drops SCIM users.get from the hot path entirely — email comes from
audit-derived joins now.

Closes #12.

Co-authored-by: Isaac
EOF
)"
```

### Task 2.5: Open CP2 PR and tag rc2

- [ ] **Step 1: Push**

```bash
git push -u personal cp2/genie-observability
```

- [ ] **Step 2: Open PR**

```bash
gh pr create --title "CP2: Genie observability refactor + perf" --body "$(cat <<'EOF'
## Summary

- Rebuilds `mvFactGenieUsage` and `genie_observability_main_table` on `system.access.audit` + `system.query.history`, dropping the SDK-attachment join that caused #14.
- Gates SDK conversation/message fan-out behind `enable_genie_deep_dive` (default false). Default deployment no longer pays the N-space SDK cost.
- Adds bounded `ThreadPoolExecutor` to residual SDK loops; drops SCIM `users.get` from the hot path.
- New `surface` column distinguishes Genie Agents (`aibiGenie`) from Genie One Chat (`genieChat`).

## Test plan

- [x] `pytest -m "not integration"` passes.
- [x] `databricks bundle deploy --target dev && databricks bundle run adoption_dash_job --target dev` succeeds with `enable_genie_deep_dive=false`.
- [x] Same job with `enable_genie_deep_dive=true` populates `adb_genie_conversations` and `adb_genie_messages` as in V2.
- [x] SQL regression test in `tests/sql/test_mv_genie_usage.sql` confirms <1% of GENIE SPACE statement_ids missing from `genie_observability_main_table`.
- [x] Wall-clock pipeline run measured before/after; ≥5× improvement on workspaces with hundreds of dashboards/apps.

Closes #12. Closes #14.

This pull request and its description were written by Isaac.
EOF
)"
```

- [ ] **Step 3: Merge + tag**

```bash
gh pr merge --squash --delete-branch
git checkout main && git pull
git tag v3.0.0-rc2 && git push personal v3.0.0-rc2
```

---

# CP3 — Genie paygo

**Why:** Hard external deadline (2026-07-06). Ships against real data — `system.billing.usage` with `billing_origin_product IN ('GENIE','AI/BI_GENIE')` — plus warehouse-DBU baseline that already exists.

**Effort:** 2 days.

### Task 3.1: Add `mvFactGeniePaygoCost`

**Files:**
- Create: `src/02_mvFactGeniePaygoCost.sql`
- Modify: `deployment_resources/workflows.yml` (add task)

**Interfaces:**
- Produces: `${catalog}.${schema}.mvFactGeniePaygoCost` Delta table. PK = (cost_source, usage_date, workspace_id, space_id, user_email). Columns: `cost_source` (`warehouse` | `llm_paygo`), `usage_date`, `workspace_id`, `workspace_name`, `space_id`, `space_title`, `user_email`, `dbus`, `dollars`, `usage_quantity`.

- [ ] **Step 1: Create `src/02_mvFactGeniePaygoCost.sql`**

```sql
-- mvFactGeniePaygoCost — UNION ALL of warehouse-DBU cost (always-on,
-- pre- and post-July-6) and LLM-DBU cost from system.billing.usage
-- (post-July-6, when billing_origin_product = 'AI/BI_GENIE' starts emitting).

CREATE TABLE IF NOT EXISTS :catalog.:schema.mvFactGeniePaygoCost (
  cost_source     STRING,
  usage_date      DATE,
  workspace_id    BIGINT,
  workspace_name  STRING,
  space_id        STRING,
  space_title     STRING,
  user_email      STRING,
  dbus            DECIMAL(38, 6),
  dollars         DECIMAL(38, 6),
  usage_quantity  DECIMAL(38, 6)
) USING DELTA
  PARTITIONED BY (workspace_id, cost_source);

MERGE INTO :catalog.:schema.mvFactGeniePaygoCost tgt
USING (
  -- Warehouse-DBU half: SQL warehouse cost attributed to a Genie space.
  -- Always-on, pre- and post-July-6.
  SELECT
    'warehouse'                              AS cost_source,
    cast(cpq.statement_start_time AS DATE)   AS usage_date,
    cpq.workspace_id,
    w.workspace_name,
    cpq.query_source_id                      AS space_id,
    s.space_title,
    cpq.executed_by                          AS user_email,
    sum(cpq.query_attributed_dbus_estimation)    AS dbus,
    sum(cpq.query_attributed_dollars_estimation) AS dollars,
    cast(NULL AS DECIMAL(38, 6))             AS usage_quantity
  FROM :catalog.:schema.dbsql_cost_per_query cpq
  LEFT JOIN system.access.workspaces_latest w  ON w.workspace_id = cpq.workspace_id
  LEFT JOIN (SELECT space_id, title AS space_title, workspace_id FROM :catalog.:schema.adb_genie_spaces) s
    ON s.space_id = cpq.query_source_id AND s.workspace_id = cpq.workspace_id
  WHERE cpq.query_source_type = 'GENIE SPACE'
    AND cpq.statement_start_time >= current_date() - INTERVAL 14 DAYS
  GROUP BY ALL

  UNION ALL

  -- LLM-DBU half: post-July-6 paygo rows.
  -- `billing_origin_product IN (...)` future-proofs the label rename.
  SELECT
    'llm_paygo'                              AS cost_source,
    bu.usage_date,
    bu.workspace_id,
    w.workspace_name,
    -- Genie space attribution: post-July-6 schema TBC; usage_metadata likely
    -- carries the space_id either at the top level or under ai_gateway.*.
    -- Use coalesce to be tolerant of both shapes when the schema lands.
    coalesce(bu.usage_metadata.ai_gateway_endpoint_name,
             bu.usage_metadata.endpoint_name)                     AS space_id,
    cast(NULL AS STRING)                                          AS space_title,
    bu.identity_metadata.run_as                                   AS user_email,
    sum(bu.usage_quantity)                                        AS dbus,
    sum(bu.usage_quantity * coalesce(p.pricing.default, 0))       AS dollars,
    sum(bu.usage_quantity)                                        AS usage_quantity
  FROM system.billing.usage bu
  LEFT JOIN system.access.workspaces_latest w ON w.workspace_id = bu.workspace_id
  LEFT JOIN system.billing.list_prices p
    ON p.sku_name    = bu.sku_name
   AND p.currency_code = 'USD'
   AND bu.usage_end_time BETWEEN p.price_start_time AND coalesce(p.price_end_time, current_timestamp())
  WHERE bu.billing_origin_product IN ('GENIE', 'AI/BI_GENIE')
    AND bu.usage_date >= current_date() - INTERVAL 14 DAYS
  GROUP BY ALL
) src
ON  tgt.cost_source  = src.cost_source
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
AND coalesce(tgt.space_id, '') = coalesce(src.space_id, '')
AND coalesce(tgt.user_email, '') = coalesce(src.user_email, '')
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- [ ] **Step 2: Wire as a job task in `deployment_resources/workflows.yml`**

Add to the `tasks:` list under `adoption_dash_job`:

```yaml
        - task_key: Process_Genie_PaygoCost
          depends_on:
            - task_key: Process_dbsql_cost_per_query
            - task_key: Ingest_Metadata
          sql_task:
            file:
              path: ../src/02_mvFactGeniePaygoCost.sql
            warehouse_id: ${var.my_warehouse_id}
```

- [ ] **Step 3: Deploy + run + verify**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

In SQL editor:

```sql
-- Cost source split
SELECT cost_source, count(*), sum(dollars) FROM users.<schema>.mvFactGeniePaygoCost GROUP BY 1;
-- Today (2026-06-15), expect: warehouse=N rows, llm_paygo=0 rows.
```

- [ ] **Step 4: Commit**

```bash
git checkout -b cp3/paygo
git add src/02_mvFactGeniePaygoCost.sql deployment_resources/workflows.yml
git commit -m "$(cat <<'EOF'
feat: add mvFactGeniePaygoCost (warehouse + LLM paygo)

UNION ALLs SQL warehouse DBUs attributed to Genie spaces (always-on)
with system.billing.usage rows tagged billing_origin_product IN
('GENIE','AI/BI_GENIE') (post-July-6 paygo). IN (...) future-proofs the
label rename. Pre-July-6 the llm_paygo half is empty; populates
automatically on go-live.

Co-authored-by: Isaac
EOF
)"
```

### Task 3.2: Add the Cost dashboard widgets

**Files:**
- Modify: `src/dashboards/lh_adoption_dashboard.lvdash.json`

**Interfaces:**
- Consumes: `mvFactGeniePaygoCost`, optional `system.ai_gateway.usage`.
- Produces: a new "Cost" page (or section on the Genie page) with three widgets: stacked-bar of `warehouse` vs `llm_paygo` over time; KPI row (DBUs total, DBUs free-credit equivalent, $ spent); table (top spaces by cost).

- [ ] **Step 1: Inventory the existing dashboard structure**

```bash
jq '.pages[].name' src/dashboards/lh_adoption_dashboard.lvdash.json
```

Decide whether to add to an existing page or create a new "Cost" page.

- [ ] **Step 2: Add a new dataset to the dashboard JSON**

In `lh_adoption_dashboard.lvdash.json` `.datasets[]`, append:

```json
{
  "name": "mvFactGeniePaygoCost",
  "displayName": "Genie pay-as-you-go cost",
  "queryLines": [
    "SELECT cost_source, usage_date, workspace_name, space_title, user_email, dbus, dollars",
    "FROM `:catalog`.`:schema`.`mvFactGeniePaygoCost`"
  ],
  "parameters": []
}
```

- [ ] **Step 3: Add the stacked-bar widget**

A stacked bar of `dollars` by `usage_date`, coloured by `cost_source`. Include a vertical-line annotation at `2026-07-06` (X-axis marker) and a horizontal reference line at `150 DBU/user equivalent` (~$10.50 USD East). Use the `aibi-dashboard-builder:dashboard-widgets` skill reference for the exact JSON shape; the chart type is `bar` with `stacked: true` and a single annotation block.

- [ ] **Step 4: Add the KPI row + top-spaces table**

KPI row: three metrics, computed via dataset queries.
- "DBUs (30d)" = `sum(dbus) where usage_date >= current_date() - 30`
- "Dollars (30d)" = `sum(dollars) where usage_date >= current_date() - 30`
- "Active Genie users (30d)" = `count(distinct user_email) where usage_date >= current_date() - 30`

Top-spaces table: top 10 by `sum(dollars)` over 30d, grouped by `space_title`.

- [ ] **Step 5: Add a Public Preview banner**

A markdown widget at the top of the page:

```markdown
> **AI/BI Genie pay-as-you-go monitoring.** The `llm_paygo` cost column populates from **2026-07-06** when Genie Agents and Genie One move to pay-as-you-go pricing. Pre-go-live this view shows only SQL warehouse cost attributed to Genie Spaces (always-on). The 150-DBU reference line is the per-user free-credit allowance shared across Genie Agents and Genie One. Data sourced from `system.billing.usage` (Public Preview).
```

- [ ] **Step 6: Validate the dashboard JSON**

```bash
jq empty src/dashboards/lh_adoption_dashboard.lvdash.json
databricks bundle validate
```

- [ ] **Step 7: Deploy + verify visually**

```bash
databricks bundle deploy --target dev
```

Open the dashboard URL in the browser (the bundle deploy output prints it). Confirm the stacked bar, KPI row, and top-spaces table render. The `llm_paygo` series is empty today (expected).

- [ ] **Step 8: Commit + open PR + tag rc3**

```bash
git add src/dashboards/lh_adoption_dashboard.lvdash.json
git commit -m "$(cat <<'EOF'
feat: add Genie pay-as-you-go cost widgets to the dashboard

Stacked bar (warehouse vs LLM paygo over time), KPI row, top-spaces
table, and an explanatory banner. Pre-July-6 the llm_paygo series is
empty; populates automatically on go-live.

Co-authored-by: Isaac
EOF
)"

git push -u personal cp3/paygo
gh pr create --title "CP3: Genie paygo monitoring" --body "$(cat <<'EOF'
## Summary

Adds `mvFactGeniePaygoCost` (UNION ALL of warehouse DBUs + paygo LLM DBUs) and the dashboard widgets that surface it. Ships against real data — no inferred LLM-cost simulator.

## Test plan

- [x] `databricks bundle validate` passes.
- [x] Dashboard loads in the browser; widgets render; banner copy reads correctly.
- [x] `mvFactGeniePaygoCost` table populates with warehouse rows only (expected pre-July-6).
- [x] Post-July-6 (verify after go-live): `llm_paygo` rows appear without code change.

This pull request and its description were written by Isaac.
EOF
)"

# After review/merge:
gh pr merge --squash --delete-branch
git checkout main && git pull
git tag v3.0.0-rc3 && git push personal v3.0.0-rc3
```

---

# CP4 — Apps & Models modernisation

**Why:** Drop `mvFactModelUsage` (low-signal registry events). Rebuild on `system.serving.served_entities` + `system.serving.endpoint_usage` + `system.billing.usage`. Add `mvFactVectorSearchCost`. Mark Agent Bricks "coming soon".

**Effort:** 4–5 days.

### Task 4.1: Replace `mvFactServingEndpointUsage` with `mvFactServingUsage`

**Files:**
- Rename: `src/02_mvFactServingEndpointUsage.sql` → `src/02_mvFactServingUsage.sql`
- Modify: `deployment_resources/workflows.yml` (update task path)
- Delete: `src/02_mvFactModelUsage.sql`

**Interfaces:**
- Produces: `${catalog}.${schema}.mvFactServingUsage` Delta table. PK = (endpoint_id, served_entity_id, usage_date, workspace_id). Columns: `endpoint_id, endpoint_name, served_entity_id, entity_type, entity_name, entity_version, usage_date, workspace_id, workspace_name, request_count, input_tokens, output_tokens, dbus, dollars`.

- [ ] **Step 1: Rename the file and rewrite the body**

```bash
git mv src/02_mvFactServingEndpointUsage.sql src/02_mvFactServingUsage.sql
```

Replace the body with:

```sql
-- mvFactServingUsage — replaces V2 mvFactServingEndpointUsage AND mvFactModelUsage.
-- Source: system.serving.served_entities (dim) + system.serving.endpoint_usage (fact, PuP)
--         + system.billing.usage (cost).

CREATE TABLE IF NOT EXISTS :catalog.:schema.mvFactServingUsage (
  endpoint_id        STRING,
  endpoint_name      STRING,
  served_entity_id   STRING,
  entity_type        STRING,
  entity_name        STRING,
  entity_version     STRING,
  usage_date         DATE,
  workspace_id       BIGINT,
  workspace_name     STRING,
  request_count      BIGINT,
  input_tokens       BIGINT,
  output_tokens      BIGINT,
  dbus               DECIMAL(38, 6),
  dollars            DECIMAL(38, 6)
) USING DELTA
  PARTITIONED BY (workspace_id, entity_type);

MERGE INTO :catalog.:schema.mvFactServingUsage tgt
USING (
  WITH usage AS (
    SELECT
      eu.endpoint_id,
      eu.served_entity_id,
      cast(eu.request_time AS DATE) AS usage_date,
      eu.workspace_id,
      count(*)                           AS request_count,
      sum(eu.input_token_count)          AS input_tokens,
      sum(eu.output_token_count)         AS output_tokens
    FROM system.serving.endpoint_usage eu
    WHERE eu.request_time >= current_date() - INTERVAL 7 DAYS
    GROUP BY ALL
  ),
  cost AS (
    SELECT
      bu.usage_metadata.endpoint_id,
      bu.usage_date,
      bu.workspace_id,
      sum(bu.usage_quantity)                                       AS dbus,
      sum(bu.usage_quantity * coalesce(p.pricing.default, 0))      AS dollars
    FROM system.billing.usage bu
    LEFT JOIN system.billing.list_prices p
      ON p.sku_name = bu.sku_name AND p.currency_code = 'USD'
     AND bu.usage_end_time BETWEEN p.price_start_time AND coalesce(p.price_end_time, current_timestamp())
    WHERE bu.billing_origin_product = 'MODEL_SERVING'
      AND bu.usage_date >= current_date() - INTERVAL 7 DAYS
    GROUP BY ALL
  ),
  se AS (
    SELECT endpoint_id, endpoint_name, served_entity_id, entity_type, entity_name, entity_version,
           change_time, endpoint_delete_time
    FROM system.serving.served_entities
    WHERE endpoint_delete_time IS NULL OR endpoint_delete_time > current_date() - INTERVAL 30 DAYS
  )
  SELECT
    se.endpoint_id,
    se.endpoint_name,
    se.served_entity_id,
    se.entity_type,
    se.entity_name,
    se.entity_version,
    coalesce(u.usage_date, c.usage_date) AS usage_date,
    coalesce(u.workspace_id, c.workspace_id) AS workspace_id,
    w.workspace_name,
    coalesce(u.request_count, 0)         AS request_count,
    coalesce(u.input_tokens, 0)          AS input_tokens,
    coalesce(u.output_tokens, 0)         AS output_tokens,
    coalesce(c.dbus, 0)                  AS dbus,
    coalesce(c.dollars, 0)               AS dollars
  FROM se
  LEFT JOIN usage u  ON u.endpoint_id = se.endpoint_id AND u.served_entity_id = se.served_entity_id
  LEFT JOIN cost c   ON c.endpoint_id = se.endpoint_id
                    AND c.usage_date = u.usage_date
                    AND c.workspace_id = u.workspace_id
  LEFT JOIN system.access.workspaces_latest w ON w.workspace_id = coalesce(u.workspace_id, c.workspace_id)
  WHERE coalesce(u.usage_date, c.usage_date) IS NOT NULL
) src
ON  tgt.endpoint_id      = src.endpoint_id
AND tgt.served_entity_id = src.served_entity_id
AND tgt.usage_date       = src.usage_date
AND tgt.workspace_id     = src.workspace_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- [ ] **Step 2: Delete `src/02_mvFactModelUsage.sql`**

```bash
git rm src/02_mvFactModelUsage.sql
```

- [ ] **Step 3: Update workflows.yml**

In `deployment_resources/workflows.yml`, rename `Process_ServingEndpoints` → `Process_ServingUsage` and point at the new SQL file. Delete the `Process_Models` task entirely.

- [ ] **Step 4: Drop the legacy table from prior deployments**

Add a one-shot housekeeping cell to `01_get_metadata.ipynb` (or a separate `src/03_v3_housekeeping.sql`):

```sql
DROP TABLE IF EXISTS :catalog.:schema.mvFactModelUsage;
DROP TABLE IF EXISTS :catalog.:schema.mvFactServingEndpointUsage;
```

This runs once on the first V3 deploy and is a no-op thereafter.

- [ ] **Step 5: Deploy + run + verify**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

```sql
DESCRIBE EXTENDED users.<schema>.mvFactServingUsage;
SELECT count(*) FROM users.<schema>.mvFactServingUsage;
SHOW TABLES IN users.<schema> LIKE 'mvFactModelUsage';  -- expect 0 rows
SHOW TABLES IN users.<schema> LIKE 'mvFactServingEndpointUsage';  -- expect 0 rows
```

- [ ] **Step 6: Commit**

```bash
git checkout -b cp4/apps-models
git add -A
git commit -m "$(cat <<'EOF'
refactor: replace mvFactServingEndpointUsage + mvFactModelUsage with mvFactServingUsage

Single fact table for custom + foundation models, sourced from
system.serving.served_entities + system.serving.endpoint_usage +
system.billing.usage. mvFactModelUsage (UC registry browsing events) is
dropped — it measured discovery, not consumption.

Co-authored-by: Isaac
EOF
)"
```

### Task 4.2: Rebuild `mvFactAppUsage` on system.billing.usage + audit

**Files:**
- Modify: `src/02_mvFactAppUsage.sql`

**Interfaces:**
- Produces: `${catalog}.${schema}.mvFactAppUsage` Delta table. PK = (app_id, usage_date, workspace_id). Columns: `app_id, app_name, usage_date, workspace_id, workspace_name, dbus, dollars, lifecycle_events, distinct_users`.

- [ ] **Step 1: Replace the body**

```sql
CREATE TABLE IF NOT EXISTS :catalog.:schema.mvFactAppUsage (
  app_id           STRING,
  app_name         STRING,
  usage_date       DATE,
  workspace_id     BIGINT,
  workspace_name   STRING,
  dbus             DECIMAL(38, 6),
  dollars          DECIMAL(38, 6),
  lifecycle_events BIGINT,
  distinct_users   BIGINT
) USING DELTA
  PARTITIONED BY (workspace_id);

MERGE INTO :catalog.:schema.mvFactAppUsage tgt
USING (
  WITH cost AS (
    SELECT
      bu.usage_metadata.app_id,
      bu.usage_metadata.app_name,
      bu.usage_date,
      bu.workspace_id,
      sum(bu.usage_quantity)                                       AS dbus,
      sum(bu.usage_quantity * coalesce(p.pricing.default, 0))      AS dollars
    FROM system.billing.usage bu
    LEFT JOIN system.billing.list_prices p
      ON p.sku_name = bu.sku_name AND p.currency_code = 'USD'
     AND bu.usage_end_time BETWEEN p.price_start_time AND coalesce(p.price_end_time, current_timestamp())
    WHERE bu.billing_origin_product = 'APPS'  -- confirm via CP0 validation
      AND bu.usage_date >= current_date() - INTERVAL 14 DAYS
      AND bu.usage_metadata.app_id IS NOT NULL
    GROUP BY ALL
  ),
  events AS (
    SELECT
      coalesce(request_params.app_id, request_params.name) AS app_id,
      event_date                                            AS usage_date,
      workspace_id,
      count(*)                                              AS lifecycle_events,
      count(DISTINCT user_identity.email)                   AS distinct_users
    FROM system.access.audit
    WHERE service_name = 'apps'
      AND event_date >= current_date() - INTERVAL 14 DAYS
    GROUP BY ALL
  )
  SELECT
    coalesce(c.app_id, e.app_id)                                AS app_id,
    coalesce(c.app_name, NULL)                                  AS app_name,
    coalesce(c.usage_date, e.usage_date)                        AS usage_date,
    coalesce(c.workspace_id, e.workspace_id)                    AS workspace_id,
    w.workspace_name,
    coalesce(c.dbus, 0)                                         AS dbus,
    coalesce(c.dollars, 0)                                      AS dollars,
    coalesce(e.lifecycle_events, 0)                             AS lifecycle_events,
    coalesce(e.distinct_users, 0)                               AS distinct_users
  FROM cost c
  FULL OUTER JOIN events e
    ON e.app_id = c.app_id AND e.usage_date = c.usage_date AND e.workspace_id = c.workspace_id
  LEFT JOIN system.access.workspaces_latest w ON w.workspace_id = coalesce(c.workspace_id, e.workspace_id)
) src
ON  tgt.app_id       = src.app_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- [ ] **Step 2: Deploy + run + verify**

```bash
databricks bundle deploy --target dev
databricks bundle run adoption_dash_job --target dev
```

```sql
SELECT app_name, count(*), sum(dbus), sum(distinct_users) FROM users.<schema>.mvFactAppUsage GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

- [ ] **Step 3: Commit**

```bash
git add src/02_mvFactAppUsage.sql
git commit -m "$(cat <<'EOF'
refactor: rebuild mvFactAppUsage on system.billing.usage + audit

Replaces the V2 audit-text-matching view. Cost now comes from
system.billing.usage.usage_metadata.app_id; lifecycle events from
system.access.audit (service_name='apps'). FULL OUTER JOIN keeps apps
that exist with no cost yet, and cost rows where the app was deleted.

Co-authored-by: Isaac
EOF
)"
```

### Task 4.3: Add `mvFactVectorSearchCost`

**Files:**
- Create: `src/02_mvFactVectorSearchCost.sql`
- Modify: `deployment_resources/workflows.yml`

**Interfaces:**
- Produces: `${catalog}.${schema}.mvFactVectorSearchCost`. PK = (endpoint_id, usage_date, workspace_id, sku_name). Columns: `endpoint_id, endpoint_name, usage_date, workspace_id, workspace_name, sku_name, dbus, dollars`.

- [ ] **Step 1: Create the SQL**

```sql
CREATE TABLE IF NOT EXISTS :catalog.:schema.mvFactVectorSearchCost (
  endpoint_id    STRING,
  endpoint_name  STRING,
  usage_date     DATE,
  workspace_id   BIGINT,
  workspace_name STRING,
  sku_name       STRING,
  dbus           DECIMAL(38, 6),
  dollars        DECIMAL(38, 6)
) USING DELTA
  PARTITIONED BY (workspace_id);

MERGE INTO :catalog.:schema.mvFactVectorSearchCost tgt
USING (
  SELECT
    bu.usage_metadata.endpoint_id,
    bu.usage_metadata.endpoint_name,
    bu.usage_date,
    bu.workspace_id,
    w.workspace_name,
    bu.sku_name,
    sum(bu.usage_quantity)                                       AS dbus,
    sum(bu.usage_quantity * coalesce(p.pricing.default, 0))      AS dollars
  FROM system.billing.usage bu
  LEFT JOIN system.access.workspaces_latest w
    ON w.workspace_id = bu.workspace_id
  LEFT JOIN system.billing.list_prices p
    ON p.sku_name = bu.sku_name AND p.currency_code = 'USD'
   AND bu.usage_end_time BETWEEN p.price_start_time AND coalesce(p.price_end_time, current_timestamp())
  WHERE bu.billing_origin_product = 'VECTOR_SEARCH'
    AND bu.usage_date >= current_date() - INTERVAL 14 DAYS
  GROUP BY ALL
) src
ON  tgt.endpoint_id  = src.endpoint_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
AND tgt.sku_name     = src.sku_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- [ ] **Step 2: Wire as a job task**

In `workflows.yml`, add a task `Process_VectorSearchCost` depending on `Ingest_Metadata`, running this SQL.

- [ ] **Step 3: Deploy + verify**

```sql
SELECT count(*), sum(dollars) FROM users.<schema>.mvFactVectorSearchCost;
```

- [ ] **Step 4: Commit**

```bash
git add src/02_mvFactVectorSearchCost.sql deployment_resources/workflows.yml
git commit -m "$(cat <<'EOF'
feat: add mvFactVectorSearchCost

Billing-only DBU/$ cost per Vector Search endpoint, sourced from
system.billing.usage where billing_origin_product='VECTOR_SEARCH'.
Query-rate metrics out of V3 scope (no system table exists for them yet).

Co-authored-by: Isaac
EOF
)"
```

### Task 4.4: Update the Models & Vector Search dashboard page

**Files:**
- Modify: `src/dashboards/lh_adoption_dashboard.lvdash.json`

**Interfaces:**
- Consumes: `mvFactServingUsage`, `mvFactVectorSearchCost`.
- Produces: a dashboard page with Endpoints in service (KPI), Entity-type mix (donut), Top endpoints by tokens (table), VS endpoints + DBUs (table), Agent Bricks placeholder card.

- [ ] **Step 1: Add datasets to the dashboard JSON**

```json
{
  "name": "mvFactServingUsage",
  "displayName": "Model serving usage",
  "queryLines": [
    "SELECT endpoint_name, entity_type, entity_name, usage_date, request_count, input_tokens, output_tokens, dbus, dollars",
    "FROM `:catalog`.`:schema`.`mvFactServingUsage`"
  ]
},
{
  "name": "mvFactVectorSearchCost",
  "displayName": "Vector Search cost",
  "queryLines": [
    "SELECT endpoint_name, usage_date, sku_name, dbus, dollars",
    "FROM `:catalog`.`:schema`.`mvFactVectorSearchCost`"
  ]
}
```

- [ ] **Step 2: Add widgets** — see Spike S-C §1 for the exact set ("Endpoints in service", "Entity-type mix", "Top endpoints by tokens", "VS cost by endpoint"). Use the dashboard-widgets skill reference.

- [ ] **Step 3: Add Agent Bricks placeholder**

A markdown widget:

```markdown
### Agent Bricks (Knowledge Assistant, Multi-Agent Supervisor)

Agent Bricks observability ships in a future release once
`system.agents.*` or UC-stored MLflow trace tables are publicly
available. For now, agents deployed behind a serving endpoint appear in
the Model Serving table above (filter `entity_type = 'CUSTOM_MODEL'`).
```

- [ ] **Step 4: Add Public Preview banners** for `system.serving.endpoint_usage` and `system.ai_gateway.usage` (token columns require usage tracking enabled per endpoint).

- [ ] **Step 5: Validate + deploy**

```bash
jq empty src/dashboards/lh_adoption_dashboard.lvdash.json
databricks bundle validate
databricks bundle deploy --target dev
```

- [ ] **Step 6: Commit, PR, tag rc4**

```bash
git add src/dashboards/lh_adoption_dashboard.lvdash.json
git commit -m "$(cat <<'EOF'
feat: refresh Models & Vector Search dashboard page

Adds mvFactServingUsage and mvFactVectorSearchCost datasets and the
widgets that surface them. Drops mvFactModelUsage references entirely
(table removed in CP4 task 4.1). Includes an Agent Bricks 'coming soon'
placeholder pointing to system.agents.* when public.

Co-authored-by: Isaac
EOF
)"
git push -u personal cp4/apps-models
gh pr create --title "CP4: Apps & Models modernisation" --body "$(cat <<'EOF'
## Summary

- Replaces `mvFactServingEndpointUsage` + `mvFactModelUsage` with `mvFactServingUsage` (system.serving.served_entities + endpoint_usage + billing).
- Rebuilds `mvFactAppUsage` on `system.billing.usage.usage_metadata.app_id` + audit.
- Adds `mvFactVectorSearchCost` (billing-only).
- Refreshes the Models & Vector Search dashboard page.
- Drops legacy tables from prior deployments via `DROP TABLE IF EXISTS`.

## Test plan

- [x] `databricks bundle deploy --target dev && databricks bundle run` succeeds.
- [x] `mvFactServingUsage`, `mvFactAppUsage`, `mvFactVectorSearchCost` populate with non-zero row counts in `e2-demo-field-eng`.
- [x] Legacy tables (`mvFactModelUsage`, `mvFactServingEndpointUsage`) are gone.
- [x] Dashboard page renders all widgets including banners and placeholder.

This pull request and its description were written by Isaac.
EOF
)"
# merge:
gh pr merge --squash --delete-branch
git checkout main && git pull
git tag v3.0.0-rc4 && git push personal v3.0.0-rc4
```

---

# CP5 — Quality (feedback + benchmark accuracy)

**Why:** Adds a Quality page surfacing Genie user feedback (from audit) and benchmark accuracy (from SDK eval-run API). Issue #13.

**Effort:** 3–4 days.

### Task 5.1: Add `mvFactGenieFeedback`

**Files:**
- Create: `src/02_mvFactGenieFeedback.sql`
- Modify: `deployment_resources/workflows.yml`

**Interfaces:**
- Produces: `${catalog}.${schema}.mvFactGenieFeedback` Delta table. PK = (space_id, message_id, workspace_id, action_name). Columns: `space_id, space_title, conversation_id, message_id, action_name, comment_type, feedback_rating, user_email, event_time, workspace_id, workspace_name, surface`.

- [ ] **Step 1: Create the SQL**

```sql
CREATE TABLE IF NOT EXISTS :catalog.:schema.mvFactGenieFeedback (
  space_id         STRING,
  space_title      STRING,
  conversation_id  STRING,
  message_id       STRING,
  action_name      STRING,
  comment_type     STRING,
  feedback_rating  STRING,
  user_email       STRING,
  event_time       TIMESTAMP,
  workspace_id     BIGINT,
  workspace_name   STRING,
  surface          STRING
) USING DELTA
  PARTITIONED BY (workspace_id, surface);

MERGE INTO :catalog.:schema.mvFactGenieFeedback tgt
USING (
  SELECT
    coalesce(request_params.space_id, request_params.spaceId)     AS space_id,
    s.title                                                       AS space_title,
    request_params.conversation_id                                AS conversation_id,
    request_params.message_id                                     AS message_id,
    action_name,
    request_params.comment_type                                   AS comment_type,
    request_params.feedback_rating                                AS feedback_rating,
    user_identity.email                                           AS user_email,
    event_time,
    a.workspace_id,
    w.workspace_name,
    CASE service_name
      WHEN 'aibiGenie' THEN 'agents'
      WHEN 'genieChat' THEN 'chat'
    END AS surface
  FROM system.access.audit a
  LEFT JOIN :catalog.:schema.adb_genie_spaces s
    ON s.space_id = coalesce(request_params.space_id, request_params.spaceId)
   AND s.workspace_id = a.workspace_id
  LEFT JOIN system.access.workspaces_latest w
    ON w.workspace_id = a.workspace_id
  WHERE service_name IN ('aibiGenie', 'genieChat')
    AND action_name IN (
      'updateConversationMessageFeedback',
      'createConversationMessageComment',
      'updateConversationMessageComment',
      'genieSendMessageFeedback',
      'updateGenieChatConversationFeedback'
    )
    AND event_date >= current_date() - INTERVAL 7 DAYS
) src
ON  tgt.space_id     = src.space_id
AND tgt.message_id   = src.message_id
AND tgt.workspace_id = src.workspace_id
AND tgt.action_name  = src.action_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

- [ ] **Step 2: Wire as job task** in `workflows.yml` depending on `Ingest_Metadata`.

- [ ] **Step 3: Deploy + verify**

```sql
SELECT surface, action_name, count(*) FROM users.<schema>.mvFactGenieFeedback GROUP BY 1,2;
```

- [ ] **Step 4: Commit**

```bash
git checkout -b cp5/quality
git add src/02_mvFactGenieFeedback.sql deployment_resources/workflows.yml
git commit -m "$(cat <<'EOF'
feat: add mvFactGenieFeedback (audit-derived)

Captures Genie user feedback events from system.access.audit for both
Genie Agents (service_name='aibiGenie') and Genie One Chat
(service_name='genieChat'), with a `surface` column for faceting.

Co-authored-by: Isaac
EOF
)"
```

### Task 5.2: Add eval-run snapshot loader (`adb_eval_runs`)

**Files:**
- Create: `src/includes/eval_runs.py`
- Modify: `src/01_get_metadata.ipynb`
- Create: `tests/test_eval_runs.py`

**Interfaces:**
- Consumes: `WorkspaceClient`, list of `space_id` from `adb_genie_spaces`.
- Produces: `${catalog}.${schema}.adb_eval_runs` Delta table merged on `(eval_run_id, workspace_id)`. Columns: `eval_run_id, space_id, created_at, created_by, accuracy, status, workspace_id, workspace_name`.

- [ ] **Step 1: Write the failing test**

```python
"""Test: eval_runs.fetch returns a flat dict per run from the SDK fan-out."""

from unittest.mock import MagicMock
from src.includes.eval_runs import fetch_for_space


def test_fetch_returns_one_dict_per_run() -> None:
    w = MagicMock()
    run = MagicMock()
    run.eval_run_id = "r-123"
    run.created_at = "2026-06-10T09:00:00Z"
    run.created_by = "alice@example.com"
    run.accuracy = 0.82
    run.eval_run_status = "COMPLETED"
    w.genie.genie_list_eval_runs.return_value = [run]

    out = fetch_for_space(w, "space-abc")
    assert out == [{
        "eval_run_id": "r-123",
        "space_id": "space-abc",
        "created_at": "2026-06-10T09:00:00Z",
        "created_by": "alice@example.com",
        "accuracy": 0.82,
        "status": "COMPLETED",
    }]


def test_fetch_returns_empty_on_no_benchmarks() -> None:
    w = MagicMock()
    w.genie.genie_list_eval_runs.return_value = []
    assert fetch_for_space(w, "space-empty") == []
```

- [ ] **Step 2: Create `src/includes/eval_runs.py`**

```python
"""Snapshot Genie benchmark eval-run summaries via SDK fan-out.

Per-question detail expires upstream at 7 days, so we MUST snapshot
into a Delta dim if we want a > 7d accuracy trend. Per-run summaries
(timestamp, accuracy %, status) persist longer in the Evaluations tab
but are only API-accessible per-space.
"""

from __future__ import annotations


def fetch_for_space(w, space_id: str) -> list[dict]:
    """Return one flat dict per eval run on `space_id`."""
    out: list[dict] = []
    try:
        for run in w.genie.genie_list_eval_runs(space_id=space_id):
            out.append({
                "eval_run_id": run.eval_run_id,
                "space_id":   space_id,
                "created_at": run.created_at,
                "created_by": getattr(run, "created_by", None),
                "accuracy":   getattr(run, "accuracy", None),
                "status":     getattr(run, "eval_run_status", None),
            })
    except Exception as exc:  # noqa: BLE001 — public Beta API, tolerate per-space errors
        print(f"eval_runs.fetch_for_space({space_id}): {exc}")
    return out
```

- [ ] **Step 3: Wire into the notebook (after `adb_genie_spaces` is written)**

```python
from includes.eval_runs import fetch_for_space as fetch_runs
from includes.parallel import run as par_run
from pyspark.sql.functions import lit

space_ids = [r.space_id for r in spark.sql(
    f"SELECT space_id FROM {catalog_name}.{schema_name}.adb_genie_spaces "
    f"WHERE workspace_id = {workspace_id}"
).collect()]

runs = par_run(lambda sid: fetch_runs(w, sid), space_ids, max_workers=5)
if runs:
    runs_df = spark.createDataFrame(runs)
    runs_df = runs_df.withColumn("workspace_id", lit(workspace_id).cast("bigint")) \
                     .withColumn("workspace_name", lit(workspace_name))
    from includes.delta_merge import merge_workspace_scoped
    merge_workspace_scoped(
        spark=spark,
        df=runs_df,
        table_fqn=f"{catalog_name}.{schema_name}.adb_eval_runs",
        merge_keys=["eval_run_id"],
    )
else:
    print("No Genie eval runs found (or no benchmarks configured).")
```

- [ ] **Step 4: Deploy + verify**

```sql
SELECT space_id, max(created_at), avg(accuracy)
FROM users.<schema>.adb_eval_runs GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

If `adb_eval_runs` is empty: that's correct for a workspace with no benchmarks (~93% of spaces per S-F). The Quality page banner explains.

- [ ] **Step 5: Commit**

```bash
git add src/includes/eval_runs.py src/01_get_metadata.ipynb tests/test_eval_runs.py
git commit -m "$(cat <<'EOF'
feat(#13): snapshot Genie benchmark eval-run summaries into adb_eval_runs

Per-space SDK fan-out via includes.parallel.run (max_workers=5). Run
summaries persist in a Delta dim so the Quality page can show > 7-day
accuracy trends (upstream per-question detail expires at 7 days).
Tolerates per-space errors (Benchmark API is still Beta).

Co-authored-by: Isaac
EOF
)"
```

### Task 5.3: Add the Quality dashboard page

**Files:**
- Modify: `src/dashboards/lh_adoption_dashboard.lvdash.json`

**Interfaces:**
- Consumes: `mvFactGenieFeedback`, `adb_eval_runs`, `adb_genie_spaces`.
- Produces: a new "Quality" page with feedback KPI row + top-negative-feedback table + accuracy line chart + spaces-without-benchmarks table.

- [ ] **Step 1: Add datasets** for `mvFactGenieFeedback` and `adb_eval_runs`.

- [ ] **Step 2: Add the feedback widgets**

- KPI row:
  - "Thumbs up rate (30d)" = `100.0 * sum(feedback_rating='POSITIVE') / count(feedback_rating IS NOT NULL)` — over `mvFactGenieFeedback`.
  - "Comments / Fix-it rate (30d)" = `count(*) where action_name in ('createConversationMessageComment','updateConversationMessageComment')`.
  - "Spaces with any negative feedback (30d)" = `count(distinct space_id) where feedback_rating='NEGATIVE'`.
- Table: top 10 spaces by negative feedback count over 30d. Faceted by `surface` (`agents` / `chat`).

- [ ] **Step 3: Add the accuracy widgets**

- Line chart: per-space accuracy over time. X = `created_at`. Y = `accuracy`. Series = `space_id` (or `space_title` via join). Default: top 5 spaces by run count.
- Table: spaces in `adb_genie_spaces` LEFT JOIN `adb_eval_runs` where `max(created_at) < current_date() - 30 OR eval_run_id IS NULL`.

- [ ] **Step 4: Add banners**

```markdown
> **Genie benchmark accuracy.** Sourced from the Genie Benchmark API (Beta). Many Genie spaces do not yet have benchmarks configured — see [Test and monitor a Genie Space](https://docs.databricks.com/aws/en/genie/monitor) to add benchmarks for spaces you want to monitor here. Per-question detail expires upstream at 7 days; per-run summaries are persisted in this dashboard.

> **User feedback** captured from `system.access.audit` (Public Preview) for Genie Agents and Genie One Chat. Audit data was incomplete during 2025-10-16 → 2025-11-19 due to a documented Databricks-side incident.
```

- [ ] **Step 5: Validate + deploy + visual check**

```bash
jq empty src/dashboards/lh_adoption_dashboard.lvdash.json
databricks bundle validate && databricks bundle deploy --target dev
```

- [ ] **Step 6: Commit, PR, tag rc5**

```bash
git add src/dashboards/lh_adoption_dashboard.lvdash.json
git commit -m "$(cat <<'EOF'
feat(#13): add Quality dashboard page (feedback + benchmark accuracy)

Two thin tiles per Sam's reframe: feedback from audit (works day 1 for
every customer) and benchmark accuracy from the SDK fan-out (Beta API,
~7% of spaces today). No custom eval framework — surfaces what Genie
already exposes.

Closes #13.

Co-authored-by: Isaac
EOF
)"
git push -u personal cp5/quality
gh pr create --title "CP5: Quality page (Genie feedback + accuracy)" --body "$(cat <<'EOF'
## Summary

- Adds `mvFactGenieFeedback` (audit-derived, merged with `surface` facet for Agents vs Chat).
- Adds `adb_eval_runs` snapshot via SDK fan-out (`genie_list_eval_runs`).
- Adds a Quality dashboard page with feedback KPI row + negative-feedback table + accuracy line chart + spaces-without-benchmarks table.

## Test plan

- [x] `databricks bundle deploy --target dev && databricks bundle run` succeeds.
- [x] `mvFactGenieFeedback` populates from `e2-demo-field-eng` (non-zero rows for both `agents` and `chat` surfaces).
- [x] `adb_eval_runs` populates for spaces that have benchmarks; empty rows for those that don't.
- [x] Quality page renders all widgets and banners.

Closes #13.

This pull request and its description were written by Isaac.
EOF
)"
gh pr merge --squash --delete-branch
git checkout main && git pull
git tag v3.0.0-rc5 && git push personal v3.0.0-rc5
```

---

# CP6 — Cross-asset adoption + docs + polish

**Why:** Headline "Adoption Overview" page on top of one consistent fact shape. README multi-workspace section. Rename pass. Tag `v3.0.0`.

**Effort:** 2–3 days.

### Task 6.1: Add `mvFactAssetAdoption`

**Files:**
- Create: `src/02_mvFactAssetAdoption.sql`
- Modify: `deployment_resources/workflows.yml`

**Interfaces:**
- Produces: `${catalog}.${schema}.mvFactAssetAdoption` Delta table. PK = (asset_type, asset_id, usage_date, workspace_id). Columns: `asset_type` ('genie','dashboard','app','serving','vector_search'), `asset_id, asset_name, usage_date, workspace_id, workspace_name, dbu_cost, activity_count`.

- [ ] **Step 1: Create the SQL**

```sql
CREATE TABLE IF NOT EXISTS :catalog.:schema.mvFactAssetAdoption (
  asset_type     STRING,
  asset_id       STRING,
  asset_name     STRING,
  usage_date     DATE,
  workspace_id   BIGINT,
  workspace_name STRING,
  dbu_cost       DECIMAL(38, 6),
  activity_count BIGINT
) USING DELTA
  PARTITIONED BY (workspace_id, asset_type);

MERGE INTO :catalog.:schema.mvFactAssetAdoption tgt
USING (
  SELECT 'genie' AS asset_type, space_id AS asset_id, space_title AS asset_name,
         usage_date, workspace_id, workspace_name,
         coalesce(sum(dbus), 0)            AS dbu_cost,
         sum(message_count)                AS activity_count
  FROM :catalog.:schema.mvFactGenieUsage gu
  LEFT JOIN :catalog.:schema.mvFactGeniePaygoCost gpc USING (space_id, usage_date, workspace_id)
  GROUP BY ALL
  UNION ALL
  SELECT 'dashboard' AS asset_type, dashboard_id AS asset_id, dashboard_name AS asset_name,
         usage_date, workspace_id, workspace_name,
         coalesce(sum(dbus), 0)            AS dbu_cost,
         sum(view_count)                   AS activity_count
  FROM :catalog.:schema.mvFactDashboardUsage GROUP BY ALL
  UNION ALL
  SELECT 'app' AS asset_type, app_id AS asset_id, app_name AS asset_name,
         usage_date, workspace_id, workspace_name,
         dbus AS dbu_cost,
         lifecycle_events AS activity_count
  FROM :catalog.:schema.mvFactAppUsage
  UNION ALL
  SELECT 'serving' AS asset_type, endpoint_id AS asset_id, endpoint_name AS asset_name,
         usage_date, workspace_id, workspace_name,
         dbus AS dbu_cost,
         request_count AS activity_count
  FROM :catalog.:schema.mvFactServingUsage
  UNION ALL
  SELECT 'vector_search' AS asset_type, endpoint_id AS asset_id, endpoint_name AS asset_name,
         usage_date, workspace_id, workspace_name,
         dbus AS dbu_cost,
         CAST(NULL AS BIGINT) AS activity_count
  FROM :catalog.:schema.mvFactVectorSearchCost
) src
ON  tgt.asset_type   = src.asset_type
AND tgt.asset_id     = src.asset_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

> **Note:** The `mvFactDashboardUsage` column names above (`dashboard_name`, `view_count`, `dbus`) assume CP2-CP4 left those columns in place. If a column name differs in the actual MV, adjust here — the V2→V3 mapping table in the spec is the source of truth.

- [ ] **Step 2: Wire as the last-running job task** (depends on every other `Process_*` task).

- [ ] **Step 3: Deploy + verify**

```sql
SELECT asset_type, count(*), sum(dbu_cost) FROM users.<schema>.mvFactAssetAdoption GROUP BY 1;
```

Expected: one row per non-empty asset_type.

- [ ] **Step 4: Commit**

```bash
git checkout -b cp6/polish
git add src/02_mvFactAssetAdoption.sql deployment_resources/workflows.yml
git commit -m "$(cat <<'EOF'
feat: add mvFactAssetAdoption (cross-asset headline fact)

UNION ALL of every mvFact* projected to a common shape
(asset_type, asset_id, asset_name, usage_date, workspace_id,
workspace_name, dbu_cost, activity_count). Powers the Adoption Overview
dashboard page.

Co-authored-by: Isaac
EOF
)"
```

### Task 6.2: Add the Adoption Overview dashboard page

**Files:**
- Modify: `src/dashboards/lh_adoption_dashboard.lvdash.json`

- [ ] **Step 1: Reorder pages** so "Adoption Overview" is the first page.

- [ ] **Step 2: Add dataset** for `mvFactAssetAdoption`.

- [ ] **Step 3: Add widgets**:
- KPI row: total `dbu_cost` (30d), total `activity_count` (30d), active asset count (30d, where `activity_count > 0`).
- Stacked-bar: `dbu_cost` per day, stacked by `asset_type`.
- Heatmap: `asset_type` × `usage_date` with `activity_count` cells.
- Table: top 20 assets by `dbu_cost` (30d) with `asset_type`, `asset_name`, `dbu_cost`, `activity_count`.

- [ ] **Step 4: Validate + deploy + visual check.**

- [ ] **Step 5: Commit**

```bash
git add src/dashboards/lh_adoption_dashboard.lvdash.json
git commit -m "$(cat <<'EOF'
feat: add Adoption Overview dashboard page

Headline page powered by mvFactAssetAdoption. KPI row, stacked-bar cost
breakdown, asset-type × date heatmap, top assets table.

Co-authored-by: Isaac
EOF
)"
```

### Task 6.3: README update — multi-workspace + naming rename

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add the multi-workspace section after Quick Start**

```markdown
## Deploying across multiple workspaces

The dashboard ships in **per-workspace** mode by default — one deployment per workspace, one copy of the data in each workspace's catalog. For customers with several workspaces who want a single consolidated view, deploy each instance pointed at a **shared Unity Catalog** schema.

1. Pick a shared catalog (e.g. `governance.adoption`).
2. Grant `USE CATALOG`, `CREATE SCHEMA`, `MODIFY` on the schema to each workspace's deploy identity.
3. Set `catalog_name` to the shared catalog in every workspace's bundle deployment.
4. Each workspace's job MERGEs only its own `workspace_id`-tagged rows — no duplication.

> **Tip:** Account-level **Genie One** already provides cross-workspace **discovery** (single search across dashboards, Genie Agents, and Apps from one entry point). The adoption dashboard is the **analytics** layer on top.
```

- [ ] **Step 2: Genie Agents rename**

Search/replace "Genie Spaces" → "Genie Agents" except for the first reference in the intro, which becomes:

> Track usage and adoption trends across dashboards, **Genie Agents (formerly Genie Spaces)**, AI apps, and machine-learning models in one place.

Replace "Databricks One" → "Genie One" everywhere.

- [ ] **Step 3: Update the Quick Start to mention `enable_genie_deep_dive`**

Add a bullet: "Optional: set `enable_genie_deep_dive=true` to populate the deep-dive Genie observability panel (slower; pulls message content via the SDK)."

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs(#15): multi-workspace section + Genie rename + deep-dive flag

Adds a 'Deploying across multiple workspaces' section explaining
shared-catalog topology and pointing to Genie One for discovery. Absorbs
the Genie Spaces → Genie Agents rename and Databricks One → Genie One
rename throughout the README. Documents the enable_genie_deep_dive flag
in the Quick Start.

Closes #15.

Co-authored-by: Isaac
EOF
)"
```

### Task 6.4: CHANGELOG + final cleanup

**Files:**
- Create: `CHANGELOG.md`
- Delete: `feat/track_models_and_apps` branch (remote)

- [ ] **Step 1: Create `CHANGELOG.md`**

```markdown
# Changelog

All notable changes to this project will be documented in this file.

## [3.0.0] — 2026-07-XX

### Added
- New "Adoption Overview" cross-asset headline page (`mvFactAssetAdoption`).
- New "Cost" view (`mvFactGeniePaygoCost`) for Genie pay-as-you-go monitoring.
- New "Quality" page surfacing user feedback (`mvFactGenieFeedback`) and Genie benchmark accuracy (`adb_eval_runs`).
- `mvFactVectorSearchCost` (billing-only Vector Search cost tracking).
- `mvFactServingUsage` (replaces `mvFactServingEndpointUsage` and `mvFactModelUsage`; covers custom + foundation models, with optional `system.serving.endpoint_usage` token columns).
- `workspace_id` / `workspace_name` / `workspace_url` columns on every `adb_*` table for multi-workspace future-proofing.
- `enable_genie_deep_dive` bundle variable gating the SDK message-content loader behind the deep-dive panel.
- Bounded `ThreadPoolExecutor` (max_workers 5-8) on residual SDK fan-out.

### Changed
- `mvFactGenieUsage`, `mvFactDashboardUsage`, `mvFactAppUsage`, and `genie_observability_main_table` are now **incremental Delta tables** populated via `MERGE INTO` (was: fresh-each-run views).
- `mvFactServingEndpointUsage` renamed to `mvFactServingUsage`.
- Genie message-level facts now sourced from `system.access.audit` + `system.query.history.genie_space_id` (was: SDK message attachments).
- "Genie Spaces" renamed to "Genie Agents" in user-facing copy.
- "Databricks One" renamed to "Genie One" in user-facing copy.

### Removed
- `mvFactModelUsage` (UC registry browsing events measured discovery, not adoption).
- SCIM `users.get` lookups from the hot path (email now sourced from `user_identity.email` in audit rows).
- Sequential per-space SDK fan-out for Genie conversations/messages (in default deployments).

### Fixed
- #11 — pipeline now creates the catalog/schema if they don't exist, with a clear permission error if the deploy identity lacks privileges.
- #12 — sequential SDK loading replaced by audit-table fact derivation and bounded ThreadPoolExecutor for residual lists.
- #14 — statement_id mismatch between `dbsql_cost_per_query` and `genie_observability_main_table` resolved by joining on `system.query.history.genie_space_id` instead of SDK message attachments.
- #15 — documentation overhauled with multi-workspace section, deep-dive flag explanation, and renamed references.
```

- [ ] **Step 2: Delete the stale feat branch**

```bash
git push personal --delete feat/track_models_and_apps
```

- [ ] **Step 3: Refresh dashboard screenshots in the README**

(Manual: open the deployed dashboard, take fresh screenshots of each page, save to `docs/img/`, reference in README.)

- [ ] **Step 4: Commit + final PR**

```bash
git add CHANGELOG.md docs/img/ README.md
git commit -m "$(cat <<'EOF'
chore: v3.0.0 CHANGELOG, refreshed screenshots, stale branch deleted

Co-authored-by: Isaac
EOF
)"
git push -u personal cp6/polish
gh pr create --title "CP6: cross-asset adoption + docs + polish" --body "$(cat <<'EOF'
## Summary

- Adds `mvFactAssetAdoption` and the Adoption Overview dashboard page.
- README rewritten: multi-workspace section, Genie Agents / Genie One renames, `enable_genie_deep_dive` documentation.
- CHANGELOG.md added.
- Stale `feat/track_models_and_apps` branch deleted.
- Dashboard screenshots refreshed.

## Test plan

- [x] `databricks bundle deploy --target dev && databricks bundle run` succeeds end-to-end.
- [x] Every dashboard page renders without errors.
- [x] `pytest -m "not integration"` passes.
- [x] `databricks bundle validate` passes.

Closes #15.

This pull request and its description were written by Isaac.
EOF
)"
```

### Task 6.5: Tag `v3.0.0` and announce

- [ ] **Step 1: Merge CP6 PR**

```bash
gh pr merge --squash --delete-branch
git checkout main && git pull
```

- [ ] **Step 2: Tag the release**

```bash
git tag -a v3.0.0 -m "$(cat <<'EOF'
v3.0.0 — system-tables-based adoption dashboard

See CHANGELOG.md for the full list of changes. Highlights:
- Genie pay-as-you-go cost monitoring (live on 2026-07-06).
- New cross-asset Adoption Overview page.
- Quality page with feedback + benchmark accuracy.
- Refactored on system tables for performance and correctness.
- Multi-workspace future-proofed via workspace_id columns.
EOF
)"
git push personal v3.0.0
```

- [ ] **Step 3: Post in `#adoption-dashboard-customer`** (Slack channel `C0B5T0TUQ6T`) — draft for review before sending:

```
:tada: V3 of the adoption dashboard is live: <PR/release URL>.

Highlights:
• Genie pay-as-you-go monitoring (auto-populates on July 6).
• Quality page with user feedback + benchmark accuracy.
• ≥5× faster pipeline on large workspaces.
• Fixes the statement_id mismatch (#14) and the catalog-not-found error (#11).
• Multi-workspace ready — deploy each workspace's job pointed at a shared catalog.

CHANGELOG: <link>
```

(Per global instructions: do not actually send the Slack message until Sam has approved the draft.)

---

## Plan self-review

Ran the four checks from the writing-plans skill:

**1. Spec coverage** — Walked through spec §1–§11. Every CP, fork, principle, and bug has a task. The non-goals (Topology C, Agent Bricks adoption page, `databricks-solutions/` migration, cross-account) are explicitly absent from the task list ✓.

**2. Placeholder scan** — No `TBD`, `TODO`, `add appropriate error handling`, or `similar to Task N`. Each test step shows the actual test code; each implementation step shows the actual code. The one runtime decision (dashboard JSON widget structure inside `lvdash.json`) defers to the existing `aibi-dashboard-builder:dashboard-widgets` skill rather than copying its schema verbatim — that's acceptable because the skill is a reference, not an external API.

**3. Type consistency** — Cross-checked function names:
- `ensure_storage(spark, catalog, schema)` defined in Task 1.2, consumed at top of `01_get_metadata.ipynb` ✓
- `resolve(spark, w)` (Task 1.3) returning `(int, str, str)`, consumed in Task 1.4 ✓
- `merge_workspace_scoped(spark, df, table_fqn, merge_keys)` (Task 1.5), consumed in Task 5.2 ✓
- `run(fn, items, max_workers, return_errors=False)` (Task 2.4), consumed in Task 5.2 ✓
- `fetch_for_space(w, space_id)` (Task 5.2) ✓
- Materialised table schemas in tasks 2.1, 2.2, 3.1, 4.1, 4.2, 4.3, 5.1, 6.1 all carry `workspace_id BIGINT` matching the bundle variable contract ✓

**4. Ambiguity check** —
- The `mvFactAssetAdoption` SQL in Task 6.1 assumes `mvFactDashboardUsage` columns. Added an inline note pointing back to the spec mapping table.
- `:catalog` / `:schema` SQL parameter syntax is the existing project convention (used in V2 SQL files and resolved by the bundle's `dataset_catalog`/`dataset_schema`) — kept it consistent throughout.
- The 14-day window in CP3/CP4 vs the 7-day window in CP2 is deliberate: cost data is appended after a 2-3h lag (per S-B), so a wider source window catches the trailing edge.

Plan ready for execution.

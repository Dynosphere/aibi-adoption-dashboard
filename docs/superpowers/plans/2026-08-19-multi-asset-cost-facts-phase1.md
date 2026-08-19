# Multi-Asset Cost Facts — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Graft v3's three asset-cost facts (serving, vector search, apps) onto the medallion branch as stateless full-rebuild tables, with a salvaged test suite and orchestration, without regressing the existing dashboard.

**Architecture:** Each fact keeps v3's typed, commented schema and source logic but replaces v3's watermark/`MERGE` wrapper with `CREATE OR REPLACE TABLE (…) ; INSERT OVERWRITE … SELECT …` over a bounded rolling window (`:lookback_days`, default 365). No `dim_pipeline_watermarks`. Two dashboard datasets that bind to changed facts are repointed in the same phase (guardrail). New asset *visualisations* are deferred to gated Phase 2.

**Tech Stack:** Databricks SQL (system tables), Databricks Asset Bundles (DABs), pytest + parameterized SQL `assert_true` tests, Databricks CLI ≥ 0.292.0.

**Spec:** `docs/superpowers/specs/2026-08-19-multi-asset-cost-facts-design.md`

## Global Constraints

- **The dashboard is the product (spec §0).** No task is complete if it leaves the dashboard unable to run, show data, or look consistent. Any fact renamed/reshaped/removed must have its dashboard bindings repointed in the same phase.
- **Stateless only.** No `dim_pipeline_watermarks`, no `MERGE`, no watermark advance. Facts rebuild each run via `INSERT OVERWRITE` over `date_sub(current_date(), :lookback_days)`.
- **Bounded window variable:** `lookback_days`, default `365`.
- **Source of salvaged files:** the archive tag `archive/v3-development-3.0.0` (extract with `git show archive/v3-development-3.0.0:<path>`).
- **Branch:** all work on `feature/multi-asset-cost-facts` (already created off Krishnan's tip).
- **Profile:** pass `--profile <PROFILE>` on every CLI call; never auto-select (ask the user which profile).
- **Naming:** table/file names keep the `mvFact*` / `02_*.sql` conventions already in `src/`.

---

### Task 1: Test tooling + scaffolding

**Files:**
- Create: `pyproject.toml`, `requirements-dev.txt`, `tests/__init__.py`, `tests/conftest.py`, `tests/test_smoke.py`

**Interfaces:**
- Produces: pytest fixtures `repo_root: Path` and `in_workspace: bool` (from `tests/conftest.py`), consumed by later Python tests.

- [ ] **Step 1: Extract the scaffolding verbatim from the archive tag**

```bash
git show archive/v3-development-3.0.0:pyproject.toml        > pyproject.toml
git show archive/v3-development-3.0.0:requirements-dev.txt  > requirements-dev.txt
mkdir -p tests
git show archive/v3-development-3.0.0:tests/__init__.py     > tests/__init__.py
git show archive/v3-development-3.0.0:tests/conftest.py     > tests/conftest.py
git show archive/v3-development-3.0.0:tests/test_smoke.py   > tests/test_smoke.py
```

- [ ] **Step 2: Install dev deps and run the smoke test**

Run: `python3 -m pip install -r requirements-dev.txt && python3 -m pytest tests/test_smoke.py -v`
Expected: PASS — `test_repo_root_has_databricks_yml` (the repo has `databricks.yml` at root).

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml requirements-dev.txt tests/
git commit -m "test: add pytest scaffolding salvaged from v3 archive"
```

---

### Task 2: `lookback_days` bundle variable (TDD)

**Files:**
- Modify: `deployment_resources/variables.yml`
- Create: `tests/test_bundle_vars.py` (adapted — v3's version checks `enable_genie_deep_dive`, which is not on this branch; ours checks `lookback_days`)

**Interfaces:**
- Produces: bundle variable `lookback_days` (default `365`), consumed by all three fact SQL files as `:lookback_days`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bundle_vars.py
"""Test: required bundle variables exist with correct defaults."""
from pathlib import Path
import yaml


def _load_vars(repo_root: Path) -> dict:
    return yaml.safe_load(
        (repo_root / "deployment_resources" / "variables.yml").read_text()
    )["variables"]


def test_lookback_days_default_365(repo_root: Path) -> None:
    vars_ = _load_vars(repo_root)
    assert "lookback_days" in vars_, "lookback_days must be declared"
    assert int(vars_["lookback_days"]["default"]) == 365
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_bundle_vars.py -v`
Expected: FAIL with `AssertionError: lookback_days must be declared`.

- [ ] **Step 3: Add the variable**

Append to the `variables:` block in `deployment_resources/variables.yml`:

```yaml
  lookback_days:
    description: Rolling window (days) of system-table history each cost fact rebuilds every run.
    default: 365
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_bundle_vars.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add deployment_resources/variables.yml tests/test_bundle_vars.py
git commit -m "feat: add lookback_days bundle variable + test"
```

---

### Task 3: `mvFactVectorSearchCost` — stateless (net-new fact)

**Files:**
- Create: `src/02_mvFactVectorSearchCost.sql`
- Create: `tests/sql/test_mv_vector_search_cost.sql`

**Interfaces:**
- Produces: table `mvFactVectorSearchCost`, grain `(endpoint_id, usage_date, workspace_id, sku_name)`, columns `endpoint_id, endpoint_name, usage_date, workspace_id, workspace_name, sku_name, dbus, dollars`.

- [ ] **Step 1: Write the SQL assert test**

```sql
-- tests/sql/test_mv_vector_search_cost.sql — runs post-deploy against the warehouse.
-- Grain uniqueness: no duplicate (endpoint_id, usage_date, workspace_id, sku_name).
SELECT assert_true(count(*) = 0, 'mvFactVectorSearchCost grain must be unique') AS grain_ok
FROM (
  SELECT endpoint_id, usage_date, workspace_id, sku_name, count(*) c
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost')
  GROUP BY 1,2,3,4 HAVING count(*) > 1
);

-- No negative cost.
SELECT assert_true(count(*) = 0, 'dbus/dollars must be non-negative') AS nonneg_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost')
WHERE dbus < 0 OR dollars < 0;
```

- [ ] **Step 2: Create the stateless fact**

Write `src/02_mvFactVectorSearchCost.sql` exactly as below (this is v3's schema + source logic, watermark machinery removed, `CREATE OR REPLACE TABLE (…)` + `INSERT OVERWRITE` wrapper, bounded window):

```sql
-- mvFactVectorSearchCost — billing-only fact for Vector Search endpoint cost.
-- Stateless full rebuild each run over a bounded :lookback_days window.
-- Source: system.billing.usage where billing_origin_product = 'VECTOR_SEARCH'.
-- Grain: (endpoint_id, usage_date, workspace_id, sku_name). sku_name is in the grain
-- because a single endpoint can emit compute and storage SKUs on the same day.
-- Note: QPS/latency are not in system tables as of mid-2026; cost only.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost') (
  endpoint_id    STRING         COMMENT 'Vector Search endpoint identifier, from system.billing.usage.usage_metadata.endpoint_id.',
  endpoint_name  STRING         COMMENT 'Human-readable Vector Search endpoint name, from usage_metadata.endpoint_name.',
  usage_date     DATE           COMMENT 'Calendar date of the underlying usage record.',
  workspace_id   BIGINT         COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name STRING         COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  sku_name       STRING         COMMENT 'Billing SKU (e.g. VECTOR_SEARCH_STANDARD, VECTOR_SEARCH_STORAGE_OPTIMIZED).',
  dbus           DECIMAL(38, 6) COMMENT 'Sum of DBUs, from system.billing.usage where billing_origin_product = VECTOR_SEARCH.',
  dollars        DECIMAL(38, 6) COMMENT 'USD cost from dbus via the list_prices cloud + usage_start_time effective-rate join.'
) USING DELTA
  PARTITIONED BY (workspace_id)
  COMMENT 'Billing-only fact for Databricks Vector Search endpoint cost. Stateless full rebuild over :lookback_days. Grain: (endpoint_id, usage_date, workspace_id, sku_name).';

INSERT OVERWRITE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost')
SELECT
  bu.usage_metadata.endpoint_id                                    AS endpoint_id,
  bu.usage_metadata.endpoint_name                                  AS endpoint_name,
  bu.usage_date                                                    AS usage_date,
  cast(bu.workspace_id AS BIGINT)                                  AS workspace_id,
  w.workspace_name                                                 AS workspace_name,
  bu.sku_name                                                      AS sku_name,
  cast(sum(bu.usage_quantity) AS DECIMAL(38, 6))                   AS dbus,
  cast(sum(bu.usage_quantity * coalesce(p.pricing.effective_list.default, 0))
       AS DECIMAL(38, 6))                                          AS dollars
FROM system.billing.usage bu
LEFT JOIN system.access.workspaces_latest w
  ON w.workspace_id = cast(bu.workspace_id AS BIGINT)
LEFT JOIN system.billing.list_prices p
  ON  p.sku_name      = bu.sku_name
  AND p.cloud         = bu.cloud
  AND p.currency_code = 'USD'
  AND bu.usage_start_time >= p.price_start_time
  AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
WHERE bu.billing_origin_product = 'VECTOR_SEARCH'
  AND bu.usage_start_time >= date_sub(current_date(), :lookback_days)
GROUP BY ALL;
```

- [ ] **Step 3: Commit**

```bash
git add src/02_mvFactVectorSearchCost.sql tests/sql/test_mv_vector_search_cost.sql
git commit -m "feat: add stateless mvFactVectorSearchCost + SQL test"
```

(SQL assert tests execute in Task 9 after deploy; there is no local warehouse to run them against.)

---

### Task 4: `mvFactServingUsage` — stateless (unified; removes two old facts)

**Files:**
- Create: `src/02_mvFactServingUsage.sql`
- Delete: `src/02_mvFactModelUsage.sql`, `src/02_mvFactServingEndpointUsage.sql`
- Create: `tests/sql/test_mv_serving_usage.sql`

**Interfaces:**
- Produces: table `mvFactServingUsage`, grain `(endpoint_id, served_entity_id, usage_date, workspace_id)`, columns `endpoint_id, endpoint_name, served_entity_id, entity_type, entity_name, entity_version, usage_date, workspace_id, workspace_name, request_count, input_tokens, output_tokens, dbus, dollars`. (Consumed by Task 7's `uc_models` dataset repoint.)

- [ ] **Step 1: Write the SQL assert test**

```sql
-- tests/sql/test_mv_serving_usage.sql — runs post-deploy.
SELECT assert_true(count(*) = 0, 'mvFactServingUsage grain must be unique') AS grain_ok
FROM (
  SELECT endpoint_id, served_entity_id, usage_date, workspace_id, count(*) c
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
  GROUP BY 1,2,3,4 HAVING count(*) > 1
);
SELECT assert_true(count(*) = 0, 'token/cost columns must be non-negative') AS nonneg_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
WHERE request_count < 0 OR input_tokens < 0 OR output_tokens < 0 OR dbus < 0 OR dollars < 0;
```

- [ ] **Step 2: Extract v3's fact and apply the stateless recipe**

```bash
git show archive/v3-development-3.0.0:src/02_mvFactServingUsage.sql > src/02_mvFactServingUsage.sql
```

Then edit `src/02_mvFactServingUsage.sql` to apply the identical recipe used in Task 3:
1. Change `CREATE TABLE IF NOT EXISTS IDENTIFIER(...mvFactServingUsage...) ( … ) USING DELTA PARTITIONED BY (workspace_id, entity_type) COMMENT '…';` → change only the leading keyword to **`CREATE OR REPLACE TABLE`** and rewrite the trailing table `COMMENT` to end with `Stateless full rebuild over :lookback_days.` (keep every typed column + its COMMENT and the `PARTITIONED BY (workspace_id, entity_type)` verbatim).
2. Replace the whole `MERGE INTO …mvFactServingUsage… tgt USING ( WITH watermark AS (…) SELECT … ) src ON … WHEN MATCHED … WHEN NOT MATCHED …;` block with `INSERT OVERWRITE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')` followed by **the same inner `SELECT … FROM system.serving.served_entities … / system.serving.endpoint_usage … / system.billing.usage …` query**, with these two changes to that SELECT:
   - Delete the `LEFT JOIN watermark wm ON …` line.
   - Replace the watermark predicate pair `… > coalesce(wm.watermark_ts, TIMESTAMP '2024-01-01 00:00:00') AND … <= current_timestamp() - INTERVAL 15 MINUTES` with `… >= date_sub(current_date(), :lookback_days)` (apply to the billing `usage_start_time` filter; keep the `billing_origin_product = 'MODEL_SERVING'` filter).
3. Delete the entire trailing second `MERGE INTO …dim_pipeline_watermarks…` statement.

Verify no residual watermark references remain:

Run: `grep -nE 'watermark|dim_pipeline_watermarks|MERGE INTO' src/02_mvFactServingUsage.sql`
Expected: no output.

- [ ] **Step 3: Remove the two superseded facts**

```bash
git rm src/02_mvFactModelUsage.sql src/02_mvFactServingEndpointUsage.sql
```

- [ ] **Step 4: Commit**

```bash
git add src/02_mvFactServingUsage.sql tests/sql/test_mv_serving_usage.sql
git commit -m "feat: add stateless mvFactServingUsage; remove mvFactModelUsage + mvFactServingEndpointUsage"
```

---

### Task 5: `mvFactAppUsage` — stateless rebuild (replaces existing schema)

**Files:**
- Modify (overwrite): `src/02_mvFactAppUsage.sql`
- Create: `tests/sql/test_mv_app_usage.sql`

**Interfaces:**
- Produces: table `mvFactAppUsage`, grain `(app_id, usage_date, workspace_id)`, columns `app_id, app_name, usage_date, workspace_id, workspace_name, dbus, dollars, lifecycle_events, distinct_users`. (Consumed by Task 7's `apps_views` reconciliation.)

- [ ] **Step 1: Write the SQL assert test**

```sql
-- tests/sql/test_mv_app_usage.sql — runs post-deploy.
SELECT assert_true(count(*) = 0, 'mvFactAppUsage grain must be unique') AS grain_ok
FROM (
  SELECT app_id, usage_date, workspace_id, count(*) c
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage')
  GROUP BY 1,2,3 HAVING count(*) > 1
);
SELECT assert_true(count(*) = 0, 'app cost/counts must be non-negative') AS nonneg_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage')
WHERE dbus < 0 OR dollars < 0 OR lifecycle_events < 0 OR distinct_users < 0;
```

- [ ] **Step 2: Extract v3's rebuilt app fact and apply the stateless recipe**

```bash
git show archive/v3-development-3.0.0:src/02_mvFactAppUsage.sql > src/02_mvFactAppUsage.sql
```

Apply the same recipe as Task 4 to `src/02_mvFactAppUsage.sql`:
1. `CREATE TABLE IF NOT EXISTS …mvFactAppUsage… ( … ) USING DELTA PARTITIONED BY (workspace_id) COMMENT '…';` → **`CREATE OR REPLACE TABLE`** (keep columns/comments + partition), COMMENT ends `Stateless full rebuild over :lookback_days.` — `CREATE OR REPLACE` also absorbs the schema change from Krishnan's old audit-only app table (no housekeeping DROP needed).
2. Replace the `MERGE INTO …mvFactAppUsage… USING ( WITH watermark AS (…) … FULL OUTER JOIN … ) …;` with `INSERT OVERWRITE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage')` + the same inner query, with:
   - Delete the `LEFT JOIN watermark …` lines.
   - The app fact filters two sources; replace both watermark predicates: billing `usage_start_time > coalesce(wm…, …) AND <= current_timestamp() - INTERVAL 15 MINUTES` → `usage_start_time >= date_sub(current_date(), :lookback_days)`; audit `event_time > coalesce(wm…, …) AND <= …` → `event_time >= date_sub(current_date(), :lookback_days)`. Keep the `billing_origin_product = 'APPS'` and `service_name = 'apps'` filters.
3. Delete the trailing `MERGE INTO …dim_pipeline_watermarks…` statement.

Run: `grep -nE 'watermark|dim_pipeline_watermarks|MERGE INTO' src/02_mvFactAppUsage.sql`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add src/02_mvFactAppUsage.sql tests/sql/test_mv_app_usage.sql
git commit -m "feat: rebuild mvFactAppUsage as stateless billing+audit fact"
```

---

### Task 6: Orchestration — wire the DAB job

**Files:**
- Modify: `deployment_resources/workflows.yml`

**Interfaces:**
- Consumes: the three fact SQL files from Tasks 3–5.

- [ ] **Step 1: Update the job tasks**

In `deployment_resources/workflows.yml`, under `resources.jobs.adoption_dash_job.tasks`:
- Delete the `Process_Models` task (pointed at `../src/02_mvFactModelUsage.sql`) and the `Process_ServingEndpoints` task (pointed at `../src/02_mvFactServingEndpointUsage.sql`).
- Add these two tasks (same shape as siblings — depend on `Ingest_Metadata`, use `${var.my_warehouse_id}`):

```yaml
        - task_key: Process_ServingUsage
          depends_on:
            - task_key: Ingest_Metadata
          sql_task:
            file:
              path: ../src/02_mvFactServingUsage.sql
            warehouse_id: ${var.my_warehouse_id}
        - task_key: Process_VectorSearchCost
          depends_on:
            - task_key: Ingest_Metadata
          sql_task:
            file:
              path: ../src/02_mvFactVectorSearchCost.sql
            warehouse_id: ${var.my_warehouse_id}
```
- Leave the existing `Process_Apps` task unchanged (its path `../src/02_mvFactAppUsage.sql` now points at the rebuilt fact).

- [ ] **Step 2: Validate the bundle**

Run: `databricks bundle validate --profile <PROFILE>`
Expected: validation succeeds; no reference to `02_mvFactModelUsage.sql` or `02_mvFactServingEndpointUsage.sql` remains.

Run: `grep -nE 'mvFactModelUsage|mvFactServingEndpointUsage' deployment_resources/workflows.yml`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add deployment_resources/workflows.yml
git commit -m "chore: wire ServingUsage + VectorSearchCost job tasks; drop old serving/model tasks"
```

---

### Task 7: Dashboard safety — repoint affected datasets (guardrail §7a)

**Files:**
- Modify: `src/dashboards/lh_adoption_dashboard.lvdash.json`

**Interfaces:**
- Consumes: `mvFactServingUsage` (Task 4), `mvFactAppUsage` (Task 5).

- [ ] **Step 1: Repoint the `uc_models` dataset**

In `src/dashboards/lh_adoption_dashboard.lvdash.json`, find the dataset named `uc_models`. Its query is `select * from mvfactmodelusage;` (with a commented `-- select * from mvfactservingendpointusage`). Replace the query text with:

```sql
select * from mvFactServingUsage
```

- [ ] **Step 2: Reconcile any widgets bound to `uc_models` / `apps_views`**

Search the JSON for widgets whose `query`/dataset is `uc_models` or `apps_views`. For each field encoding, confirm the referenced column exists in the new schema:
- `uc_models` → `mvFactServingUsage` columns: `endpoint_id, endpoint_name, served_entity_id, entity_type, entity_name, entity_version, usage_date, workspace_id, workspace_name, request_count, input_tokens, output_tokens, dbus, dollars`. Remap any field that referenced old `mvFactModelUsage`/`mvFactServingEndpointUsage` columns to the nearest equivalent (e.g. a model-name field → `entity_name`; a cost field → `dbus`/`dollars`).
- `apps_views` → `mvFactAppUsage` columns: `app_id, app_name, usage_date, workspace_id, workspace_name, dbus, dollars, lifecycle_events, distinct_users`. Remap any field that referenced old audit-only columns (e.g. `num_deploys`, `num_gets`) to `lifecycle_events` / `distinct_users`.

- [ ] **Step 3: Validate the JSON parses**

Run: `python3 -c "import json; json.load(open('src/dashboards/lh_adoption_dashboard.lvdash.json')); print('valid json')"`
Expected: `valid json`.

Run: `grep -oiE 'mvfactmodelusage|mvfactservingendpointusage' src/dashboards/lh_adoption_dashboard.lvdash.json`
Expected: no output (no dataset still points at a removed table).

- [ ] **Step 4: Commit**

```bash
git add src/dashboards/lh_adoption_dashboard.lvdash.json
git commit -m "fix(dashboard): repoint uc_models to mvFactServingUsage; reconcile apps_views fields"
```

---

### Task 8: Validation tooling

**Files:**
- Create: `scripts/validate_system_tables.py`, `docs/v3-system-table-validation.md`, `docs/superpowers/spikes/spike-g-incrementalisation.md`

**Interfaces:**
- Consumes: nothing at build time; used manually in Task 9 to reconcile fact totals against raw `system.billing.usage`.

- [ ] **Step 1: Salvage the validation script and docs**

```bash
mkdir -p scripts docs/superpowers/spikes
git show archive/v3-development-3.0.0:scripts/validate_system_tables.py            > scripts/validate_system_tables.py
git show archive/v3-development-3.0.0:docs/v3-system-table-validation.md           > docs/v3-system-table-validation.md
git show archive/v3-development-3.0.0:docs/superpowers/spikes/spike-g-incrementalisation.md > docs/superpowers/spikes/spike-g-incrementalisation.md
```

- [ ] **Step 2: Confirm the script imports cleanly**

Run: `python3 -c "import ast; ast.parse(open('scripts/validate_system_tables.py').read()); print('parses')"`
Expected: `parses`.

- [ ] **Step 3: Commit**

```bash
git add scripts/validate_system_tables.py docs/v3-system-table-validation.md docs/superpowers/spikes/spike-g-incrementalisation.md
git commit -m "chore: salvage system-table validation script + docs from v3"
```

---

### Task 9: Deploy, validate, and dashboard render check — CHECKPOINT (hard gate)

**Files:** none (integration + human review).

**Interfaces:**
- Consumes: everything from Tasks 1–8.

- [ ] **Step 1: Deploy the bundle to a target workspace**

Run: `databricks bundle deploy -t <TARGET> --profile <PROFILE>`
Expected: deploy succeeds.

- [ ] **Step 2: Run the job**

Run: `databricks bundle run adoption_dash_job -t <TARGET> --profile <PROFILE>`
Expected: all tasks succeed, including `Process_ServingUsage`, `Process_VectorSearchCost`, `Process_Apps`.

- [ ] **Step 3: Run the SQL assert tests against the warehouse**

For each of `tests/sql/test_mv_vector_search_cost.sql`, `tests/sql/test_mv_serving_usage.sql`, `tests/sql/test_mv_app_usage.sql`, execute the file against the SQL warehouse with `catalog_name`/`schema_name` parameters bound (via `databricks experimental aitools tools query` or the SQL editor with the deployed values).
Expected: every `assert_true` returns true (grain unique, no negatives, tables non-empty where data exists).

- [ ] **Step 4: Reconcile totals**

Run `scripts/validate_system_tables.py` against the workspace and confirm each fact's `dbus`/`dollars` totals match raw `system.billing.usage` filtered to the matching `billing_origin_product` (`VECTOR_SEARCH`, `MODEL_SERVING`, `APPS`).
Expected: totals reconcile within rounding.

- [ ] **Step 5: Dashboard load-and-render check (spec §0 — release blocker)**

Open the deployed dashboard. Confirm: it loads with no dataset errors; every page's widgets display data (including the repointed `uc_models` and `apps_views` widgets); styling and layout are consistent with the other pages.
Expected: no broken widgets, no empty-because-erroring datasets, consistent look.

- [ ] **Step 6: STOP — present results for human review**

Post the job run link, SQL assert results, reconciliation numbers, and dashboard screenshots. **Do not begin Phase 2 (dashboard surfaces) until the human approves.** After approval, proceed to branch housekeeping: notify TshBerryBlue, then `git push origin --delete v3-development` (the `archive/v3-development-3.0.0` tag already preserves it).

---

## Post-plan self-review

- **Spec coverage:** §0 guardrail → Tasks 7 & 9 (render check as gate). §2 scope → Tasks 3–8. §3 salvage → Tasks 1, 8 (facts in 3–5). §4 recipe → Tasks 3–5. §5 file changes → Tasks 3–4 (adds/removes). §6 tests → Tasks 1–5 (curated: `test_delta_merge`/`test_eval_runs` intentionally not ported; `test_mv_genie_usage.sql` intentionally not ported — it targets `genie_observability_main_table`, which this branch does not have — replaced by three asset-fact SQL tests). §7 orchestration → Task 6. §7a dashboard safety → Task 7. §8 phases + checkpoint → Task 9. §9 risks (app widget in Phase 1; regression blocker) → Tasks 5, 7, 9.
- **Placeholder scan:** none — every SQL/YAML/JSON edit shows exact content or an exact extract+edit recipe against a real committed source.
- **Type consistency:** `mvFactServingUsage` column list is stated once (Task 4 Interfaces) and reused by Task 7; `mvFactAppUsage` column list stated in Task 5 and reused by Task 7; `lookback_days` name consistent across Tasks 2–5.

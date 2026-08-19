# Design — Graft v3 asset-cost facts onto the medallion branch (stateless)

**Date:** 2026-08-19 · **Author:** Sam Le Corre · **Status:** Design — awaiting review
**Branch:** `feature/multi-asset-cost-facts` (off `feature/genie-medallion-observability-cost`, "Krishnan's" branch)

---

## 0. Design guardrail — the dashboard is the product

The AI/BI dashboard is the key deliverable; fact tables exist to feed it. Every decision,
build step, and test must keep the dashboard **(1) running** — no broken datasets or failing
queries; **(2) displaying data** — every widget resolves its fields and returns rows; and
**(3) looking good and consistent** — layout, naming, and styling coherent across pages. A
change that lands a "correct" fact but regresses the dashboard is a failed change.

Two rules follow, and bind every phase below:
- **No fact is renamed, reshaped, or removed without repointing — in the same change — every
  dashboard dataset and widget that binds to it.** (See §7a for the Phase-1 obligations this
  creates.)
- **Every phase ends with a dashboard load-and-render check**, not only SQL/row validation:
  deploy, open the dashboard, confirm each affected page loads, widgets show data, and styling
  is consistent.

## 1. Context & decisions

Three lines of work diverged from the same `main` baseline (`22bf934`):

- **`main`** — planning baseline / current trunk.
- **`feature/genie-medallion-observability-cost`** ("Krishnan's", off `main`) — a validated,
  live-tested medallion re-architecture of the Genie slice. Correct billing attribution keys
  (`usage_metadata.genie.agent_id/surface/channel`), a robust grain fix (statement_id as
  `ARRAY<STRING>`, exploded 1:1 to `dbsql_cost_per_query`), and **stateless full-rebuild**
  tables (`CREATE OR REPLACE TABLE`). Genie-only; **no tests**.
- **`v3-development`** (off `main`, authored by Sam) — broader multi-asset coverage + a
  watermarked incremental layer + a pytest/SQL test suite. Its Genie cost model
  (`mvFactGeniePaygoCost`) is buggy (attributes on `endpoint_name`, NULL on 100% of GENIE rows).

**Key decision — build on Krishnan's branch, stateless.** v3's central differentiator was
incrementalisation. Verified against the **latest SDK (0.129.0)**: none of the five entity
list endpoints (`genie.list_spaces`, `genie.list_conversations`,
`genie.list_conversation_messages`, `lakeview.list`, `registered_models.list`,
`serving_endpoints.list`, `apps.list`) expose any time/`updated_after` filter — the crawl
**must** full-scan. Only `query_history.list` (`QueryFilter.query_start_time_range`) and the
system tables support incremental reads. v3's own `spike-g-incrementalisation.md` reached the
same conclusion and filed the heavy watermark table under "V4 backlog", recommending a simple
bounded date-window predicate instead. Krishnan's stateless full-rebuild is therefore the
correct, lower-risk posture (no mutable watermark state, no outage-skip failure mode). We keep
it and do **not** port v3's `dim_pipeline_watermarks` layer.

**The contributor `#17` fix is not applicable.** `eaf8757` patches
`genie_observability_main_table.sql` and `v3_housekeeping.sql`, neither of which exists on
Krishnan's branch (he deleted the table). Its grain intent is already solved — more robustly —
by Krishnan's ARRAY-explode model (no ±5-min fuzzy window join). The only potentially reusable
morsel is a dashboard-level `COUNT(*) → COUNT(DISTINCT message_id)` fix; evaluated in Phase 2,
not cherry-picked.

## 2. Scope

**In (Phase 1):** port v3's three asset-cost facts, converted to the stateless pattern;
salvage the test scaffolding + validation tooling; wire orchestration; **and, per the §0
guardrail, keep the existing dashboard un-regressed** — repoint the datasets that bind to the
changed facts and verify the dashboard still loads and displays data (§7a). New asset
*visualisations* remain Phase 2.

**Deferred (recoverable from the v3 archive tag):** `mvFactAssetAdoption` (cross-asset
rollup), `mvFactGenieFeedback`, eval-runs / Quality dashboard page.

**Phase 2 (planned, gated — see §8):** surface the asset facts in the dashboard.

**Out / discarded:** `dim_pipeline_watermarks`, `mvFactGeniePaygoCost`,
`genie_observability_main_table`, `v3_housekeeping.sql`, the `#17` fix.

## 3. v3 salvage inventory (gates the archive)

| Item | Disposition |
|---|---|
| `02_mvFactServingUsage.sql` | Salvage → convert to stateless |
| `02_mvFactVectorSearchCost.sql` | Salvage → convert to stateless |
| `02_mvFactAppUsage.sql` (v3 rebuilt) | Salvage → convert; replaces Krishnan's audit-only app fact |
| `conftest.py`, `test_smoke`, `test_bundle_vars`, `test_validation_script`, `sql/test_mv_genie_usage.sql` | Salvage → adapt |
| `scripts/validate_system_tables.py`, `docs/.../v3-system-table-validation.md` | Salvage as-is |
| `docs/.../spike-g-incrementalisation.md` | Salvage (rationale record) |
| `pyproject.toml`, `requirements-dev.txt` | Salvage (test tooling) |
| `mvFactAssetAdoption`, `mvFactGenieFeedback`, `adb_eval_runs`, `eval_runs.py`, v3 dashboard pages | **Defer** — recoverable from archive tag |
| `dim_pipeline_watermarks`, `mvFactGeniePaygoCost`, `genie_observability_main_table`, `v3_housekeeping.sql`, `#17` | **Discard** — history retained by tag |

## 4. Stateless conversion recipe (applied identically to all three facts)

All three v3 facts share one shape: typed `CREATE TABLE IF NOT EXISTS` → `MERGE` whose source
opens with `WITH watermark AS (SELECT … FROM dim_pipeline_watermarks)` + a `> watermark`
predicate → a trailing MERGE that advances the watermark.

**Keep verbatim:** the typed column list + per-column `COMMENT`s and the table `COMMENT`; the
core source logic — the `system.billing.usage` ↔ `system.billing.list_prices` effective-rate
join (serving, vector search, apps), the `served_entities` ↔ `endpoint_usage` join (serving),
and the billing ⟗ `system.access.audit` FULL OUTER JOIN (apps).

**Change the wrapper only:**
1. `CREATE TABLE IF NOT EXISTS … ; MERGE INTO … USING (…)` → `CREATE OR REPLACE TABLE … AS SELECT …`.
2. Remove the `WITH watermark` CTE and every `… > watermark` predicate; replace with a bounded
   window on the partition-pruned timestamp: `usage_start_time >= current_date() - INTERVAL :lookback_days DAY`
   (and `event_time >= …` for the audit half of the app fact).
3. Delete the trailing watermark-advance MERGE.
4. Drop the `dim_pipeline_watermarks` dependency entirely. `CREATE OR REPLACE` absorbs schema
   changes (e.g. the app-fact grain change) with no housekeeping `DROP`.

**New bundle variable** (`deployment_resources/variables.yml`):
```yaml
  lookback_days:
    description: Rolling window (days) of system-table history each fact rebuilds each run.
    default: 365
```

## 5. Net file changes on `feature/multi-asset-cost-facts`

**Add:** `src/02_mvFactServingUsage.sql`, `src/02_mvFactVectorSearchCost.sql` (stateless).
**Rewrite:** `src/02_mvFactAppUsage.sql` (v3's rebuilt + converted version).
**Remove:** `src/02_mvFactModelUsage.sql`, `src/02_mvFactServingEndpointUsage.sql`
(the unified `mvFactServingUsage` supersedes both).
**Add tooling/tests:** `pyproject.toml`, `requirements-dev.txt`, `scripts/validate_system_tables.py`,
`tests/` (curated — see §6), `docs/superpowers/*` salvaged docs.

## 6. Tests (curated, not copied)

- **Keep/adapt:** `conftest.py`, `test_smoke`, `test_bundle_vars`, `test_validation_script`,
  `sql/test_mv_genie_usage.sql`.
- **Add:** one SQL test per stateless fact (`sql/test_mv_serving_usage.sql`,
  `sql/test_mv_vector_search_cost.sql`, `sql/test_mv_app_usage.sql`) asserting grain uniqueness,
  no-null keys, and non-negative DBUs/dollars.
- **Drop:** `test_delta_merge` (no MERGE helper), `test_eval_runs` (eval deferred).
- **`src/includes/`:** Phase 1 brings **no** Python includes — the stateless SQL facts need
  none, and refactoring Krishnan's crawl notebook to use helpers is out of scope. The includes
  layer (`workspace_id.py`, `setup_storage.py`, `parallel.py`) is deferred with any future crawl
  refactor; `delta_merge.py` / `eval_runs.py` are discarded outright. (Narrows the earlier
  "bring the includes layer".)

## 7. Orchestration (`deployment_resources/workflows.yml`)

`adoption_dash_job` currently has parallel `Process_*` SQL tasks after `Ingest_Metadata`.
- Replace `Process_Models` + `Process_ServingEndpoints` with a single `Process_ServingUsage`
  → `02_mvFactServingUsage.sql`.
- Add `Process_VectorSearchCost` → `02_mvFactVectorSearchCost.sql`.
- `Process_Apps` keeps its path (`02_mvFactAppUsage.sql`), now the rebuilt fact.
All depend on `Ingest_Metadata`, `warehouse_id: ${var.my_warehouse_id}`.

## 7a. Dashboard safety (Phase 1, per §0 guardrail)

Removing/replacing facts must not break the current dashboard. Exactly two of its 31 datasets
bind to changed facts (verified against `lh_adoption_dashboard.lvdash.json`):

- **`uc_models`** — `select * from mvFactModelUsage`. That table is removed, so this dataset
  breaks unless repointed. **Repoint to `mvFactServingUsage`** and reconcile any bound widget's
  columns to the unified schema (`entity_type`, `request_count`, `input_tokens`, `dbus`,
  `dollars`, …).
- **`apps_views`** — `select * from mvFactAppUsage`. The `select *` survives the table swap,
  but the fact's schema changes (now `dbus`, `dollars`, `lifecycle_events`, `distinct_users`).
  **Reconcile any widget field bindings** that referenced the old audit-only columns.

`mvFactVectorSearchCost` has no existing dataset — its widgets are Phase 2. No other dataset
references the changed facts. After the repoint, run the §0 dashboard load-and-render check.

## 8. Phases & the human-in-the-loop checkpoint

1. **Phase 1 — this spec.** Salvage tooling/tests/docs → TDD-convert the three facts (SQL test
   first) → wire the DAB job → **repoint the affected dashboard datasets (§7a)** → deploy + run
   against a real workspace → validate row counts, grain, and cost totals via
   `validate_system_tables.py` → **dashboard load-and-render check (§0): open the dashboard,
   confirm every page loads, widgets show data, and styling is consistent.**
2. **CHECKPOINT (hard gate).** Stop. Present validation results **and the dashboard render
   check** for human review. Do not begin Phase 2 until approved.
3. **Phase 2 — dashboard (gated, separate plan).** Rebuild v3's "Models & Vector Search" page +
   an Apps section onto Krishnan's `lvdash.json` (structural rebuild, not a copy). Evaluate the
   `#17` `COUNT(DISTINCT message_id)` widget fix here.
4. **Archive v3** (after Phase 1 salvage confirmed complete):
   `git tag archive/v3-development-3.0.0 origin/v3-development && git push origin archive/v3-development-3.0.0`,
   then delete the remote branch. All deferred items remain reachable via the tag.

## 9. Risks & validation

- **`endpoint_usage` opt-in.** Token counts are zero unless per-endpoint usage tracking is
  enabled; DBU/USD cost from billing is unaffected. Document in the fact and dashboard.
- **Full-window cost.** `lookback_days=365` re-scans a year of `system.billing.usage` each run;
  bounded and partition-pruned on `usage_start_time`, so acceptable — but validate scan time on
  a large metastore and lower the default if needed.
- **App fact replacement.** Krishnan's audit-only `mvFactAppUsage` has a different schema;
  `CREATE OR REPLACE` handles the table swap, but the `apps_views` dataset's widgets must be
  reconciled to the new columns **in Phase 1** (§7a) — per the §0 guardrail, not deferred.
- **Dashboard regression is a release blocker.** Per §0, Phase 1 does not complete until the
  dashboard passes the load-and-render check; a green SQL suite with a broken dashboard fails.
- **Validation method.** Reuse v3's `spike`/`v3-system-table-validation` approach: reconcile
  fact totals against raw `system.billing.usage` per `billing_origin_product`.
```

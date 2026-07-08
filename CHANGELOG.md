# Changelog

All notable changes to this project will be documented in this file.

## [3.0.0] — 2026-07-08 (Unreleased)

### Added

- **Adoption Overview** dashboard page powered by the new `mvFactAssetAdoption` cross-asset headline fact. Aggregates daily active users and cost across Genie Agents, Genie One, Dashboards, Apps, and Serving in a single view.
- **Cost** dashboard page and `mvFactGeniePaygoCost` MV for Genie pay-as-you-go monitoring (warehouse DBU cost + LLM token cost). Auto-populates from the 2026-07-06 pay-as-you-go go-live date.
- **Quality** dashboard page with a user-feedback tile (`mvFactGenieFeedback`, sourced from audit thumbs-up/down events) and a Genie benchmark accuracy tile (`adb_eval_runs`, snapshotted via SDK fan-out from eval-run results).
- **Models and Vector Search** dashboard page with the new `mvFactServingUsage` (custom and foundation models) and `mvFactVectorSearchCost` (billing-only Vector Search cost tracking).
- `dim_pipeline_watermarks` table and a per-source LEFT-JOIN CTE watermark pattern wired into every audit-derived fact MV. Facts are now incremental Delta tables rather than fresh-each-run views.
- `workspace_id`, `workspace_name`, and `workspace_url` columns on every `adb_*` table for multi-workspace future-proofing.
- `workspace_id` resolver helper (`src/includes/workspace_id.py`) that reads from `system.access.workspaces_latest`.
- `enable_genie_deep_dive` bundle variable (default `false`) that gates the SDK conversation/message fan-out behind an optional deep-dive panel.
- `force_reinit` widget variable (default `false`) that gates the cell-5 drop-all-tables loop so accidental reruns cannot truncate production tables.
- Bounded `ThreadPoolExecutor` (`src/includes/parallel.py`, max_workers 5–8) on all residual SDK fan-outs: Lakeview schedules/subscriptions, model/serving/apps `get_permissions`.
- V3 housekeeping SQL (`src/02_v3_housekeeping.sql`) that drops V2 remnants of retired or renamed tables on the first run, ensuring `CREATE TABLE IF NOT EXISTS` in the new fact scripts creates the correct V3 schema.
- `system.access.workspaces_latest` used as the workspace dimension — no separate `dim_workspace` table needed.
- pytest harness and GitHub Actions CI workflow.
- System-table validation script and snapshot for pre-deploy schema verification.

### Changed

- **`mvFactGenieUsage`** rebuilt as an incremental Delta MERGE over `system.access.audit` (`service_name IN ('aibiGenie', 'genieChat')`). New grain: `(space_id, usage_date, workspace_id, user_email, surface)`. The `surface` column distinguishes Genie Agents (`agents`) from Genie One (`chat`). Previously a fresh-each-run view.
- **`mvFactServingEndpointUsage`** renamed to **`mvFactServingUsage`** and rebuilt on `system.serving.served_entities` + `system.serving.endpoint_usage` + `system.billing.usage`. Now covers both custom and foundation models with optional token-count columns.
- **`mvFactAppUsage`** rebuilt on `system.billing.usage.usage_metadata.app_id` + `system.access.audit` (`service_name='apps'`). Adds DBU/dollar cost columns and changes grain to `(app_id, usage_date, workspace_id)`. Previous V2 table dropped by housekeeping SQL on first V3 deploy.
- **`genie_observability_main_table`** rebuilt on `system.access.audit` + `system.query.history` (joined via `query_source.genie_space_id`) rather than unreliable SDK message attachments.
- `adb_*` writes changed from `df.write.mode("overwrite")` to `MERGE INTO` keyed on `(natural_key, workspace_id)` — safe for shared-catalog multi-workspace deployments.
- Pricing joins now include `cloud` and `usage_start_time` for correct effective-rate lookups.
- "Genie Spaces" renamed to "Genie Agents" in all user-facing copy (per DAIS 2026 announcement).
- "Databricks One" renamed to "Genie One" in all user-facing copy.
- Implicit retry disabled on the `Ingest_Metadata` job task to prevent double-writes on transient failures.

### Removed

- **`mvFactModelUsage`** — measured UC model-registry browsing events, not actual inference consumption. Real signal is now in `mvFactServingUsage`.
- SDK `w.users.get()` SCIM lookups from the hot path. User email is now sourced directly from `user_identity.email` in audit rows.
- Sequential per-conversation/per-message SDK fan-out in default deployments. Now gated behind the `enable_genie_deep_dive` variable.
- V2 `mvFactServingEndpointUsage` table (renamed; dropped by housekeeping SQL on first V3 deploy).

### Fixed

- **#11**: Pipeline now auto-creates the target catalog and schema (`ensure_storage`) with a clear, actionable permission error if the deploy identity lacks the required privileges.
- **#12**: Sequential SDK loading replaced by audit-table fact derivation and bounded `ThreadPoolExecutor` for residual SDK lists. `Ingest_Metadata` wall-clock reduced approximately 31 % on large workspaces versus the V2 baseline.
- **#14**: `statement_id` mismatch between `dbsql_cost_per_query` and `genie_observability_main_table` resolved by joining on `system.query.history.query_source.genie_space_id` instead of unreliable SDK message attachments.
- **#15**: Documentation overhauled — multi-workspace section, `enable_genie_deep_dive` and `force_reinit` flags documented, Genie Agents rename absorbed throughout.
- `SCALAR_SUBQUERY_TOO_MANY_ROWS` in watermark subqueries — all fact and watermark-advance MERGEs now use a LEFT JOIN CTE keyed on `(source_name, workspace_id)` instead of a correlated scalar subquery.
- `DELTA_MERGE_ADD_VOID_COLUMN` — `merge_workspace_scoped` now coerces all-NULL columns to string before the `CREATE TABLE IF NOT EXISTS` so the schema is not inferred as `VOID`.
- Notebook cell-14 `\"""..."""` escape bug introduced during the Task 2.4 refactor — fixed via a clean triple-quote round-trip.
- `distinct_users` column rename to `distinct_user_surfaces` with corrected semantics documented in the column comment.
- `genie_space` rows filtered on coalesced `space_id` (covers both `request_params.space_id` and `request_params.spaceId` key variants in the audit log).

### Known limitations and planned follow-ups

- `system.serving.endpoint_usage` requires per-endpoint opt-in; endpoints without usage tracking enabled show zero request and token counts.
- Genie space attribution in the `llm_paygo` half of `mvFactGeniePaygoCost` uses `usage_metadata.endpoint_name` — will need refinement once Databricks exposes a first-class `usage_metadata.genie.space_id` field.
- `dbu_cost = 0` for the `dashboard` asset type in `mvFactAssetAdoption` — `mvFactDashboardUsage` is audit-only with no billing cost column. A billing join is deferred to V3.1.
- Vector Search `activity_count` is NULL in mid-2026 system tables; no QPS or latency data available yet.

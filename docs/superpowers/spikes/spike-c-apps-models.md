# Spike S-C: Apps & Models data discovery

**Date:** 2026-06-15
**Owner:** Sam Le Corre
**Target release:** V3 / CP4 (before 2026-07-06)
**Audience:** public-facing repo (`Dynosphere/aibi-adoption-dashboard`)

## Summary

Three of the five asset types are mature enough to ship in CP4 using GA or stable Public Preview system tables: **Model Serving endpoints** (via `system.serving.served_entities` + optional `system.serving.endpoint_usage`), **Vector Search** (cost-only, via `system.billing.usage` with `billing_origin_product = 'VECTOR_SEARCH'`), and **Databricks Apps** (via `system.billing.usage` `usage_metadata.app_id`/`app_name` plus `system.access.audit` where `service_name = 'apps'`). **Foundation Models** are covered by the same Model Serving plumbing (`endpoint_usage` token counts) provided the customer has enabled usage tracking / AI Gateway. **Agent Bricks** (Knowledge Assistant, Multi-Agent Supervisor) has no dedicated system table yet — adoption telemetry is limited to audit-log events and the underlying serving endpoint(s) those agents deploy, so it should be deferred to V3.x or surfaced through MLflow Trace tables once those land in UC.

Net for CP4: replace the SDK-snapshot pattern in the stale branch with system-table-driven MVs for Apps and Serving; drop the standalone `mvFactModelUsage` (model-registry views are low-signal); add a Vector Search cost panel; mark Agents as "coming soon".

## Per-asset-type sources

| Asset | Best source (mid-2026) | Status | Key columns | Metrics to surface | Known gaps |
|---|---|---|---|---|---|
| **Databricks Apps** | `system.billing.usage` (cost) + `system.access.audit` where `service_name='apps'` (activity) | Both GA. A dedicated `system.apps.apps` dimension table is in progress (XTA-7362, security review noted "GA" for `name`, `id`, `service_principal_id`, etc., May 2026) but **not yet public**. | Billing: `usage_metadata.app_id`, `usage_metadata.app_name`, `usage_quantity`, `usage_date`, `sku_name`. Audit: `event_date`, `event_time`, `request_params.app_name`/`name`/`app_id`, `action_name` (`deployApplication`, `getApplication`, `updateApp`, `startApp`, `stopApp`), `user_identity.email`, `workspace_id`. | Active apps, deployments / updates per app, daily unique users, DBU cost per app, cost trend, cold-start frequency (start/stop events). | App-level request logs / HTTP error rate / latency live only in app-side logs (`docs/.../databricks-apps/monitor`), not system tables. Per-user inside-the-app activity requires structured app logging. The `apps.server_logs`/`request_logs`/`auth_logs` tables from XTA-7362 are still internal — don't reference them in a public dashboard. |
| **Model Serving endpoints** | `system.serving.served_entities` (dim) + `system.serving.endpoint_usage` (fact, optional) + `system.billing.usage` (cost) + `system.access.audit` (`service_name='serving'`) | `served_entities` and `endpoint_usage` are **Public Preview**, 365-day retention; usage tracking must be enabled per-endpoint (or AI Gateway configured). Billing fields GA. | `served_entities`: `served_entity_id`, `endpoint_id`, `endpoint_name`, `entity_type` (FOUNDATION_MODEL / EXTERNAL_MODEL / CUSTOM_MODEL), `entity_name`, `entity_version`, `change_time`, `endpoint_delete_time`. `endpoint_usage`: `request_time`, `served_entity_id`, `endpoint_id`, `request_count`, `input_token_count`, `output_token_count`, `token_details` struct (cache reads/creates, reasoning), `request_status`/error class. Billing: `usage_metadata.endpoint_id`/`endpoint_name`. | Endpoints in service, served-entity inventory by `entity_type`, request volume, total tokens (in/out), pay-per-token vs provisioned-throughput split, error-rate per endpoint, DBU cost per endpoint, top consumers. | `endpoint_usage` only populated when usage tracking is enabled — many customers will see zero rows. Cache-read/write tokens aren't broken out cleanly in aggregate token columns (BL-16624). No p50/p95 latency column in system tables yet — only via inference tables / Lakehouse Monitoring. Public Preview = schema can change. |
| **Foundation Models** | Same as Model Serving — filter `served_entities.entity_type='FOUNDATION_MODEL'`; pay-per-token endpoints attribute under `databricks-*` system endpoint names in `system.billing.usage` (SKU = `MODEL_SERVING`, `billing_origin_product='MODEL_SERVING'`). | Public Preview (same table). | As above + `entity_name` = e.g. `databricks-meta-llama-3-3-70b-instruct`, `databricks-claude-sonnet-4`. | Tokens consumed per model, DBU cost per FM, model mix, pay-per-token vs provisioned. | FMAPI cost attribution to *user* or app is partial — token counts are per-endpoint and don't carry caller identity unless AI Gateway is on. FMAPI calls from external apps may not show under the customer's `endpoint_usage`. |
| **Vector Search indexes** | `system.billing.usage` where `billing_origin_product='VECTOR_SEARCH'`, grouped by `usage_metadata.endpoint_name`/`endpoint_id`; index/ingest metadata via the Vector Search SDK (`w.vector_search_endpoints.list`, `w.vector_search_indexes.list_indexes`). | Billing GA. No dedicated VS usage/query system table exists in mid-2026 (Databricks Community thread #138454 + internal cost-observability docs). | Billing: `usage_date`, `sku_name` (e.g. `VECTOR_SEARCH_STORAGE_OPTIMIZED`), `usage_metadata.endpoint_id/name`, `usage_quantity`. SDK: `endpoint_name`, `endpoint_type` (STANDARD/STORAGE_OPTIMIZED), index `name`, `primary_key`, `index_type` (DELTA_SYNC / DIRECT_ACCESS), `pipeline_type`, last sync state. | Endpoints count, indexes count, daily DBUs by VS endpoint, ingest-vs-serving DBU split (`endpoint_name IS NULL` heuristic from go/money), storage-tier mix. | No QPS / latency / hit-rate in system tables — would need inference table on the calling serving endpoint. Index size / row count not in system tables (SDK only). |
| **AI Agents / Agent Bricks (KA, MAS, Genie-as-agent)** | No dedicated system table mid-2026. Indirect signal: (a) the serving endpoint hosting the deployed agent shows in `served_entities`/`endpoint_usage`; (b) `system.access.audit` carries some Agent Bricks lifecycle events (`createLabelingSession`, review-app events); (c) MLflow trace tables (`system.mlflow.run_metrics_history` Public Preview, OTel-traces-in-UC still preview/internal). Genie-as-agent remains under `system.access.audit` `service_name='aibiGenie'` — already covered by `mvFactGenieUsage`. | Agent Bricks KA + MAS GA on AWS/Azure/GCP as of Mar 2026, but observability is via MLflow + serving endpoint, not via an `system.agents.*` table. | Endpoint-level metrics from `endpoint_usage` (if agent deployed behind a serving endpoint). MLflow `run_metrics_history`: `run_id`, `experiment_id`, `metric_name`, `value`, `timestamp` (180-day retention). | No first-class "agent" dimension in system tables. KA/MAS internals (tool calls, retrieval hops, judge scores) only via MLflow traces — UC trace storage is preview and per-customer config. Do not ship a public agent-usage panel in CP4. |

## Consolidation recommendation

The current three-MV layout (`mvFactAppUsage`, `mvFactModelUsage`, `mvFactServingEndpointUsage`) is the wrong cut. For CP4:

1. **Drop `mvFactModelUsage`** — model-registry audit events measure browsing the registry, not consumption. The real signal lives in `system.serving.served_entities` joined to `endpoint_usage`/`billing`.
2. **One MV per asset, sourced from system tables, not SDK snapshots**. Rename to `mvFactServingUsage` (covers custom + foundation models) and `mvFactAppUsage`. Add `mvFactVectorSearchCost` (billing-only; flag that query metrics are unavailable).
3. **Remove the `adb_apps` / `adb_models` / `adb_serving_endpoints` SDK-snapshot dimension tables** the stale branch introduced. They drift, require workspace-by-workspace SDK iteration, and add no information that `system.serving.served_entities` + `system.billing.usage.usage_metadata.app_id` don't already give us. The one exception is `adb_apps` if/until `system.apps.apps` is public — but app names can be synthesised from `usage_metadata.app_name` in billing.
4. **Defer Agents/Agent Bricks** to V3.x. Add a placeholder panel with a "coming when `system.agents.*` or UC-stored MLflow traces are GA" note.
5. **Cost is the cross-cutting metric**. Unifying frame: "AI/BI assets x DBU cost x activity". All MVs expose `(asset_type, asset_id, asset_name, workspace_id, usage_date, dbu_cost, activity_count)` and let the dashboard pivot.

Redundancy check vs `mvFactGenieUsage`: no overlap. Genie is audit-only (`service_name='aibiGenie'`); model serving is a different surface. Keep separate fact MVs but share the same `(workspace_id, usage_date, asset_id)` shape so the dashboard can stitch a unified "AI asset adoption" page.

## Comparison to stale `feat/track_models_and_apps` branch (commit `cc3850b`)

Right: identified that Apps/Models/Serving Endpoints needed coverage, used the SDK to enumerate assets (a reasonable fallback when `system.apps.*` didn't exist), followed the existing `adb_*` snapshot pattern Genie + Dashboards use. Wrong: (a) relies entirely on `system.access.audit` text-matching on `action_name` (`like '%getApp%'`), which is brittle and misses real usage signal — the audit log doesn't capture in-app user requests, only control-plane lifecycle events; (b) duplicates effort the new `system.billing.usage.usage_metadata.app_id`/`endpoint_id` columns already solve; (c) creates an `mvFactModelUsage` over the UC registry that conflates "model exists" with "model is being served". CP4 should keep the intent but rebuild on system tables and pivot to cost+token metrics.

## Open questions

- **Live verification** — bash/CLI access was blocked in this spike, so I could not run `DESCRIBE EXTENDED system.serving.served_entities` against `e2-demo-field-eng` to confirm exact column names at our workspace's preview ring. Do this before opening the CP4 PR.
- **`system.serving.endpoint_usage` enablement story** — empty unless customer enables usage tracking per endpoint. Should the dashboard detect this and show a "turn on usage tracking" banner?
- **AI Gateway dependence** — token-level usage and cache-token breakouts require AI Gateway. For non-Gateway endpoints we'd only see request counts. Acceptable?
- **Public Preview risk** — `served_entities` + `endpoint_usage` schemas may change before V3 ships. Pin to current columns and add a schema-evolution test.
- **`system.apps.apps` ETA** — XTA-7362 dimension table for Apps. If GA before 2026-07-06 we should switch off the SDK snapshot. Worth a ping in #data-platform-system-tables.
- **MLflow trace UC tables for Agent Bricks** — when will `system.mlflow.traces` (or the OTel-in-UC equivalent) be public and GA? That's the unlock for an Agent Bricks adoption panel.
- **Vector Search query-rate** — accept "cost-only" coverage for V3, or invest in inference-table aggregation? Probably cost-only for CP4.

## References

- System tables reference: https://docs.databricks.com/aws/en/admin/system-tables/
- Configure AI Gateway / shared system tables: https://docs.databricks.com/aws/en/ai-gateway/configure-ai-gateway-endpoints
- Monitor usage for AI Gateway endpoints (column-level schema): https://docs.databricks.com/aws/en/ai-gateway/usage-tracking-beta
- Monitor model serving costs: https://docs.databricks.com/aws/en/admin/system-tables/model-serving-cost
- Logging & monitoring for Databricks Apps: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/monitor
- Billable usage system table reference (app_id, endpoint_id): https://docs.databricks.com/aws/en/admin/system-tables/billing
- Community: VS system tables (cost-only): https://community.databricks.com/t5/generative-ai/system-tables-for-vector-search-index/td-p/138454
- Internal: System Tables PoCs + Docs tracker (Confluence page 3559522342)
- Internal: Databricks Apps System Tables (JIRA XTA-7362)
- Stale branch: `git show feat/track_models_and_apps` (commit `cc3850b`)

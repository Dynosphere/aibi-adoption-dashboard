# Spike S-D — Data-model review (synthesis)

**Date:** 2026-06-15
**Author:** Brian (synthesised from S-A, S-B, S-C, S-E findings + V2 source review)
**Target:** V3 / before 2026-07-06
**Status:** Recommendation for V3 spec.

## Headline

The V2 model conflates two responsibilities in `01_get_metadata.ipynb`: **building dimensions** (entity catalogues) and **building facts** (usage / events). The four spikes converge on the same answer: **dimensions stay in the SDK, facts move to system tables.** This shrinks the metadata notebook to about a quarter of its size, dissolves issue #14, fixes issue #12, and gives us a single consistent join key (`workspace_id, asset_id, asset_type`) across every asset type — including the new ones (Apps, Serving, Vector Search, Genie paygo).

The standalone `genie_observability_main_table` is the biggest casualty: it should be re-derived from `system.access.audit (service_name='aibiGenie')` joined to `system.query.history (genie_space_id)`, removing both the SDK fan-out (#12) and the attachment-statement_id mismatch (#14) as a single side-effect.

## Object-by-object keep / refactor / drop

### Dimension tables (`adb_*`) — SDK-sourced

| Object | V2 source | V3 verdict | Why |
|---|---|---|---|
| `adb_genie_spaces` | `w.genie.list_spaces()` | **Keep, add `workspace_id`** | SDK is the only public source. Use the new `serialized_space` field (v0.117) for richer space metadata in one call. |
| `adb_genie_conversations` | `w.genie.list_conversations()` per space | **Drop** | Derive from `system.access.audit` (`genieCreateConversation`, `genieStartConversationMessage`). Eliminates the N-space SDK fan-out. |
| `adb_genie_messages` | `w.genie.list_conversation_messages()` per conv + `users.get()` per user | **Drop** | Derive from `system.access.audit` (~12 `aibiGenie` action_names) + `system.query.history.genie_space_id` for executed SQL / statement_id / cost link. SCIM `users.get` removed — use `user_identity.email` from audit. |
| `adb_dashboards` | `w.lakeview.list()` | **Keep, add `workspace_id`** | No system-table catalogue exists yet. |
| `adb_dashboard_schedules` | `w.lakeview.list_schedules()` per dashboard | **Keep, parallelise, add `workspace_id`** | N+1 stays SDK-only — but the ceiling is dashboard count, not message count. ThreadPoolExecutor handles it. |
| `adb_dashboard_subscriptions` | `w.lakeview.list_subscriptions()` per schedule | **Keep, parallelise, add `workspace_id`** | Same as schedules. |
| `adb_models` | `w.registered_models.list()` + `get_permissions()` per | **Drop** | UC registry events ≠ adoption signal. Replace usage with `system.serving.served_entities`. Keep an optional `adb_registered_models` thin catalogue only if a customer specifically wants registry coverage — out of V3 scope. |
| `adb_serving_endpoints` | `w.serving_endpoints.list()` + `get_permissions()` per | **Keep, add `workspace_id`. Filter `get_permissions()` calls to "active last 30d" via `system.access.table_lineage`** | Permissions API GET is 100/s — fine threaded, but most endpoints aren't worth the call. |
| `adb_apps` | `w.apps.list()` + `get_permissions()` per | **Keep, add `workspace_id`, accommodate new `space` field** | `system.apps.apps` dim table is in progress (XTA-7362) but not yet public — keep SDK for V3. Re-evaluate at V3.x. |

### Fact tables (`mvFact*`) — SQL-sourced

| Object | V2 source | V3 verdict | Why |
|---|---|---|---|
| `mvFactDashboardUsage` | `system.access.audit (service_name='dashboards')` joined to `adb_dashboards` | **Keep, modernise** | Add `system.query.history.dashboard_id` for query-level cost. Make the `workspace_id` join explicit (no-op for Topology A, ready for B). |
| `mvFactGenieUsage` | `system.access.audit (service_name='aibiGenie')` joined to `adb_genie_spaces` | **Keep, modernise** | Audit alone is enough — no longer joins to `adb_genie_conversations`/`adb_genie_messages` (those tables go away). |
| `mvFactAppUsage` | audit `service_name='apps'` + `adb_apps` | **Keep, modernise** | Add `system.billing.usage.usage_metadata.app_id` for DBU cost. Audit alone undercounts real usage. |
| `mvFactModelUsage` | UC registry audit events + `adb_models` | **Drop** | Low-signal — measures registry browsing, not consumption. Real signal is in serving. |
| `mvFactServingEndpointUsage` | audit + `adb_serving_endpoints` | **Rename → `mvFactServingUsage`** | Source: `system.serving.served_entities` + (optional) `system.serving.endpoint_usage` + `system.billing.usage` + audit. Covers custom + foundation models in one fact. |
| `mvFactGeniePaygoCost` | — | **Add** | UNION ALL of (a) warehouse DBUs from `dbsql_cost_per_query WHERE query_source_type='GENIE SPACE'` and (b) LLM DBUs from `system.billing.usage WHERE billing_origin_product IN ('GENIE','AI/BI_GENIE')`. Pre-July-6 the LLM half is empty; post-July-6 it populates automatically. |
| `mvFactVectorSearchCost` | — | **Add** | Billing-only, `billing_origin_product='VECTOR_SEARCH'`. No query-rate metrics until inference-table aggregation is in scope. |
| `mvFactAssetAdoption` (optional) | UNION ALL of all `mvFact*` projected to `(asset_type, asset_id, asset_name, workspace_id, usage_date, dbu_cost, activity_count)` | **Add — see Fork 2** | Powers the "cross-asset adoption" headline page. Whether to ship it or let the dashboard union per-asset MVs is a design choice — see Architectural Forks below. |

### Standalone tables

| Object | V2 source | V3 verdict | Why |
|---|---|---|---|
| `dbsql_cost_per_query` | Big SQL view on `system.query.history` + `system.billing.usage` + `system.compute.warehouse_events` + `list_prices` | **Keep** | Already best-of-breed. Reuse directly in `mvFactGeniePaygoCost`. |
| `genie_observability_main_table` | Built from `adb_genie_messages` (SDK), joins attachments `statement_id` | **Drop and re-derive** | This is the root cause of #14: SDK message attachments sometimes lose `statement_id`. Re-derive as a view over `system.access.audit (service_name='aibiGenie')` joined to `system.query.history.genie_space_id`. The audit log captures every message lifecycle event; query.history captures every executed statement. Joining them at-source removes the mismatch. |

### Dimensions we don't need to build

- `dim_workspace` — `system.access.workspaces_latest` (account-scoped, already joined in every `mvFact*`).
- `dim_user` — derive from `user_identity.email` in audit / billing on demand.
- `dim_date` — none needed; date_trunc on usage timestamps is cheap and consistent.

## Architectural forks worth user input

### Fork 1 — Statement_id fix approach

| Option | Description | Cost | Risk |
|---|---|---|---|
| **1a. Patch** | Improve attachment-handling in current SDK-built `genie_observability_main_table`; keep V2 architecture. | 1-2 days | Doesn't fix #12 (sequential SDK loading). Doesn't fix the documented Genie API gap (`GenieListConversations` excludes benchmark conversations). |
| **1b. Refactor** | Drop `adb_genie_conversations`/`messages` + `genie_observability_main_table`; re-derive from `system.access.audit` + `system.query.history`. | 3-4 days | Bigger change; testing surface includes the existing dashboard widgets that read `genie_observability_main_table`. **But fixes #12 and #14 as one piece of work and removes 600+ lines from the metadata notebook.** |

**Brian's recommendation: 1b.** This is the architectural decision that unlocks everything else; patching 1a means CP2 perf and CP1 bug-fix remain coupled to the SDK fan-out forever.

### Fork 2 — Cross-asset adoption fact

| Option | Description | Cost | Tradeoff |
|---|---|---|---|
| **2a. Build `mvFactAssetAdoption`** | New MV that UNION ALLs every per-asset fact projected to a common shape. Dashboard reads one source for the headline page. | 0.5 day | Extra MV in the workflow; needs to be kept in sync as new asset types arrive. |
| **2b. Dashboard-side union** | Per-asset MVs only. The Lakeview dashboard does the union via dataset queries. | 0 day | Dashboard tile SQL gets longer; harder to keep consistent if the dashboard JSON drifts. |

**Brian's recommendation: 2a.** One source of truth makes the "Adoption overview" page cleaner and means we can drop the per-asset MVs from a future Genie space without re-writing tiles.

### Fork 3 — `workspace_id` rollout

| Option | Description | Cost | Tradeoff |
|---|---|---|---|
| **3a. Universal `workspace_id`** | Add the column to every `adb_*` and `mvFact*` in V3, even though Topology A is the only shipping topology. | 0.5 day | Pays a small tax now; eliminates a breaking change at V3.x when a customer moves to shared catalog. |
| **3b. Defer** | Ship V3 without `workspace_id`; add later when needed. | 0 day | Cheaper now; guarantees a breaking change for any Topology-B customer in V3.x. |

**Brian's recommendation: 3a.** S-E is explicit: this is the lowest-friction future-proofing we'll ever get.

### Fork 4 — Apps catalogue source

| Option | Description | Cost | Tradeoff |
|---|---|---|---|
| **4a. Keep `adb_apps` (SDK)** | Status quo + add `workspace_id`. | minimal | SDK call required per workspace. |
| **4b. Drop `adb_apps`, derive from `system.billing.usage.usage_metadata.app_id/app_name`** | No SDK call for apps. | -0.5 day | Loses the "active app with zero cost yet" case (apps that exist but haven't run); permissions go away too. |

**Brian's recommendation: 4a.** The cost is trivial and we want app names for inventories where billing is zero. Re-evaluate when `system.apps.apps` is public.

### Fork 5 — AI Gateway / endpoint_usage tile policy

| Option | Description | Tradeoff |
|---|---|---|
| **5a. Soft-prerequisite** | Build tiles that depend on `system.serving.endpoint_usage` or `system.ai_gateway.usage`; show a "configure AI Gateway / enable usage tracking" banner when empty. | More work; better UX for unconfigured customers. |
| **5b. Hard-prerequisite** | Require AI Gateway + usage tracking; document as a precondition. | Less code; some customers will see empty tiles with no explanation. |

**Brian's recommendation: 5a.** Public-facing solution; empty-with-banner is the professional default.

## Bug roots: where #11 and #14 actually live

- **#11 (catalog/schema missing).** Pure usability issue in `01_get_metadata.ipynb`. Add a `CREATE CATALOG IF NOT EXISTS` / `CREATE SCHEMA IF NOT EXISTS` block with a clear error if the deploy identity lacks `CREATE CATALOG` permission. Independent of the model refactor.
- **#14 (statement_id mismatch).** Not a SQL bug — a **modelling bug**. `genie_observability_main_table` joins to message attachments that the SDK does not always surface. Fork 1b dissolves it.

## Updated CP1–CP6 implications

Based on this synthesis, the original phase boundaries shift slightly:

| Phase | What it absorbs | Notes |
|---|---|---|
| **CP1 — Stabilise + future-proof** | #11 (try-create catalog/schema) + `workspace_id` columns on every `adb_*` + switch `df.write.mode("overwrite")` → `MERGE` keyed on `(<entity_id>, workspace_id)` | 2-3 days. Foundation for everything else. |
| **CP2 — Genie observability refactor + perf** | Fork 1b. Drops `adb_genie_conversations`/`messages` + `genie_observability_main_table`. Re-derives from audit + query.history. Adds bounded `ThreadPoolExecutor` to residual SDK calls. **Resolves #12 and #14 together.** | 4-6 days. Biggest single piece of work. |
| **CP3 — Genie paygo** | New `mvFactGeniePaygoCost`. New dashboard widget. Optional `system.ai_gateway.usage` tile gated on AI Gateway enablement. | 2 days. Must land before July 6. |
| **CP4 — Apps & Models modernisation** | Drop `mvFactModelUsage`. Rename `mvFactServingEndpointUsage` → `mvFactServingUsage`. Rebuild on system tables. Add `mvFactVectorSearchCost`. Mark Agent Bricks "coming soon". | 4-5 days. |
| **CP5 — Genie accuracy tracking** | Issue #13. Source: `system.access.audit (service_name='aibiGenie' AND action_name LIKE '%feedback%')`. Optional Genie eval-run API (`genie_create_eval_run`/`genie_list_eval_runs`) for benchmark-driven accuracy. | 4-6 days. **Highest-uncertainty item — first candidate to slip to V3.1 if July 6 is tight.** |
| **CP6 — Cross-asset adoption + docs + polish** | Build `mvFactAssetAdoption` (Fork 2a). Multi-workspace section in README. Try-create error messages. Refresh screenshots. | 2-3 days. |

**Total estimated effort:** 18-25 days against a 21-day budget. CP5 is the natural slip candidate.

## Open synthesis-level questions for Sam

1. **Approve forks 1b, 2a, 3a, 4a, 5a as the V3 architectural baseline?** If yes, I write the spec on that basis. If any fork should flip, say which and why.
2. **Should we run a live `DESCRIBE EXTENDED` validation pass against `e2-demo-field-eng`** for every system table referenced (S-A and S-C both flagged this as a pre-merge check) — and if yes, treat it as part of CP1 or as a separate prep task before any code is touched?
3. **`mvFactAssetAdoption` shape** — is `(asset_type, asset_id, asset_name, workspace_id, usage_date, dbu_cost, activity_count)` the right 7-column projection, or do you want extra dimensions (e.g. `owner_email`, `is_active_30d`)?
4. **CP5 accuracy tracking — slip-or-ship?** Worth deciding now whether V3.0 ships with CP5 or whether we commit to "V3.0 by July 6 without accuracy; V3.1 in late July with accuracy". My read: pre-commit to the V3.1 slip — it removes pressure from the critical path and gives accuracy tracking the design attention it actually needs.

## References

- `docs/superpowers/spikes/spike-a-system-tables-sdk.md`
- `docs/superpowers/spikes/spike-b-genie-paygo.md`
- `docs/superpowers/spikes/spike-c-apps-models.md`
- `docs/superpowers/spikes/spike-e-multi-workspace.md`

# Spike A — System tables × SDK audit

**Date:** 2026-06-15 · **Author:** Sam Le Corre · **Status:** Findings only — read-only, no source changes · **Target:** V3 (before 2026-07-06 Genie PAYG)

## Summary

- **Keep the SDK for what only the SDK does well (entity catalogues + permissions ACLs); replace per-entity-per-event polling with system tables.** Most usage signal already lives in `system.access.audit`, `system.query.history`, and `system.serving.*`. `01_get_metadata.ipynb` should shrink to ~5 thin "dimension" pulls and stop fetching conversations/messages entirely.
- **Drop `list_conversations` + `list_conversation_messages`.** Genie message-level observability is covered by `system.access.audit` filtered on `service_name='aibiGenie'` — this is the AI/BI team's official guidance ("Monitor Genie Spaces usage with audit logs and alerts", Public Preview, May 2026). Eliminating these two N+1 loops resolves issue #12.
- **For the residual SDK calls, run per-resource lists in a bounded `ThreadPoolExecutor` (5–8 workers) and remove the SCIM `users.get` user→email lookup** (replace with `user_identity.email` from audit rows). That takes the hot-path SCIM traffic to zero — the SCIM workspace GET cap is only ~4/s.

## SDK call → system-table mapping

| Resource | Current SDK call | System-table replacement | Notes / caveats |
|---|---|---|---|
| Genie spaces (catalogue) | `w.genie.list_spaces(...)` | **Keep SDK** — no system table directories Genie spaces. UC lineage (Jan 2026 GA) exposes them as edges only. | SDK is the only public source of `space_id → title → warehouse_id`. Genie Space mgmt APIs GA March 2026. |
| Genie conversations | `w.genie.list_conversations(space_id,...)` per space | **Drop.** Derive from `system.access.audit` where `service_name='aibiGenie' AND action_name IN ('genieCreateConversation','genieStartConversationMessage')`. `conversation_id` is in `request_params`/`response`. | Eliminates the N-space × M-page fan-out behind issue #12. Also avoids the known gap (ES-1688908) that `GenieListConversations` excludes conversations created by benchmarks. |
| Genie messages | `w.genie.list_conversation_messages(...)` + `users.get` per user_id | **Drop.** `system.access.audit` (`service_name='aibiGenie'`, ~12 action_names incl. message create / feedback / comment / cancel). For SQL produced by Genie, join `system.query.history` on `genie_space_id` (column already exists). For Genie One Chat, May 2026 added 9 new `genieChat` actions (`mcpToolInvocation`, `steerGenieChatConversation`, `cancelGenieChatConversation`, etc.). | **Caveat:** audit captures metadata, not full attachment payloads. If `attachments[].query.statement_id` linkage is load-bearing for V3, use `system.query.history.genie_space_id` instead — it has executed SQL, statement_id, warehouse_id, exec time, bytes scanned. **Also:** documented outage 2025-10-16 → 2025-11-19 dropped some dashboard/Genie audit rows. |
| Lakeview dashboards (catalogue) | `w.lakeview.list(page_size=100)` | **Keep SDK** — no `system.lakeflow.dashboards`-style table exists. | SDK now exposes `dataset_catalog`/`dataset_schema` on Create/UpdateDashboardRequest (Apr 2026) — already used by the bundle. |
| Dashboard schedules | `w.lakeview.list_schedules(...)` per dashboard | **Keep SDK** (no system-table coverage). Run concurrently. | N+1 shape, but ceiling is dashboard count, not messages → usually 10–100× smaller blast radius. |
| Dashboard subscriptions | `w.lakeview.list_subscriptions(...)` per schedule | **Keep SDK**. | New `skip_notify` field added 2026. |
| Dashboard *usage* | not in notebook — in `02_mvFactDashboardUsage.sql` | Already on `system.access.audit` (`service_name='dashboards'`, action_names `getDashboard`, `getPublishedDashboard`, `sendDashboardSnapshot`, `triggerDashboardSnapshot`). Add `system.query.history.dashboard_id` for query-level cost. | Mar 2026: `triggerDashboardSnapshot` cleaned of thumbnail/probe noise; `sendDashboardSnapshot` added for Slack/Teams subs. |
| Registered models (catalogue) | `w.registered_models.list()` + `get_permissions` per model | **Hybrid.** Catalogue: `system.information_schema.tables WHERE table_type='MODEL'` (single query) or keep SDK. Permissions: keep SDK but filter to "active in last N days" via `system.access.table_lineage` before calling `get_permissions`. | Calling `get_permissions` for every model is the biggest hidden cost on large workspaces. Permissions API limit: 100 GET/s/workspace. |
| Model usage events | `02_mvFactModelUsage.sql` | Keep `system.access.audit` + add `system.serving.served_entities` (endpoint→model dim) + `system.access.table_lineage`. | Already largely aligned. |
| Serving endpoints (catalogue) | `w.serving_endpoints.list()` + `get_permissions` per | **Keep SDK** for catalogue. Add `system.serving.served_entities` for current served models. | Same permissions anti-pattern as models. |
| Serving endpoint *usage* | `02_mvFactServingEndpointUsage.sql` | **`system.serving.endpoint_usage`** (Public Preview, 90-day retention) — token counts per request. Currently the view only uses audit logs; adding this gives token volume + provisioned-throughput visibility. | **Caveat:** requires "enable usage tracking" per endpoint (opt-in). Document this in the dashboard's "if a tile is empty" panel. |
| Apps (catalogue) | `w.apps.list()` + `get_permissions` per | **Keep SDK**. | New `space` field (Spaces inside Apps) added May/Jun 2026 — V3 schema should accommodate. |
| User ID → email | `w.users.get(user_id)` per unique user (cached) | **Drop.** Read `user_identity.email` from audit rows; SCIM only as a fallback for IDs that never appear. | Largest SCIM risk: workspace SCIM GET is **255/min ≈ 4.25/s**. The current code can stall for minutes on workspaces with many distinct Genie users. |
| DBSQL warehouse cost/query | already SQL-only (`system.compute.warehouse_events`, `system.billing.usage`, `system.billing.list_prices`) | unchanged | Already best-of-breed. |

## SDK modernisation findings (databricks-sdk-py v0.72 → v0.117, Nov 2025 → Jun 2026)

- **No async/await rewrite shipped.** No asyncio client, no httpx migration. Pagination is still lazy iterators on top of `requests`. Concurrency must come from `concurrent.futures` (threads — the workload is I/O-bound).
- **Genie surface grew:** `create_message_comment`, `list_conversation_comments`, `list_message_comments`, `genie_create_eval_run` / `genie_get_eval_run` / `genie_list_eval_runs` (Genie evaluation API), `create_space` / `update_space` GA. New `serialized_space` field on `GenieGetSpaceRequest` — returns full space definition in one call (useful if V3 wants to snapshot space contents).
- **Dashboards:** `dataset_catalog` / `dataset_schema` on Create/UpdateDashboardRequest (Apr 2026, already used by V2); `skip_notify` on `Subscription`.
- **Auth & resilience:** `WorkspaceClient.get_workspace_id()` no longer round-trips SCIM `/Me` when ID is known (v0.117.0). Async-token-refresh failures now retry with 1-min backoff instead of disabling renewal. `custom_headers` parameter on `WorkspaceClient` / `AccountClient` for User-Agent correlation IDs.
- **No bulk-list endpoints** added for Genie/Lakeview/Apps/Models. Pagination is still per-parent.
- **Breaking change to watch:** `id` and `user_id` on `GenieConversation` are no longer required; `id` on `GenieMessage` no longer required. Current notebook's defensive `getattr(..., None)` already handles this — revalidate on upgrade.

## Parallelisation recommendation

**Pattern:** bounded `ThreadPoolExecutor`, one work-queue per resource type, SDK iterator inside each task.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # thread-safe

def fetch_schedules(dashboard_id: str):
    return [s.as_dict() | {"dashboard_id": dashboard_id}
            for s in w.lakeview.list_schedules(dashboard_id=dashboard_id)]

with ThreadPoolExecutor(max_workers=8) as ex:
    futures = {ex.submit(fetch_schedules, d): d for d in dashboard_ids}
    schedules = [row for f in as_completed(futures) for row in f.result()]
```

**Sizing & limits (public):**

- `WorkspaceClient` is thread-safe and shares a connection pool. Recommended `max_workers` is **5–8** per resource type, **not** per item.
- SCIM (workspace) GET: **255/min ≈ 4/s** → eliminate the lookup entirely, not thread it.
- Permissions API GET: **100/s** → safe to thread aggressively, but still filter to active resources first.
- Genie Spaces API: **5 questions/min/workspace (free tier), 20/min UI** — this is the *question* limit, not list/get. List endpoints are governed by the generic workspace MSP-pod concurrency (default 64 concurrent in-flight; raisable via account team).
- Query History API list: **10/s/account.**
- **Backoff on 429:** SDK ships a `rate_limited` enum on compute/SQL termination reasons but the HTTP layer still raises generic 429s. Use the SDK's built-in retry with `max_attempts=5, multiplier=2, max_wait=60` or `tenacity`.
- **Do not use multiprocessing** — I/O-bound workload, pickling overhead, can't share the connection pool across processes.

**Expected impact** for a 5k-Genie-space / 50k-conversation / 500k-message workspace: ~6h sequential and frequent 429s today → ~5–10 min in V3, with zero SCIM `users.get` calls, one `list_spaces`, one `MERGE` from `system.access.audit`, and concurrent per-resource-type fetches for the remaining catalogues.

## Open questions

1. **`system.access.audit` retention (365d Public Preview).** If V3 needs YoY views, add a historisation pipeline. Confirm requirement.
2. **Genie One (`genieChat`) vs Genie Spaces (`aibiGenie`) audit split.** Decide whether V3 merges these views or surfaces them separately.
3. **`system.serving.endpoint_usage` opt-in.** Token logging only fires when usage tracking is enabled on an endpoint. Need to confirm default for our reference customer setups or the token panel will be empty.
4. **Workspace MSP-pod concurrency (default 64).** Not in public docs; confirm with SDK team before recommending tuning in customer-facing notes.
5. **Genie PAYG go-live (2026-07-06) SKU codes.** Confirm new entries in `system.billing.list_prices` so the Genie cost view picks them up on day one. AI/BI PM team owns this.
6. **Live system-table schema validation.** Could not run `DESCRIBE EXTENDED` against `e2-demo-field-eng` from this sandbox (CLI auth blocked). Schemas referenced are from GA public docs as of 2026-06-15 — recommend a 30-min validation pass in the workspace before merging the V3 spec.

# Spike G — Incrementalisation: can we delta-fetch entity-catalogue endpoints?

**Date:** 2026-07-01 · **Author:** Sam Le Corre · **Status:** Research-only — no source changes · **Target:** V3.x (post-CP2) / V4 planning  
**SDK version researched:** databricks-sdk-py v0.119.0 (latest GA, Jun 24 2026)

---

## Summary

- **None of the five entity-catalogue SDK APIs (Genie spaces, Lakeview dashboards, registered models, serving endpoints, apps) expose any incremental / delta-fetch capability.** There is no `updated_after`, `filter_by` time-range, ETag, or CDC cursor on any of these endpoints. Every run must re-list every entity — this is structural, not a missing parameter.
- **The only SDK call with server-side time-range filtering is `w.query_history.list(filter_by=QueryFilter(query_start_time_range=...))`.** This is actionable today: switch from the system-table read (`system.query.history`) to the API for incremental pulls, or continue to use the system table with a simple `WHERE start_time > watermark` predicate (simpler and supports longer windows).
- **System tables (`system.access.audit`, `system.query.history`) are the primary incrementalisation lever for V3.** Both have a `event_time` / `start_time` timestamp column safe enough to use as a high-watermark for batch runs, with a ~15-minute latency caveat. A dedicated `dim_pipeline_watermarks` Delta table can track per-source HWMs across runs.
- **Practical recommendation for V3.x:** (1) Keep SDK full-scans for all five catalogue endpoints but run them concurrently (already done in CP2); (2) Switch the `system.access.audit` and `system.query.history` reads to watermarked incremental pulls; (3) Track watermarks in `dim_pipeline_watermarks`; (4) Accept full-rescans for entity catalogues — they are bounded in time by the N entity count and 8-worker thread pool, not by SDK incrementalisation.

---

## Table: endpoint × incremental support

| SDK method | REST path | Incremental support | Notes |
|---|---|---|---|
| `w.genie.list_spaces(page_size, page_token)` | `GET /api/2.0/genie/spaces` | **No** | Only `page_size` (max 100) and `page_token`. No time filter, no ETag. |
| `w.genie.list_conversations(space_id, include_all, page_size, page_token)` | `GET /api/2.0/genie/spaces/{space_id}/conversations` | **No** | `include_all` (bool — show all users' convs if caller has Manage) is the only non-pagination param. No `since`, no time filter. Spike A already recommends dropping this call in favour of `system.access.audit`. |
| `w.genie.list_conversation_messages(space_id, conversation_id, page_size, page_token)` | `GET /api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages` | **No** | Pagination only. Spike A recommends dropping in favour of `system.access.audit`. |
| `w.lakeview.list(page_size, page_token, show_trashed, view)` | `GET /api/2.0/lakeview/dashboards` | **No** | `show_trashed` and `view` (BASIC vs FULL) are the only non-pagination params. No `update_time` filter. |
| `w.lakeview.list_schedules(dashboard_id, page_size, page_token)` | `GET /api/2.0/lakeview/dashboards/{dashboard_id}/schedules` | **No** | Pagination only. |
| `w.lakeview.list_subscriptions(dashboard_id, schedule_id, page_size, page_token)` | `GET /api/2.0/lakeview/dashboards/{dashboard_id}/schedules/{schedule_id}/subscriptions` | **No** | Pagination only. |
| `w.registered_models.list(catalog_name, schema_name, include_browse, max_results, page_token)` | `GET /api/2.1/unity-catalog/models` | **No** | Scope filters (`catalog_name`, `schema_name`) and browse-visibility flag only. No time filter. |
| `w.registered_models.get_permissions(name)` | `GET /api/2.0/permissions/registered-models/{name}` | N/A | Single-resource GET, not a list. |
| `w.serving_endpoints.list()` | `GET /api/2.0/serving-endpoints` | **No** | Zero query parameters. Returns all endpoints flat. No pagination token, no filter. |
| `w.serving_endpoints.get_permissions(name)` | `GET /api/2.0/permissions/serving-endpoints/{name}` | N/A | Single-resource GET. |
| `w.apps.list(page_size, page_token, space)` | `GET /api/2.0/apps` | **No** | `space` (filter by App Space name, added May/Jun 2026) is the only non-pagination param. No time filter. |
| `w.apps.get_permissions(name)` | `GET /api/2.0/permissions/apps/{name}` | N/A | Single-resource GET. |
| `w.query_history.list(filter_by, include_metrics, max_results, page_token)` | `GET /api/2.0/sql/history/queries` | **Partial** — `filter_by.query_start_time_range` | `filter_by=QueryFilter(query_start_time_range=TimeRange(start_time_ms=..., end_time_ms=...))`. Supports filtering by user IDs, warehouse IDs, status, and time range. `max_results` ≤ 1000. Pagination via `page_token`. Note: the V3 pipeline currently reads `system.query.history` via SQL — this API is an alternative path, not a replacement of the system table. |
| `system.access.audit` (SQL) | Delta table / OpenSharing | **Partial** — `WHERE event_time > watermark` | `event_time` is a UTC timestamp (reliable event time, not ingestion time). Recommended predicate: `event_date >= (watermark - INTERVAL 1 DAY)` (uses the partition column) then refine with `event_time`. ~15-min ingestion lag observed in practice; no formal SLA during Public Preview. |
| `system.query.history` (SQL) | Delta table / OpenSharing | **Partial** — `WHERE start_time > watermark` | `start_time` is UTC timestamp representing when Databricks received the request. Predicate push-down works on `start_time`. Same ~15-min lag applies; no SLA during Public Preview. |
| `system.serving.served_entities` (SQL) | Delta table / OpenSharing | **No column for delta-pull** | Slow-changing dimension (SCD). Contains endpoint metadata but no `change_time` column documented. Best approach: full-replace or streaming with `skipChangeCommits=true`. |
| `system.serving.endpoint_usage` (SQL) | Delta table / OpenSharing | **Partial** — `request_time` | 90-day retention. Contains `request_time` (timestamp) per token-usage row. Incremental `WHERE request_time > watermark` is valid. Requires "enable usage tracking" opt-in per endpoint. |

---

## What we could implement now vs backlog

### Implement now (V3.x — no architecture change)

1. **Watermark `system.access.audit` reads.** The `02_mvFact*.sql` views re-scan all audit rows on every run. Replace with:
   ```sql
   -- In 02_mvFactDashboardUsage.sql (and equivalents)
   WHERE event_date >= date(watermark_ts - INTERVAL 1 DAY)
     AND event_time > watermark_ts
   ```
   Read `watermark_ts` from `dim_pipeline_watermarks` keyed by `source='system.access.audit'`. After each successful run, write `MAX(event_time)` back. Use a safety lag of `NOW() - INTERVAL 15 MINUTES` as the upper bound to avoid capturing still-inflight events.

2. **Watermark `system.query.history` reads.** Same pattern using `start_time`. Already scoped to a time window in `02_mvFactDashboardUsage.sql`? Confirm and enforce.

3. **Watermark `system.serving.endpoint_usage` reads.** Add a `WHERE request_time > watermark` predicate once the endpoint usage panel is activated (opt-in).

4. **Confirm `w.query_history.list(filter_by=...)` is not needed.** The system-table path is simpler and can cover longer windows. The API path (`filter_by.query_start_time_range`) is a valid alternative if system-table latency is ever unacceptable, but keep to the SQL path for now.

5. **Document the ~15-min lag in the pipeline.** Add a note in `01_get_metadata.ipynb` and the dashboard "freshness" panel that system-table data has a ~15-minute ingestion delay — run windows should start at `last_watermark` and end at `NOW() - 15 MINUTES`.

### Backlog (V4 or later — architecture/schema change needed)

1. **`dim_pipeline_watermarks` Delta table.** Create a small watermark-tracking table (source, last_updated_ts, run_id, row_count). Updated as the final step of each notebook run. Enables true incremental reads across all system-table sources.

   Suggested schema:
   ```sql
   CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_pipeline_watermarks (
     source        STRING NOT NULL,   -- e.g. 'system.access.audit', 'system.query.history'
     watermark_ts  TIMESTAMP NOT NULL,
     run_id        STRING,
     updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
     CONSTRAINT pk_watermark PRIMARY KEY (source)
   );
   ```

2. **SDK full-scan amortisation via MERGE.** The current `merge_workspace_scoped` already does upsert — so even though the SDK re-lists everything, unchanged rows are no-ops in Delta. The scan cost is the SDK network time, not the Delta write. For very large workspaces (49k dashboards), the bottleneck is SDK pagination time (8 pages of 100 × 490 = ~4 API calls per second ≈ ~2 min). This is acceptable; no SDK incremental path exists to improve it.

3. **Active-only filtering via lineage before `get_permissions` fan-out.** For registered models and serving endpoints: filter the list to `full_name IN (SELECT DISTINCT resource_id FROM system.access.table_lineage WHERE event_time > NOW() - INTERVAL 30 DAY)` before calling `get_permissions`. This reduces permission-check fan-out to recently-accessed resources only, shrinking from O(N total models) to O(N active models).

4. **`system.serving.served_entities` as dimension for serving endpoint catalogue.** Instead of or alongside `w.serving_endpoints.list()`, join `system.serving.served_entities` (SCD table) to get served model names, config snapshots, and `endpoint_delete_time` proxies. This does not enable incremental listing (no clean change_time), but eliminates the `get_permissions` fan-out for serving endpoints — permissions are not in the system table, so the SDK call remains for ACLs.

5. **Monitor for Databricks to add incremental listing to the entity catalogue APIs.** Watch the `databricks-sdk-py` changelog (currently v0.119.0) for any `filter_by`, `updated_after`, or `if_modified_since` additions to `lakeview`, `genie`, `apps`, or `registeredmodels` list endpoints. As of 2026-07-01, none exist and no public roadmap mentions them.

---

## Watermark strategy — `dim_pipeline_watermarks` sketch

### Motivation

Each pipeline run currently reads all audit/query-history rows every time. At 365-day retention with millions of rows, this is increasingly expensive as the workspace grows. A per-source watermark allows each notebook cell to read only rows newer than the previous run's high-water mark.

### Proposed pattern

```python
# --- in 01_get_metadata.ipynb or a shared helper ---

def get_watermark(spark, catalog: str, schema: str, source: str,
                  fallback_days: int = 30) -> str:
    """Return ISO timestamp for the last successful watermark, or NOW()-fallback_days."""
    from datetime import datetime, timedelta, timezone
    try:
        row = spark.sql(f"""
            SELECT watermark_ts FROM {catalog}.{schema}.dim_pipeline_watermarks
            WHERE source = '{source}'
        """).first()
        if row:
            return row["watermark_ts"].isoformat()
    except Exception:
        pass
    fallback = datetime.now(timezone.utc) - timedelta(days=fallback_days)
    return fallback.isoformat()


def set_watermark(spark, catalog: str, schema: str, source: str,
                  new_ts: str, run_id: str) -> None:
    """Upsert the watermark for a source after a successful run."""
    spark.sql(f"""
        MERGE INTO {catalog}.{schema}.dim_pipeline_watermarks AS t
        USING (SELECT '{source}' AS source,
                      CAST('{new_ts}' AS TIMESTAMP) AS watermark_ts,
                      '{run_id}' AS run_id,
                      CURRENT_TIMESTAMP() AS updated_at) AS s
        ON t.source = s.source
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

### Per-source watermark logic

| Source | Watermark column | Upper bound | Safety lag |
|---|---|---|---|
| `system.access.audit` | `event_time` | `NOW()` | `- INTERVAL 15 MINUTES` |
| `system.query.history` | `start_time` | `NOW()` | `- INTERVAL 15 MINUTES` |
| `system.serving.endpoint_usage` | `request_time` | `NOW()` | `- INTERVAL 15 MINUTES` |
| SDK entity catalogues | N/A — always full-scan | N/A | N/A |

### Clock-skew and latency notes

- `event_time` in `system.access.audit` represents the actual event time (not ingestion time). Rows can arrive up to ~15 minutes late. Using `NOW() - 15 MINUTES` as the upper bound prevents reading a half-written batch.
- No formal SLA exists during Public Preview. Build a monitoring alert if `MAX(event_time)` in the table falls more than 2 hours behind `NOW()` — that signals a Databricks-side ingestion outage (one documented in 2025-10-16 → 2025-11-19 for some tenants).
- `system.query.history` does **not** support streaming (`Trigger.AvailableNow` is not supported on OpenSharing tables). Use batch reads with the watermark pattern.
- The `event_date` partition column is the right push-down predicate; always filter `event_date >= date(watermark_ts - INTERVAL 1 DAY)` first, then refine with the timestamp column.

---

## Open questions

1. **Does `system.query.history` already have a `WHERE start_time > ...` predicate in `02_mvFact*.sql`?** If not, this is the single highest-value change — a 50k-row workspace could scan 365 days × N rows every run.
2. **Is `system.serving.endpoint_usage` opt-in per endpoint enabled in reference customer workspaces?** If not, the token-count panel will be empty. Add a setup step or documentation note.
3. **`dim_pipeline_watermarks` persistence across `force_reinit=true` runs.** Should a `force_reinit` reset watermarks (full historical re-scan) or preserve them? Recommend: reset watermarks only if `force_reinit=true` AND the user explicitly passes `reset_watermarks=true`.
4. **Are there any Databricks engineering plans to add `filter_by` or `updated_after` to the Lakeview or Genie catalogue list endpoints?** No public roadmap item found as of 2026-07-01. Worth filing a product feedback request for `GET /api/2.0/lakeview/dashboards?updated_after=<ISO>` and `GET /api/2.0/genie/spaces?updated_after=<ISO>` — the data model has `update_time`/`updated_at` on both resources.
5. **Safety of using `event_time` as a monotonic watermark across re-runs during a Databricks system-table outage.** If an outage causes a gap, the watermark would jump over it and those events would be permanently missed. Mitigation: keep a configurable `max_lookback_days` floor (e.g. 7) so the watermark never advances more than 7 days behind the outage start.

---

## Sources

- [w.genie: Genie — SDK docs](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/dashboards/genie.html)
- [w.lakeview: Lakeview — SDK docs](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/dashboards/lakeview.html)
- [w.registered_models: Registered Models — SDK docs](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/catalog/registered_models.html)
- [w.serving_endpoints: Serving Endpoints — SDK docs](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/serving/serving_endpoints.html)
- [w.apps: Apps — SDK docs](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/apps/apps.html)
- [w.query_history: Query History — SDK docs](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/sql/query_history.html)
- [List Genie Spaces | REST API reference](https://docs.databricks.com/api/workspace/genie/listspaces)
- [List conversations in a Genie Space | REST API reference](https://docs.databricks.com/api/workspace/genie/listconversations)
- [List conversation messages | REST API reference](https://docs.databricks.com/api/workspace/genie/listconversationmessages)
- [List dashboards | Lakeview API | REST API reference](https://docs.databricks.com/api/workspace/lakeview/list)
- [List Registered Models | REST API reference](https://docs.databricks.com/api/workspace/registeredmodels/list)
- [List Queries | Query History API | REST API reference](https://docs.databricks.com/api/workspace/queryhistory/list)
- [Use the Genie Spaces API | Databricks on AWS](https://docs.databricks.com/aws/en/genie/conversation-api)
- [Audit log system table reference | Databricks on AWS](https://docs.databricks.com/aws/en/admin/system-tables/audit-logs)
- [Query history system table reference | Databricks on AWS](https://docs.databricks.com/aws/en/admin/system-tables/query-history)
- [System tables reference | Databricks on AWS](https://docs.databricks.com/aws/en/admin/system-tables/)
- [databricks-sdk-py CHANGELOG.md (main)](https://github.com/databricks/databricks-sdk-py/blob/main/CHANGELOG.md)
- [databricks-sdk-py releases](https://github.com/databricks/databricks-sdk-py/releases)
- [Community: system.access.audit lag (~15 min)](https://community.databricks.com/t5/data-engineering/unity-catalog-system-access-audit-lag/td-p/67319)
- [Community: system tables freshness SLA (no SLA in Public Preview)](https://community.databricks.com/t5/data-engineering/system-tables-latency/td-p/73935)

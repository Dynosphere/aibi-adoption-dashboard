-- mvFactAssetAdoption — cross-asset headline fact table.
-- UNION ALL of every asset-type mvFact* projected to a common 8-column shape
-- so that a single table powers the Adoption Overview dashboard page.
--
-- Sources:
--   * mvFactGenieUsage          — Genie space usage (audit-derived; no direct DBU cost here).
--   * mvFactDashboardUsage      — AI/BI dashboard views (audit-derived; no DBU column).
--   * mvFactAppUsage            — Databricks Apps DBU cost + lifecycle events.
--   * mvFactServingUsage        — Model Serving DBU cost + request counts.
--   * mvFactVectorSearchCost    — Vector Search DBU cost only (no activity metric available).
--
-- Common shape: (asset_type, asset_id, asset_name, usage_date, workspace_id,
--               workspace_name, dbu_cost, activity_count).
-- Grain / MERGE ON: (asset_type, asset_id, usage_date, workspace_id).
-- Partition: (workspace_id, asset_type).
--
-- Source window: 30-day rolling window on each UNION branch to bound the MERGE
-- source. No per-run watermarking is needed here — each upstream source MV
-- manages its own incremental state; this rollup reads a bounded recent window.
--
-- DBU cost notes:
--   * genie:         dbu_cost = 0 (mvFactGenieUsage carries no DBU column; consumers
--                    needing cost should join to mvFactGeniePaygoCost directly).
--   * dashboard:     dbu_cost = 0 (mvFactDashboardUsage is audit-only; no billing source).
--   * app:           dbu_cost = sum(dbus) from mvFactAppUsage.
--   * serving:       dbu_cost = sum(dbus) aggregated over served_entity_id (base grain
--                    is finer than this table's grain).
--   * vector_search: dbu_cost = sum(dbus) aggregated over sku_name (base grain is finer).
--
-- activity_count notes:
--   * genie:         sum(message_count) across user_email/surface within (space, date, workspace).
--   * dashboard:     num_views (count of audit events for the dashboard-date-workspace).
--   * app:           lifecycle_events from mvFactAppUsage.
--   * serving:       sum(request_count) aggregated over served_entity_id.
--   * vector_search: NULL — no query-rate metric available in Databricks system tables
--                    as of mid-2026.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAssetAdoption') (
  asset_type     STRING        COMMENT 'Category of the AI/BI asset. One of: genie, dashboard, app, serving, vector_search.',
  asset_id       STRING        COMMENT 'Unique identifier of the asset within its type (e.g. space_id for Genie, dashboard_id for dashboards, app_id for Apps, endpoint_id for Serving and Vector Search).',
  asset_name     STRING        COMMENT 'Human-readable display name of the asset at ingest time.',
  usage_date     DATE          COMMENT 'Calendar date of the underlying usage or event record.',
  workspace_id   BIGINT        COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name STRING        COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  dbu_cost       DECIMAL(38,6) COMMENT 'Sum of DBUs consumed by the asset on this date. Zero for genie (no direct DBU column in mvFactGenieUsage; join to mvFactGeniePaygoCost for cost) and for dashboard (audit-only source). NULL is replaced with 0 for consistency.',
  activity_count BIGINT        COMMENT 'Count of user-facing activities: message count for Genie, view count for dashboards, lifecycle event count for Apps, request count for Serving. NULL for vector_search (no query-rate metric available in Databricks system tables as of mid-2026).'
) USING DELTA
  PARTITIONED BY (workspace_id, asset_type)
  COMMENT 'V3 mvFactAssetAdoption. Cross-asset headline fact table. UNION ALL of mvFactGenieUsage, mvFactDashboardUsage, mvFactAppUsage, mvFactServingUsage, and mvFactVectorSearchCost projected to a common 8-column shape (asset_type, asset_id, asset_name, usage_date, workspace_id, workspace_name, dbu_cost, activity_count). Powers the Adoption Overview dashboard page. Grain: (asset_type, asset_id, usage_date, workspace_id). Source window: 30-day rolling window per branch. DBU cost is 0 for genie and dashboard (no billing source in those MVs).';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAssetAdoption') tgt
USING (

  -- ── Genie ──────────────────────────────────────────────────────────────────
  -- Source grain: (space_id, usage_date, workspace_id, user_email, surface).
  -- Aggregate to (space_id, usage_date, workspace_id) for the common shape.
  -- dbu_cost = 0 (no DBU column in mvFactGenieUsage; see table COMMENT).
  -- activity_count = sum of message_count across all user/surface combinations.
  SELECT
    'genie'                                                           AS asset_type,
    gu.space_id                                                       AS asset_id,
    gu.space_title                                                    AS asset_name,
    gu.usage_date                                                     AS usage_date,
    gu.workspace_id                                                   AS workspace_id,
    gu.workspace_name                                                 AS workspace_name,
    CAST(0 AS DECIMAL(38,6))                                          AS dbu_cost,
    CAST(sum(gu.message_count) AS BIGINT)                             AS activity_count
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage') gu
  WHERE gu.usage_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY
    gu.space_id,
    gu.space_title,
    gu.usage_date,
    gu.workspace_id,
    gu.workspace_name

  UNION ALL

  -- ── Dashboard ──────────────────────────────────────────────────────────────
  -- Source grain: (dashboard_id, viewed_date, workspace_id).
  -- Column name for date is `viewed_date` (not `usage_date`) in mvFactDashboardUsage.
  -- dbu_cost = 0 (audit-only source; no billing column in mvFactDashboardUsage).
  -- activity_count = num_views (total audit event count for the dashboard-date-workspace).
  SELECT
    'dashboard'                                                       AS asset_type,
    du.dashboard_id                                                   AS asset_id,
    du.dashboard_name                                                 AS asset_name,
    du.viewed_date                                                    AS usage_date,
    du.workspace_id                                                   AS workspace_id,
    du.workspace_name                                                 AS workspace_name,
    CAST(0 AS DECIMAL(38,6))                                          AS dbu_cost,
    CAST(du.num_views AS BIGINT)                                      AS activity_count
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactDashboardUsage') du
  WHERE du.viewed_date >= current_date() - INTERVAL 30 DAYS

  UNION ALL

  -- ── Apps ───────────────────────────────────────────────────────────────────
  -- Source grain: (app_id, usage_date, workspace_id) — matches the common shape exactly.
  -- dbu_cost = dbus from mvFactAppUsage.
  -- activity_count = lifecycle_events from mvFactAppUsage.
  SELECT
    'app'                                                             AS asset_type,
    au.app_id                                                         AS asset_id,
    au.app_name                                                       AS asset_name,
    au.usage_date                                                     AS usage_date,
    au.workspace_id                                                   AS workspace_id,
    au.workspace_name                                                 AS workspace_name,
    CAST(au.dbus AS DECIMAL(38,6))                                    AS dbu_cost,
    CAST(au.lifecycle_events AS BIGINT)                               AS activity_count
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage') au
  WHERE au.usage_date >= current_date() - INTERVAL 30 DAYS

  UNION ALL

  -- ── Model Serving ──────────────────────────────────────────────────────────
  -- Source grain: (endpoint_id, served_entity_id, usage_date, workspace_id).
  -- The base grain is finer than the common shape (includes served_entity_id).
  -- Aggregate SUM(dbus) and SUM(request_count) per (endpoint_id, usage_date, workspace_id).
  -- endpoint_name is stable across served_entity rows for the same endpoint.
  -- dbu_cost = sum(dbus) across all served entities on this endpoint-date-workspace.
  -- activity_count = sum(request_count) across all served entities.
  SELECT
    'serving'                                                         AS asset_type,
    su.endpoint_id                                                    AS asset_id,
    max(su.endpoint_name)                                             AS asset_name,
    su.usage_date                                                     AS usage_date,
    su.workspace_id                                                   AS workspace_id,
    max(su.workspace_name)                                            AS workspace_name,
    CAST(sum(su.dbus) AS DECIMAL(38,6))                               AS dbu_cost,
    CAST(sum(su.request_count) AS BIGINT)                             AS activity_count
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage') su
  WHERE su.usage_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY
    su.endpoint_id,
    su.usage_date,
    su.workspace_id

  UNION ALL

  -- ── Vector Search ──────────────────────────────────────────────────────────
  -- Source grain: (endpoint_id, usage_date, workspace_id, sku_name).
  -- The base grain is finer than the common shape (includes sku_name for compute
  -- vs. storage SKU split). Aggregate SUM(dbus) per (endpoint_id, usage_date, workspace_id).
  -- activity_count = NULL (no query-rate metric in Databricks system tables as of mid-2026).
  SELECT
    'vector_search'                                                   AS asset_type,
    vc.endpoint_id                                                    AS asset_id,
    max(vc.endpoint_name)                                             AS asset_name,
    vc.usage_date                                                     AS usage_date,
    vc.workspace_id                                                   AS workspace_id,
    max(vc.workspace_name)                                            AS workspace_name,
    CAST(sum(vc.dbus) AS DECIMAL(38,6))                               AS dbu_cost,
    CAST(NULL AS BIGINT)                                              AS activity_count
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost') vc
  WHERE vc.usage_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY
    vc.endpoint_id,
    vc.usage_date,
    vc.workspace_id

) src
ON  tgt.asset_type   = src.asset_type
AND tgt.asset_id     = src.asset_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

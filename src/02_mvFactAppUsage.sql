-- mvFactAppUsage — unified fact for Databricks Apps billing cost + audit lifecycle events.
-- Replaces the V2 audit-only schema (num_deploys, num_gets, etc.) and the V3 incremental
-- MERGE. V3 unified DBU cost from billing and lifecycle event counts from audit;
-- this stateless rebuild keeps that logic but uses a bounded lookback window instead.
--
-- Sources:
--   * system.billing.usage      — DBU quantity and USD cost rows where
--                                 billing_origin_product = 'APPS'.
--   * system.access.audit       — lifecycle events where service_name = 'apps'
--                                 (deployApplication, getApplication, updateApp,
--                                  startApp, stopApp, and related actions).
--   * system.billing.list_prices — effective list-price rate for the DBU-to-USD join.
--   * system.access.workspaces_latest — workspace name resolution.
--
-- Grain: (app_id, usage_date, workspace_id).
-- Partition: (workspace_id).
-- Stateless full rebuild each run over a bounded :lookback_days window.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage') (
  app_id           STRING        COMMENT 'App identifier; coalesces billing usage_metadata.app_id and audit request_params.app_id (falling back to request_params.name when app_id is absent from audit).',
  app_name         STRING        COMMENT 'App display name from system.billing.usage usage_metadata.app_name; NULL when the row originates from audit only.',
  usage_date       DATE          COMMENT 'Calendar date of the underlying usage or event record.',
  workspace_id     BIGINT        COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name   STRING        COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  dbus             DECIMAL(38,6) COMMENT 'Sum of DBUs consumed by Apps for this (app, date, workspace), from system.billing.usage where billing_origin_product = APPS. Zero when no billing row exists for this combination.',
  dollars          DECIMAL(38,6) COMMENT 'USD list-price cost derived from dbus via the system.billing.list_prices cloud + usage_start_time effective-rate join. Zero when no billing row or no matching price row exists.',
  lifecycle_events BIGINT        COMMENT 'Count of audit events for this (app, date, workspace) from system.access.audit where service_name = apps (covers deployApplication, getApplication, updateApp, startApp, stopApp, and related actions). Zero when no audit row exists for this combination.',
  distinct_users   BIGINT        COMMENT 'Count of distinct user_identity.email values in audit events for this (app, date, workspace). Zero when no audit row exists for this combination.'
) USING DELTA
  PARTITIONED BY (workspace_id)
  COMMENT 'Unified fact table for Databricks Apps consumption and lifecycle activity. Sources: system.billing.usage (DBU + USD cost where billing_origin_product = APPS) full-outer-joined with system.access.audit (lifecycle events where service_name = apps). Grain: (app_id, usage_date, workspace_id). Stateless full rebuild over :lookback_days.';

INSERT OVERWRITE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage')
WITH
  -- Aggregate billing DBU and USD cost from system.billing.usage for APPS rows.
  -- Lookback lower bound applied to usage_start_time (the billing timestamp column).
  -- Pricing join uses the established convention:
  --   sku_name + cloud + currency_code = 'USD' + usage_start_time in [price_start, price_end).
  billing_agg AS (
    SELECT
      bu.usage_metadata.app_id                                       AS app_id,
      bu.usage_metadata.app_name                                     AS app_name,
      bu.usage_date                                                  AS usage_date,
      cast(bu.workspace_id AS BIGINT)                                AS workspace_id,
      cast(sum(bu.usage_quantity) AS DECIMAL(38,6))                  AS dbus,
      cast(sum(bu.usage_quantity * coalesce(p.pricing.effective_list.default, 0))
           AS DECIMAL(38,6))                                         AS dollars
    FROM system.billing.usage bu
    LEFT JOIN system.billing.list_prices p
      ON  p.sku_name      = bu.sku_name
      AND p.cloud         = bu.cloud
      AND p.currency_code = 'USD'
      AND bu.usage_start_time >= p.price_start_time
      AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
    WHERE bu.billing_origin_product = 'APPS'
      AND bu.usage_metadata.app_id IS NOT NULL
      AND bu.usage_start_time >= date_sub(current_date(), :lookback_days)
    GROUP BY ALL
  ),

  -- Aggregate audit lifecycle event counts from system.access.audit for Apps rows.
  -- Lookback lower bound applied to event_time (the audit timestamp column).
  audit_agg AS (
    SELECT
      coalesce(a.request_params.app_id, a.request_params.name)      AS app_id,
      cast(a.event_date AS DATE)                                     AS usage_date,
      cast(a.workspace_id AS BIGINT)                                 AS workspace_id,
      count(*)                                                       AS lifecycle_events,
      count(DISTINCT a.user_identity.email)                          AS distinct_users
    FROM system.access.audit a
    WHERE a.service_name = 'apps'
      AND a.event_time >= date_sub(current_date(), :lookback_days)
      AND coalesce(a.request_params.app_id, a.request_params.name) IS NOT NULL
    GROUP BY ALL
  )

SELECT
  coalesce(b.app_id,       e.app_id)                               AS app_id,
  b.app_name,
  coalesce(b.usage_date,   e.usage_date)                           AS usage_date,
  coalesce(b.workspace_id, e.workspace_id)                         AS workspace_id,
  w.workspace_name,
  coalesce(b.dbus,    cast(0 AS DECIMAL(38,6)))                    AS dbus,
  coalesce(b.dollars, cast(0 AS DECIMAL(38,6)))                    AS dollars,
  coalesce(e.lifecycle_events, 0)                                  AS lifecycle_events,
  coalesce(e.distinct_users,   0)                                  AS distinct_users
FROM billing_agg b
FULL OUTER JOIN audit_agg e
  ON  e.app_id       = b.app_id
  AND e.usage_date   = b.usage_date
  AND e.workspace_id = b.workspace_id
LEFT JOIN system.access.workspaces_latest w
  ON  w.workspace_id = coalesce(b.workspace_id, e.workspace_id);

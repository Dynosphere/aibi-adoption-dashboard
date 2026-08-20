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
--
-- Grain reconciliation — app_id is canonical:
-- billing carries both usage_metadata.app_id (a UUID, canonical) and usage_metadata.app_name
-- on every row; app_name is a duplicable label (names get reused across ids on redeploy), so
-- cost is keyed by app_id. Audit, by contrast, carries only the app NAME (never an id), under
-- action-specific request_params keys — 'name' (start/stop), 'app_name' (deploy), and the
-- nested 'app' JSON (create). Audit events are therefore resolved to a name via a coalesce
-- across those keys (recovering deploy/create events the previous name-only read dropped),
-- then mapped to the canonical app_id through a deterministic (workspace_id, app_name) ->
-- app_id crosswalk built from billing (ambiguous names resolve to the highest-DBU id,
-- tie-broken by app_id). Both sides then join on app_id at the grain below; app_name is
-- coalesced so it is NEVER NULL, and audit-only apps that never incurred cost keep their name
-- as a stable id rather than forming a NULL bucket. Cost TOTALS reconcile to system.billing.usage.
--
-- The adb_apps crawl dimension is intentionally NOT used here (it is currently empty, and a
-- live crawl would miss deleted apps that still carry billing history); billing is a complete,
-- authoritative source for the app_id<->app_name mapping.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage') (
  app_id           STRING        COMMENT 'Canonical app identifier from billing usage_metadata.app_id (UUID). Audit events are mapped to this id via the billing-built (workspace_id, app_name) crosswalk; audit-only apps with no billing history fall back to the app name as a stable id.',
  app_name         STRING        COMMENT 'App display name, resolved from billing usage_metadata.app_name (canonical) or the audit request_params name keys. Never NULL.',
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
  -- Aggregate billing DBU and USD cost from system.billing.usage for APPS rows, keyed by the
  -- canonical usage_metadata.app_id. Lookback lower bound applied to usage_start_time.
  -- Pricing join uses the established convention:
  --   sku_name + cloud + currency_code = 'USD' + usage_start_time in [price_start, price_end).
  -- app_name via max() is deterministic (a given app_id has a single stable name in billing).
  billing_agg AS (
    SELECT
      bu.usage_metadata.app_id                                       AS app_id,
      max(bu.usage_metadata.app_name)                                AS app_name,
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
    GROUP BY bu.usage_metadata.app_id, bu.usage_date, cast(bu.workspace_id AS BIGINT)
  ),

  -- Deterministic (workspace_id, app_name) -> app_id crosswalk built from billing. Names can
  -- repeat across app_ids (redeploys), so an ambiguous (workspace_id, app_name) pair resolves
  -- to the app_id with the highest total DBUs, tie-broken by app_id, via ROW_NUMBER. This is
  -- how audit events (which carry only a name) are mapped to the canonical id.
  name_totals AS (
    SELECT
      cast(bu.workspace_id AS BIGINT)     AS workspace_id,
      bu.usage_metadata.app_name          AS app_name,
      bu.usage_metadata.app_id            AS app_id,
      sum(bu.usage_quantity)              AS dbus
    FROM system.billing.usage bu
    WHERE bu.billing_origin_product = 'APPS'
      AND bu.usage_metadata.app_id   IS NOT NULL
      AND bu.usage_metadata.app_name IS NOT NULL
      AND bu.usage_start_time >= date_sub(current_date(), :lookback_days)
    GROUP BY 1, 2, 3
  ),
  app_crosswalk AS (
    SELECT workspace_id, app_name, app_id
    FROM (
      SELECT workspace_id, app_name, app_id,
             row_number() OVER (PARTITION BY workspace_id, app_name
                                ORDER BY dbus DESC, app_id) AS rn
      FROM name_totals
    )
    WHERE rn = 1
  ),

  -- Aggregate audit lifecycle events from system.access.audit for Apps rows, keyed by the app
  -- NAME resolved across the action-specific request_params keys (audit never carries an id):
  --   'name'      — startApp / stopApp
  --   'app_name'  — deployApp
  --   'app' JSON  — createApp  (e.g. {"name":"my-app", ...})
  -- Coalescing across all three recovers deploy/create events the previous name-only read
  -- dropped. Lookback lower bound applied to event_time.
  audit_agg AS (
    SELECT
      cast(a.workspace_id AS BIGINT)                                 AS workspace_id,
      coalesce(
        a.request_params['name'],
        a.request_params['app_name'],
        get_json_object(a.request_params['app'], '$.name')
      )                                                              AS app_name,
      cast(a.event_date AS DATE)                                     AS usage_date,
      count(*)                                                       AS lifecycle_events,
      count(DISTINCT a.user_identity.email)                          AS distinct_users
    FROM system.access.audit a
    WHERE a.service_name = 'apps'
      AND a.event_time >= date_sub(current_date(), :lookback_days)
      AND coalesce(
            a.request_params['name'],
            a.request_params['app_name'],
            get_json_object(a.request_params['app'], '$.name')
          ) IS NOT NULL
    GROUP BY 1, 2, 3
  ),

  -- Map audit's app_name to the canonical app_id via the billing crosswalk. Apps with audit
  -- activity but no billing history (never incurred cost) are not in the crosswalk; they keep
  -- the app_name as a stable synthetic id so they are not lost and never form a NULL bucket.
  audit_by_id AS (
    SELECT
      coalesce(x.app_id, e.app_name)  AS app_id,
      e.app_name                      AS app_name,
      e.usage_date                    AS usage_date,
      e.workspace_id                  AS workspace_id,
      e.lifecycle_events              AS lifecycle_events,
      e.distinct_users                AS distinct_users
    FROM audit_agg e
    LEFT JOIN app_crosswalk x
      ON  x.workspace_id = e.workspace_id
      AND x.app_name     = e.app_name
  )

SELECT
  coalesce(b.app_id,       e.app_id)                               AS app_id,
  coalesce(b.app_name,     e.app_name)                             AS app_name,
  coalesce(b.usage_date,   e.usage_date)                           AS usage_date,
  coalesce(b.workspace_id, e.workspace_id)                         AS workspace_id,
  w.workspace_name,
  coalesce(b.dbus,    cast(0 AS DECIMAL(38,6)))                    AS dbus,
  coalesce(b.dollars, cast(0 AS DECIMAL(38,6)))                    AS dollars,
  coalesce(e.lifecycle_events, 0)                                  AS lifecycle_events,
  coalesce(e.distinct_users,   0)                                  AS distinct_users
FROM billing_agg b
FULL OUTER JOIN audit_by_id e
  ON  e.app_id       = b.app_id
  AND e.usage_date   = b.usage_date
  AND e.workspace_id = b.workspace_id
LEFT JOIN system.access.workspaces_latest w
  ON  w.workspace_id = coalesce(b.workspace_id, e.workspace_id);

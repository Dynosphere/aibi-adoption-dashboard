-- mvFactAppUsage — V3 incremental MERGE on system.billing.usage + system.access.audit.
-- Replaces the V2 audit-only CREATE OR REPLACE TABLE that joined a custom adb_apps dimension
-- and had a different schema (num_deploys, num_gets, etc.). V3 unifies DBU cost from billing
-- and lifecycle event counts from audit into a single fact table.
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
-- Source window: watermark-driven incremental read.
--   lower bound  = watermark_ts from dim_pipeline_watermarks (or TIMESTAMP '2024-01-01 00:00:00'
--                  on first run — bootstrap epoch).
--   upper bound  = current_timestamp() - INTERVAL 15 MINUTES (ingestion-lag safety buffer).
-- Each source is pre-filtered by its own timestamp column before the FULL OUTER JOIN:
--   billing: usage_start_time > watermark (coalesced to epoch).
--   audit:   event_time       > watermark (coalesced to epoch).
-- Watermark advance = LEAST(MAX(billing.usage_start_time), MAX(audit.event_time)) capped
-- at the upper bound — the safe minimum across both sources so neither source gets skipped.
-- After the fact MERGE, a second MERGE advances dim_pipeline_watermarks per workspace_id.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage') (
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
  COMMENT 'V3 mvFactAppUsage. Unified fact table for Databricks Apps consumption and lifecycle activity. Sources: system.billing.usage (DBU + USD cost where billing_origin_product = APPS) full-outer-joined with system.access.audit (lifecycle events where service_name = apps). Grain: (app_id, usage_date, workspace_id). Source window: watermark-driven incremental read from dim_pipeline_watermarks; bootstrap epoch 2024-01-01.';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage') tgt
USING (
  WITH watermark AS (
    -- Per-workspace watermarks for this source. Left-joined against source tables
    -- so workspaces that have never been processed receive NULL, which COALESCE
    -- converts to the bootstrap epoch below. Avoids SCALAR_SUBQUERY_TOO_MANY_ROWS
    -- in shared-catalog multi-workspace deployments where multiple workspaces
    -- write watermarks for the same source_name.
    SELECT workspace_id, watermark_ts
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
    WHERE source_name = 'mvFactAppUsage'
  ),

  -- Aggregate billing DBU and USD cost from system.billing.usage for APPS rows.
  -- Watermark lower bound applied to usage_start_time (the billing timestamp column).
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
           AS DECIMAL(38,6))                                         AS dollars,
      max(bu.usage_start_time)                                       AS max_billing_ts
    FROM system.billing.usage bu
    LEFT JOIN watermark w
      ON w.workspace_id = cast(bu.workspace_id AS BIGINT)
    LEFT JOIN system.billing.list_prices p
      ON  p.sku_name      = bu.sku_name
      AND p.cloud         = bu.cloud
      AND p.currency_code = 'USD'
      AND bu.usage_start_time >= p.price_start_time
      AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
    WHERE bu.billing_origin_product = 'APPS'
      AND bu.usage_metadata.app_id IS NOT NULL
      AND bu.usage_start_time > coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
      AND bu.usage_start_time <= current_timestamp() - INTERVAL 15 MINUTES
    GROUP BY ALL
  ),

  -- Aggregate audit lifecycle event counts from system.access.audit for Apps rows.
  -- Watermark lower bound applied to event_time (the audit timestamp column).
  -- event_date partition overhang (-1 DAY) guards against day-boundary clock skew.
  audit_agg AS (
    SELECT
      coalesce(a.request_params.app_id, a.request_params.name)      AS app_id,
      cast(a.event_date AS DATE)                                     AS usage_date,
      a.workspace_id                                                 AS workspace_id,
      count(*)                                                       AS lifecycle_events,
      count(DISTINCT a.user_identity.email)                          AS distinct_users,
      max(a.event_time)                                              AS max_audit_ts
    FROM system.access.audit a
    LEFT JOIN watermark w
      ON w.workspace_id = a.workspace_id
    WHERE a.service_name = 'apps'
      AND a.event_date >= date(coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')) - INTERVAL 1 DAY
      AND a.event_time > coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
      AND a.event_time <= current_timestamp() - INTERVAL 15 MINUTES
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
    ON  w.workspace_id = coalesce(b.workspace_id, e.workspace_id)
) src
ON  tgt.app_id       = src.app_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Advance the watermark to LEAST(MAX(billing.usage_start_time), MAX(audit.event_time))
-- across both sources, capped at the upper bound used in the fact MERGE. Taking the
-- minimum across sources ensures that neither source is skipped if one is behind the other.
-- Grouped per workspace_id so that shared-catalog / multi-workspace deployments each
-- get their own watermark row. On first run, no row exists -> WHEN NOT MATCHED inserts it.
MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks') tgt
USING (
  SELECT
    'mvFactAppUsage'                                                 AS source_name,
    workspace_id,
    least(
      coalesce(max_billing_ts, current_timestamp() - INTERVAL 15 MINUTES),
      coalesce(max_audit_ts,   current_timestamp() - INTERVAL 15 MINUTES),
      current_timestamp() - INTERVAL 15 MINUTES
    )                                                                AS watermark_ts,
    current_timestamp()                                              AS updated_at
  FROM (
    SELECT
      coalesce(b.workspace_id, a.workspace_id)                      AS workspace_id,
      max(b.usage_start_time)                                        AS max_billing_ts,
      max(a.event_time)                                              AS max_audit_ts
    FROM (
      SELECT cast(workspace_id AS BIGINT) AS workspace_id, usage_start_time
      FROM system.billing.usage
      WHERE billing_origin_product = 'APPS'
        AND usage_start_time > coalesce(
              (SELECT watermark_ts
                 FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
                WHERE source_name  = 'mvFactAppUsage'
                  AND workspace_id = cast(system.billing.usage.workspace_id AS BIGINT)),
              TIMESTAMP '2024-01-01 00:00:00'
            )
        AND usage_start_time <= current_timestamp() - INTERVAL 15 MINUTES
    ) b
    FULL OUTER JOIN (
      SELECT workspace_id, event_time
      FROM system.access.audit
      WHERE service_name = 'apps'
        AND event_time > coalesce(
              (SELECT watermark_ts
                 FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
                WHERE source_name  = 'mvFactAppUsage'
                  AND workspace_id = system.access.audit.workspace_id),
              TIMESTAMP '2024-01-01 00:00:00'
            )
        AND event_time <= current_timestamp() - INTERVAL 15 MINUTES
    ) a
    ON a.workspace_id = b.workspace_id
    GROUP BY coalesce(b.workspace_id, a.workspace_id)
  )
) src
ON  tgt.source_name  = src.source_name
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET
  watermark_ts = greatest(tgt.watermark_ts, src.watermark_ts),
  updated_at   = src.updated_at
WHEN NOT MATCHED THEN INSERT *;

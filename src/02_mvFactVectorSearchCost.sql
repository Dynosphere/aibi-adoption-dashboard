-- mvFactVectorSearchCost — V3 billing-only fact table for Vector Search endpoint cost.
-- Sources:
--   * system.billing.usage  — DBU and USD cost rows where
--                             billing_origin_product = 'VECTOR_SEARCH'.
--
-- Grain: (endpoint_id, usage_date, workspace_id, sku_name).
-- sku_name is part of the grain because a single endpoint can emit rows for
-- both compute SKUs (e.g. VECTOR_SEARCH_STANDARD) and storage SKUs
-- (e.g. VECTOR_SEARCH_STORAGE_OPTIMIZED) on the same day.
-- Partition: (workspace_id).
-- Source window: watermark-driven incremental read.
--   lower bound  = watermark_ts from dim_pipeline_watermarks (or TIMESTAMP '2024-01-01 00:00:00'
--                  on first run — bootstrap epoch).
--   upper bound  = current_timestamp() - INTERVAL 15 MINUTES (ingestion-lag safety buffer).
-- After the fact MERGE a second MERGE advances the watermark per (source_name, workspace_id).
--
-- Note: query-rate (QPS) and latency metrics for Vector Search are not available
-- in Databricks system tables as of mid-2026. This table covers billing cost only.
-- See Superpowers spike-g-incrementalisation for context.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost') (
  endpoint_id    STRING         COMMENT 'Vector Search endpoint identifier, sourced from system.billing.usage.usage_metadata.endpoint_id.',
  endpoint_name  STRING         COMMENT 'Human-readable name of the Vector Search endpoint, sourced from system.billing.usage.usage_metadata.endpoint_name.',
  usage_date     DATE           COMMENT 'Calendar date of the underlying usage record.',
  workspace_id   BIGINT         COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name STRING         COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  sku_name       STRING         COMMENT 'Billing SKU name for the Vector Search charge (e.g. VECTOR_SEARCH_STANDARD, VECTOR_SEARCH_STORAGE_OPTIMIZED).',
  dbus           DECIMAL(38, 6) COMMENT 'Sum of DBUs consumed, from system.billing.usage where billing_origin_product = VECTOR_SEARCH.',
  dollars        DECIMAL(38, 6) COMMENT 'USD cost derived from dbus via the system.billing.list_prices cloud + usage_start_time effective-rate join.'
) USING DELTA
  PARTITIONED BY (workspace_id)
  COMMENT 'V3 mvFactVectorSearchCost. Billing-only fact table for Databricks Vector Search endpoint cost. Sources system.billing.usage where billing_origin_product = VECTOR_SEARCH. QPS and latency metrics are not available in Databricks system tables as of mid-2026; this table covers DBU and USD cost only. Grain: (endpoint_id, usage_date, workspace_id, sku_name). sku_name is in the grain because a single endpoint may emit rows for both compute and storage SKUs on the same day. Source window: watermark-driven incremental read from dim_pipeline_watermarks, bootstrap epoch 2024-01-01.';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost') tgt
USING (
  WITH watermark AS (
    -- Per-workspace watermarks for this source. Left-join against the billing
    -- source table so that workspaces that have never been processed receive
    -- NULL, which the COALESCE below converts to the bootstrap epoch.
    -- Avoids SCALAR_SUBQUERY_TOO_MANY_ROWS in shared-catalog multi-workspace
    -- deployments where multiple workspaces write watermarks for the same source_name.
    SELECT workspace_id, watermark_ts
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
    WHERE source_name = 'mvFactVectorSearchCost'
  )

  SELECT
    bu.usage_metadata.endpoint_id                                    AS endpoint_id,
    bu.usage_metadata.endpoint_name                                  AS endpoint_name,
    bu.usage_date                                                    AS usage_date,
    cast(bu.workspace_id AS BIGINT)                                  AS workspace_id,
    w.workspace_name                                                 AS workspace_name,
    bu.sku_name                                                      AS sku_name,
    cast(sum(bu.usage_quantity) AS DECIMAL(38, 6))                   AS dbus,
    cast(sum(bu.usage_quantity * coalesce(p.pricing.effective_list.default, 0))
         AS DECIMAL(38, 6))                                          AS dollars
  FROM system.billing.usage bu
  LEFT JOIN watermark wm
    ON wm.workspace_id = cast(bu.workspace_id AS BIGINT)
  LEFT JOIN system.access.workspaces_latest w
    ON w.workspace_id = cast(bu.workspace_id AS BIGINT)
  LEFT JOIN system.billing.list_prices p
    ON  p.sku_name      = bu.sku_name
    AND p.cloud         = bu.cloud
    AND p.currency_code = 'USD'
    AND bu.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
  WHERE bu.billing_origin_product = 'VECTOR_SEARCH'
    AND bu.usage_start_time > coalesce(wm.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
    AND bu.usage_start_time <= current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY ALL
) src
ON  tgt.endpoint_id  = src.endpoint_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
AND tgt.sku_name     = src.sku_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Advance the watermark to MAX(usage_start_time) actually processed in the window above,
-- grouped per workspace_id so that shared-catalog / multi-workspace deployments each
-- get their own watermark row. Capped at the upper bound used in the fact MERGE so
-- we never advance past data we did not read.
-- On first run, no row exists for this workspace -> WHEN NOT MATCHED inserts the initial watermark.
--
-- Watermark filter uses the same LEFT JOIN CTE pattern as the fact MERGE. The scalar
-- subquery form failed with SCALAR_SUBQUERY_TOO_MANY_ROWS on shared catalogs — see
-- commit 4ec0b12. GREATEST() on WHEN MATCHED prevents regression regardless of the
-- filter, but forward-only pre-filter still avoids unnecessary scan.
MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks') tgt
USING (
  WITH watermark AS (
    SELECT workspace_id, watermark_ts
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
    WHERE source_name = 'mvFactVectorSearchCost'
  )
  SELECT
    'mvFactVectorSearchCost'                                         AS source_name,
    cast(bu.workspace_id AS BIGINT)                                  AS workspace_id,
    least(
      max(bu.usage_start_time),
      current_timestamp() - INTERVAL 15 MINUTES
    )                                                                AS watermark_ts,
    current_timestamp()                                              AS updated_at
  FROM system.billing.usage bu
  LEFT JOIN watermark wm ON wm.workspace_id = cast(bu.workspace_id AS BIGINT)
  WHERE bu.billing_origin_product = 'VECTOR_SEARCH'
    AND bu.usage_start_time > coalesce(wm.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
    AND bu.usage_start_time <= current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY bu.workspace_id
) src
ON  tgt.source_name  = src.source_name
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET
  watermark_ts = greatest(tgt.watermark_ts, src.watermark_ts),
  updated_at   = src.updated_at
WHEN NOT MATCHED THEN INSERT *;

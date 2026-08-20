-- mvFactVectorSearchCost — billing-only fact for Vector Search endpoint cost.
-- Stateless full rebuild each run over a bounded :lookback_days window.
-- Source: system.billing.usage where billing_origin_product = 'VECTOR_SEARCH'.
-- Grain: (endpoint_id, usage_date, workspace_id, sku_name). sku_name is in the grain
-- because a single endpoint can emit compute and storage SKUs on the same day.
-- Note: QPS/latency are not in system tables as of mid-2026; cost only.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost') (
  endpoint_id    STRING         COMMENT 'Vector Search endpoint identifier, from system.billing.usage.usage_metadata.endpoint_id.',
  endpoint_name  STRING         COMMENT 'Human-readable Vector Search endpoint name, from usage_metadata.endpoint_name.',
  usage_date     DATE           COMMENT 'Calendar date of the underlying usage record.',
  workspace_id   BIGINT         COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name STRING         COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  sku_name       STRING         COMMENT 'Billing SKU (e.g. VECTOR_SEARCH_STANDARD, VECTOR_SEARCH_STORAGE_OPTIMIZED).',
  dbus           DECIMAL(38, 6) COMMENT 'Sum of DBUs, from system.billing.usage where billing_origin_product = VECTOR_SEARCH.',
  dollars        DECIMAL(38, 6) COMMENT 'USD cost from dbus via the list_prices cloud + usage_start_time effective-rate join.'
) USING DELTA
  PARTITIONED BY (workspace_id)
  COMMENT 'Billing-only fact for Databricks Vector Search endpoint cost. Stateless full rebuild over :lookback_days. Grain: (endpoint_id, usage_date, workspace_id, sku_name).';

INSERT OVERWRITE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost')
SELECT
  bu.usage_metadata.endpoint_id                                    AS endpoint_id,
  any_value(bu.usage_metadata.endpoint_name)                       AS endpoint_name,
  bu.usage_date                                                    AS usage_date,
  cast(bu.workspace_id AS BIGINT)                                  AS workspace_id,
  any_value(w.workspace_name)                                      AS workspace_name,
  bu.sku_name                                                      AS sku_name,
  cast(sum(bu.usage_quantity) AS DECIMAL(38, 6))                   AS dbus,
  cast(sum(bu.usage_quantity * coalesce(p.pricing.effective_list.default, 0))
       AS DECIMAL(38, 6))                                          AS dollars
FROM system.billing.usage bu
LEFT JOIN system.access.workspaces_latest w
  ON w.workspace_id = cast(bu.workspace_id AS BIGINT)
LEFT JOIN system.billing.list_prices p
  ON  p.sku_name      = bu.sku_name
  AND p.cloud         = bu.cloud
  AND p.currency_code = 'USD'
  AND bu.usage_start_time >= p.price_start_time
  AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
WHERE bu.billing_origin_product = 'VECTOR_SEARCH'
  AND bu.usage_start_time >= date_sub(current_date(), :lookback_days)
-- GROUP BY ALL groups by the non-aggregate columns only (endpoint_id, usage_date,
-- workspace_id, sku_name); endpoint_name/workspace_name are any_value() so they are
-- excluded from the grain. Avoids the alias/source-column ambiguity of naming them.
GROUP BY ALL;

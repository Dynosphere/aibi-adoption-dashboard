-- mvFactServingUsage — unified fact for all model serving activity.
-- Replaces V2 mvFactServingEndpointUsage and mvFactModelUsage.
-- Sources:
--   * system.serving.served_entities   — dimension: endpoint x entity metadata.
--   * system.serving.endpoint_usage    — per-request token counts (Public Preview; requires
--                                        per-endpoint opt-in; many endpoints will yield zeros).
--   * system.billing.usage             — DBU and USD cost rows where
--                                        billing_origin_product = 'MODEL_SERVING'.
--
-- Grain: (endpoint_id, served_entity_id, usage_date, workspace_id).
-- Partition: (workspace_id, entity_type).
-- Stateless full rebuild each run over a bounded :lookback_days window.
-- endpoint_usage opt-in note: customers who have not enabled usage tracking on an endpoint
-- will see request_count = 0, input_tokens = 0, output_tokens = 0 for that endpoint.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage') (
  endpoint_id        STRING    COMMENT 'Serving endpoint identifier from system.serving.served_entities.',
  endpoint_name      STRING    COMMENT 'Human-readable name of the serving endpoint.',
  served_entity_id   STRING    COMMENT 'Identifier of the entity (model + version) deployed on this endpoint, as recorded in system.serving.served_entities.',
  entity_type        STRING    COMMENT 'Category of the served entity: FOUNDATION_MODEL, EXTERNAL_MODEL, CUSTOM_MODEL, or FEATURE_SPEC.',
  entity_name        STRING    COMMENT 'Name of the served model (e.g. databricks-meta-llama-3-3-70b-instruct for foundation models).',
  entity_version     STRING    COMMENT 'Version of the served entity; NULL for foundation and external models that are not versioned.',
  usage_date         DATE      COMMENT 'Calendar date of the underlying usage record.',
  workspace_id       BIGINT    COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name     STRING    COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  request_count      BIGINT    COMMENT 'Total request count from system.serving.endpoint_usage for this (endpoint, entity, date, workspace). Zero when endpoint usage tracking is not enabled.',
  input_tokens       BIGINT    COMMENT 'Sum of input token counts from system.serving.endpoint_usage. Zero when endpoint usage tracking is not enabled.',
  output_tokens      BIGINT    COMMENT 'Sum of output token counts from system.serving.endpoint_usage. Zero when endpoint usage tracking is not enabled.',
  dbus               DECIMAL(38, 6) COMMENT 'DBUs consumed, from system.billing.usage where billing_origin_product = MODEL_SERVING.',
  dollars            DECIMAL(38, 6) COMMENT 'USD cost derived from dbus via the system.billing.list_prices cloud + usage_start_time effective-rate join.'
) USING DELTA
  PARTITIONED BY (workspace_id, entity_type)
  COMMENT 'Unified fact table covering all Databricks model serving activity (custom, foundation, and external models). Sources: system.serving.served_entities (endpoint/entity dimension), system.serving.endpoint_usage (per-request token counts — requires per-endpoint opt-in; missing opt-in yields zeros), and system.billing.usage (DBU + USD cost rows where billing_origin_product = MODEL_SERVING). Grain: (endpoint_id, served_entity_id, usage_date, workspace_id). Stateless full rebuild over :lookback_days.';

INSERT OVERWRITE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
WITH
  -- Aggregate per-request token counts from system.serving.endpoint_usage.
  -- served_entity_id is the join key to the dimension. workspace_id is cast to
  -- BIGINT to align with the fact table schema (source type is string).
  endpoint_usage_agg AS (
    SELECT
      eu.served_entity_id,
      cast(eu.workspace_id AS BIGINT)            AS workspace_id,
      cast(eu.request_time AS DATE)              AS usage_date,
      count(*)                                   AS request_count,
      sum(coalesce(eu.input_token_count,  0))    AS input_tokens,
      sum(coalesce(eu.output_token_count, 0))    AS output_tokens
    FROM system.serving.endpoint_usage eu
    WHERE eu.request_time >= date_sub(current_date(), :lookback_days)
    GROUP BY ALL
  ),

  -- Aggregate billing DBU and USD cost from system.billing.usage for
  -- MODEL_SERVING rows. Join list_prices using the established convention:
  --   sku_name + cloud + currency_code = 'USD' + usage_start_time in [price_start, price_end).
  -- usage_metadata.endpoint_id links to served_entities.endpoint_id.
  billing_agg AS (
    SELECT
      bu.usage_metadata.endpoint_id              AS endpoint_id,
      cast(bu.workspace_id AS BIGINT)            AS workspace_id,
      bu.usage_date                              AS usage_date,
      cast(sum(bu.usage_quantity) AS DECIMAL(38, 6))
                                                 AS dbus,
      cast(sum(bu.usage_quantity * coalesce(p.pricing.effective_list.default, 0))
           AS DECIMAL(38, 6))                    AS dollars
    FROM system.billing.usage bu
    LEFT JOIN system.billing.list_prices p
      ON  p.sku_name      = bu.sku_name
      AND p.cloud         = bu.cloud
      AND p.currency_code = 'USD'
      AND bu.usage_start_time >= p.price_start_time
      AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
    WHERE bu.billing_origin_product = 'MODEL_SERVING'
      AND bu.usage_start_time >= date_sub(current_date(), :lookback_days)
    GROUP BY ALL
  ),

  -- Dimension: all served entities that are active or were active within
  -- the last 30 days (guards against briefly-deleted endpoints disappearing
  -- from the dimension before their billing rows are processed).
  -- workspace_id cast to BIGINT for consistent join types.
  entities AS (
    SELECT
      se.endpoint_id,
      se.endpoint_name,
      se.served_entity_id,
      se.entity_type,
      se.entity_name,
      se.entity_version,
      cast(se.workspace_id AS BIGINT) AS workspace_id
    FROM system.serving.served_entities se
    WHERE se.endpoint_delete_time IS NULL
       OR se.endpoint_delete_time > current_date() - INTERVAL 30 DAYS
  )

SELECT
  e.endpoint_id,
  e.endpoint_name,
  e.served_entity_id,
  e.entity_type,
  e.entity_name,
  e.entity_version,
  coalesce(eu.usage_date, b.usage_date)                            AS usage_date,
  coalesce(eu.workspace_id, b.workspace_id, e.workspace_id)        AS workspace_id,
  w.workspace_name,
  coalesce(eu.request_count, 0)                                    AS request_count,
  coalesce(eu.input_tokens,  0)                                    AS input_tokens,
  coalesce(eu.output_tokens, 0)                                    AS output_tokens,
  coalesce(b.dbus,    cast(0 AS DECIMAL(38, 6)))                   AS dbus,
  coalesce(b.dollars, cast(0 AS DECIMAL(38, 6)))                   AS dollars
FROM entities e
-- endpoint_usage joined on served_entity_id + workspace_id
LEFT JOIN endpoint_usage_agg eu
  ON  eu.served_entity_id = e.served_entity_id
  AND eu.workspace_id     = e.workspace_id
-- billing joined on endpoint_id + workspace_id + usage_date
LEFT JOIN billing_agg b
  ON  b.endpoint_id  = e.endpoint_id
  AND b.workspace_id = coalesce(eu.workspace_id, e.workspace_id)
  AND b.usage_date   = eu.usage_date
LEFT JOIN system.access.workspaces_latest w
  ON  w.workspace_id = coalesce(eu.workspace_id, b.workspace_id, e.workspace_id)
-- Only emit rows where at least one source has a date (avoids phantom rows
-- for endpoints that exist in the dimension but have no usage in this window).
WHERE coalesce(eu.usage_date, b.usage_date) IS NOT NULL;

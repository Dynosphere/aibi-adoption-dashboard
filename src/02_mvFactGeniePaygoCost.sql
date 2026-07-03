-- mvFactGeniePaygoCost — UNION ALL of warehouse-DBU cost (always-on,
-- pre- and post-July-6) and LLM-DBU cost from system.billing.usage
-- (post-July-6, when billing_origin_product IN ('GENIE','AI/BI_GENIE')
-- starts emitting).
--
-- Grain: (cost_source, usage_date, workspace_id, space_id, user_email).
-- Source window: bounded 14-day rolling window — no watermarking needed
-- (symmetric watermark support is a CP4 concern).
-- Idempotent: CREATE TABLE IF NOT EXISTS + MERGE INTO.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGeniePaygoCost') (
  cost_source     STRING    COMMENT 'Origin of the DBU charge: `warehouse` for SQL warehouse DBUs attributed to Genie SQL execution (always-on), or `llm_paygo` for LLM DBUs post-2026-07-06 paygo go-live.',
  usage_date      DATE      COMMENT 'Date of the underlying cost record.',
  workspace_id    BIGINT    COMMENT 'Databricks workspace identifier.',
  workspace_name  STRING    COMMENT 'Resolved workspace name (from system.access.workspaces_latest).',
  space_id        STRING    COMMENT 'Genie space identifier attributed to this cost (query_source_id for warehouse rows; usage_metadata endpoint field for llm_paygo rows until schema is confirmed).',
  space_title     STRING    COMMENT 'Genie space title from adb_genie_spaces (may be NULL for llm_paygo rows).',
  user_email      STRING    COMMENT 'Attributed user.',
  dbus            DECIMAL(38, 6) COMMENT 'Sum of DBUs for this partition.',
  dollars         DECIMAL(38, 6) COMMENT 'Sum of USD cost for this partition (effective list price via system.billing.list_prices).',
  usage_quantity  DECIMAL(38, 6) COMMENT 'Raw usage_quantity from system.billing.usage for llm_paygo rows; NULL for warehouse rows.'
) USING DELTA
  PARTITIONED BY (workspace_id, cost_source)
  COMMENT 'V3 mvFactGeniePaygoCost. UNION ALL of warehouse DBUs attributed to Genie SQL execution (always-on via dbsql_cost_per_query) and LLM DBUs from system.billing.usage where billing_origin_product IN (GENIE, AI/BI_GENIE). Populates from 2026-07-06 Genie paygo go-live. IN (GENIE, AI/BI_GENIE) future-proofs the label rename. Grain: (cost_source, usage_date, workspace_id, space_id, user_email).';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGeniePaygoCost') tgt
USING (
  -- Warehouse-DBU half: SQL warehouse cost attributed to a Genie space.
  -- Always-on, pre- and post-July-6. Source: dbsql_cost_per_query
  -- (populated by CP1), filtered to query_source_type = 'GENIE SPACE'.
  SELECT
    'warehouse'                                                    AS cost_source,
    cast(cpq.start_time AS DATE)                                   AS usage_date,
    cast(cpq.workspace_id AS BIGINT)                               AS workspace_id,
    w.workspace_name                                               AS workspace_name,
    cpq.query_source_id                                            AS space_id,
    sp.name                                                        AS space_title,
    cpq.executed_by                                                AS user_email,
    cast(sum(cpq.query_attributed_dbus_estimation)    AS DECIMAL(38, 6)) AS dbus,
    cast(sum(cpq.query_attributed_dollars_estimation) AS DECIMAL(38, 6)) AS dollars,
    cast(NULL AS DECIMAL(38, 6))                                   AS usage_quantity
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dbsql_cost_per_query_table') cpq
  LEFT JOIN system.access.workspaces_latest w
    ON w.workspace_id = cpq.workspace_id
  LEFT JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_spaces') sp
    ON sp.space_id   = cpq.query_source_id
   AND sp.workspace_id = cpq.workspace_id
  WHERE cpq.query_source_type = 'GENIE SPACE'
    AND cpq.start_time >= current_date() - INTERVAL 14 DAYS
  GROUP BY ALL

  UNION ALL

  -- LLM-DBU half: post-July-6 paygo rows where billing_origin_product
  -- IN ('GENIE','AI/BI_GENIE'). The IN (...) clause future-proofs the
  -- label rename. Pre-July-6 this half is empty; it populates automatically
  -- on go-live without any code change.
  SELECT
    'llm_paygo'                                                    AS cost_source,
    bu.usage_date                                                  AS usage_date,
    cast(bu.workspace_id AS BIGINT)                                AS workspace_id,
    w.workspace_name                                               AS workspace_name,
    -- Genie space attribution: usage_metadata field name is TBC post-go-live;
    -- coalesce over both candidate paths to be tolerant of either schema shape.
    coalesce(
      bu.usage_metadata.ai_gateway_endpoint_name,
      bu.usage_metadata.endpoint_name
    )                                                              AS space_id,
    cast(NULL AS STRING)                                           AS space_title,
    bu.identity_metadata.run_as                                    AS user_email,
    cast(sum(bu.usage_quantity) AS DECIMAL(38, 6))                 AS dbus,
    cast(sum(bu.usage_quantity * coalesce(p.pricing.effective_list.default, 0))
         AS DECIMAL(38, 6))                                        AS dollars,
    cast(sum(bu.usage_quantity) AS DECIMAL(38, 6))                 AS usage_quantity
  FROM system.billing.usage bu
  LEFT JOIN system.access.workspaces_latest w
    ON w.workspace_id = bu.workspace_id
  LEFT JOIN system.billing.list_prices p
    ON  p.sku_name       = bu.sku_name
    AND p.currency_code  = 'USD'
    AND bu.usage_end_time BETWEEN p.price_start_time
                              AND coalesce(p.price_end_time, current_timestamp())
  WHERE bu.billing_origin_product IN ('GENIE', 'AI/BI_GENIE')
    AND bu.usage_date >= current_date() - INTERVAL 14 DAYS
  GROUP BY ALL
) src
ON  tgt.cost_source  = src.cost_source
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
AND coalesce(tgt.space_id,    '') = coalesce(src.space_id,    '')
AND coalesce(tgt.user_email,  '') = coalesce(src.user_email,  '')
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

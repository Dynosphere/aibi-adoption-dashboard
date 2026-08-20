-- mvFactServingUsage — unified fact for all model serving activity.
-- Replaces V2 mvFactServingEndpointUsage and mvFactModelUsage.
-- Sources:
--   * system.billing.usage             — DBU and USD cost where billing_origin_product =
--                                        'MODEL_SERVING'. DRIVES cost (endpoint grain).
--   * system.serving.endpoint_usage    — per-request token counts (requires per-endpoint opt-in;
--                                        endpoints without it simply contribute no usage rows).
--   * system.serving.served_entities   — dimension: endpoint x entity metadata (SCD).
--   * system.access.workspaces_latest  — workspace name resolution.
--   * system.billing.list_prices       — effective list-price rate for the DBU-to-USD join.
--
-- Grain: (endpoint_id, usage_date, workspace_id).  Partition: (workspace_id, entity_type).
-- Stateless full rebuild each run over a bounded :lookback_days window. Deterministic & watermark-ready.
--
-- COST MODEL — endpoint grain (fixes the prior ~96% undercapture):
-- The previous version attached billing via the endpoint_usage date key, so endpoints WITHOUT
-- usage-tracking opt-in (no endpoint_usage rows) lost ALL their cost, and endpoint-grain billing
-- was replicated across an endpoint's served entities (overstating per-entity cost). This version
-- DRIVES cost straight from system.billing.usage at (endpoint_id, usage_date, workspace_id) via a
-- FULL OUTER JOIN with endpoint-aggregated usage, so:
--   * cost is retained for every billed endpoint (dollars/dbus reconcile to system.billing.usage),
--     including endpoints absent from served_entities (deleted / not yet in the dimension);
--   * cost is never replicated across served entities (no double-count on rollup);
--   * served_entities is deduped to the latest config version per served_entity_id (was I2).
-- Per-endpoint request_count / tokens are summed across the endpoint's served entities. entity_*
-- columns describe a single REPRESENTATIVE served entity per endpoint (highest request share, then
-- latest config) so existing per-name dashboard widgets keep working; entity_count flags multiplicity.
-- endpoint_name / entity_name fall back to the billing endpoint_name when the dimension lacks the
-- endpoint, so neither is ever NULL.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage') (
  endpoint_id        STRING    COMMENT 'Serving endpoint identifier (canonical grain key), from system.billing.usage.usage_metadata.endpoint_id / system.serving.served_entities.',
  endpoint_name      STRING    COMMENT 'Human-readable serving endpoint name; from served_entities, falling back to billing usage_metadata.endpoint_name. Never NULL.',
  served_entity_id   STRING    COMMENT 'Identifier of the REPRESENTATIVE served entity for this endpoint (highest request share, then latest config). NULL only when the endpoint is absent from served_entities.',
  entity_type        STRING    COMMENT 'Category of the representative served entity: FOUNDATION_MODEL, EXTERNAL_MODEL, CUSTOM_MODEL, or FEATURE_SPEC.',
  entity_name        STRING    COMMENT 'Name of the representative served model; falls back to the billing endpoint_name when the endpoint is absent from served_entities. Never NULL.',
  entity_version     STRING    COMMENT 'Version of the representative served entity; NULL for foundation/external models that are not versioned.',
  entity_count       INT       COMMENT 'Number of distinct served entities on this endpoint (in the dedup dimension); >1 means entity_* shows only the representative one.',
  usage_date         DATE      COMMENT 'Calendar date of the underlying usage/cost record.',
  workspace_id       BIGINT    COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name     STRING    COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  request_count      BIGINT    COMMENT 'Total request count from system.serving.endpoint_usage for this (endpoint, date, workspace), summed across served entities. Zero when usage tracking is not enabled.',
  input_tokens       BIGINT    COMMENT 'Sum of input token counts from system.serving.endpoint_usage. Zero when usage tracking is not enabled.',
  output_tokens      BIGINT    COMMENT 'Sum of output token counts from system.serving.endpoint_usage. Zero when usage tracking is not enabled.',
  dbus               DECIMAL(38, 6) COMMENT 'DBUs consumed, from system.billing.usage where billing_origin_product = MODEL_SERVING, at endpoint grain. Reconciles to billing.',
  dollars            DECIMAL(38, 6) COMMENT 'USD cost derived from dbus via the system.billing.list_prices cloud + usage_start_time effective-rate join. Reconciles to billing.'
) USING DELTA
  PARTITIONED BY (workspace_id, entity_type)
  COMMENT 'Unified fact for Databricks model serving activity (custom, foundation, external). Endpoint grain (endpoint_id, usage_date, workspace_id). Cost driven from system.billing.usage (MODEL_SERVING) so it reconciles to billing; per-endpoint request/token counts from system.serving.endpoint_usage; entity_* describe a representative served entity from a deduped system.serving.served_entities. Stateless full rebuild over :lookback_days.';

INSERT OVERWRITE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
WITH
  -- served_entities deduped to the latest config version per served_entity_id (fixes I2 SCD dup).
  -- 30-day delete guard keeps recently-deleted endpoints until their billing rows are processed.
  entities_latest AS (
    SELECT endpoint_id, endpoint_name, served_entity_id, entity_type, entity_name, entity_version, workspace_id
    FROM (
      SELECT
        se.endpoint_id, se.endpoint_name, se.served_entity_id, se.entity_type, se.entity_name,
        se.entity_version, cast(se.workspace_id AS BIGINT) AS workspace_id,
        row_number() OVER (PARTITION BY se.served_entity_id
                           ORDER BY se.endpoint_config_version DESC, se.change_time DESC) AS rn
      FROM system.serving.served_entities se
      WHERE se.endpoint_delete_time IS NULL
         OR se.endpoint_delete_time > date_sub(current_date(), 30)
    )
    WHERE rn = 1
  ),

  -- Per-request token counts from endpoint_usage, resolved to endpoint_id via the deduped dimension.
  usage_by_entity AS (
    SELECT
      el.endpoint_id,
      cast(eu.workspace_id AS BIGINT)          AS workspace_id,
      eu.served_entity_id,
      cast(eu.request_time AS DATE)            AS usage_date,
      count(*)                                 AS request_count,
      sum(coalesce(eu.input_token_count,  0))  AS input_tokens,
      sum(coalesce(eu.output_token_count, 0))  AS output_tokens
    FROM system.serving.endpoint_usage eu
    JOIN entities_latest el
      ON el.served_entity_id = eu.served_entity_id
    WHERE eu.request_time >= date_sub(current_date(), :lookback_days)
    GROUP BY 1, 2, 3, 4
  ),

  -- Endpoint-grain usage (sum across served entities).
  usage_by_endpoint AS (
    SELECT endpoint_id, workspace_id, usage_date,
           sum(request_count) AS request_count,
           sum(input_tokens)  AS input_tokens,
           sum(output_tokens) AS output_tokens
    FROM usage_by_entity
    GROUP BY 1, 2, 3
  ),

  -- Endpoint-grain cost, driven straight from billing (independent of usage-tracking opt-in).
  billing_agg AS (
    SELECT
      bu.usage_metadata.endpoint_id            AS endpoint_id,
      cast(bu.workspace_id AS BIGINT)          AS workspace_id,
      bu.usage_date                            AS usage_date,
      max(bu.usage_metadata.endpoint_name)     AS endpoint_name_billing,
      cast(sum(bu.usage_quantity) AS DECIMAL(38, 6))                                        AS dbus,
      cast(sum(bu.usage_quantity * coalesce(p.pricing.effective_list.default, 0)) AS DECIMAL(38, 6)) AS dollars
    FROM system.billing.usage bu
    LEFT JOIN system.billing.list_prices p
      ON  p.sku_name      = bu.sku_name
      AND p.cloud         = bu.cloud
      AND p.currency_code = 'USD'
      AND bu.usage_start_time >= p.price_start_time
      AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
    WHERE bu.billing_origin_product = 'MODEL_SERVING'
      AND bu.usage_metadata.endpoint_id IS NOT NULL
      AND bu.usage_start_time >= date_sub(current_date(), :lookback_days)
    GROUP BY bu.usage_metadata.endpoint_id, cast(bu.workspace_id AS BIGINT), bu.usage_date
  ),

  -- Representative served entity per (endpoint, workspace): highest request share in-window, then
  -- latest config, tie-broken by served_entity_id. Also carries entity_count for multiplicity.
  endpoint_dim AS (
    SELECT endpoint_id, workspace_id, endpoint_name, served_entity_id, entity_type, entity_name,
           entity_version, entity_count
    FROM (
      SELECT
        el.endpoint_id, el.workspace_id, el.endpoint_name, el.served_entity_id, el.entity_type,
        el.entity_name, el.entity_version,
        count(*)      OVER (PARTITION BY el.endpoint_id, el.workspace_id) AS entity_count,
        row_number()  OVER (PARTITION BY el.endpoint_id, el.workspace_id
                            ORDER BY coalesce(ur.request_count, 0) DESC, el.served_entity_id) AS rn
      FROM entities_latest el
      LEFT JOIN (
        SELECT endpoint_id, workspace_id, served_entity_id, sum(request_count) AS request_count
        FROM usage_by_entity GROUP BY 1, 2, 3
      ) ur
        ON  ur.endpoint_id      = el.endpoint_id
        AND ur.workspace_id     = el.workspace_id
        AND ur.served_entity_id = el.served_entity_id
    )
    WHERE rn = 1
  )

SELECT
  coalesce(b.endpoint_id, u.endpoint_id)                          AS endpoint_id,
  coalesce(d.endpoint_name, b.endpoint_name_billing)             AS endpoint_name,
  d.served_entity_id                                             AS served_entity_id,
  d.entity_type                                                  AS entity_type,
  coalesce(d.entity_name, b.endpoint_name_billing)              AS entity_name,
  d.entity_version                                               AS entity_version,
  coalesce(d.entity_count, 0)                                    AS entity_count,
  coalesce(u.usage_date, b.usage_date)                           AS usage_date,
  coalesce(b.workspace_id, u.workspace_id)                       AS workspace_id,
  w.workspace_name                                               AS workspace_name,
  coalesce(u.request_count, 0)                                   AS request_count,
  coalesce(u.input_tokens,  0)                                   AS input_tokens,
  coalesce(u.output_tokens, 0)                                   AS output_tokens,
  coalesce(b.dbus,    cast(0 AS DECIMAL(38, 6)))                 AS dbus,
  coalesce(b.dollars, cast(0 AS DECIMAL(38, 6)))                 AS dollars
FROM billing_agg b
FULL OUTER JOIN usage_by_endpoint u
  ON  u.endpoint_id  = b.endpoint_id
  AND u.usage_date   = b.usage_date
  AND u.workspace_id = b.workspace_id
LEFT JOIN endpoint_dim d
  ON  d.endpoint_id  = coalesce(b.endpoint_id, u.endpoint_id)
  AND d.workspace_id = coalesce(b.workspace_id, u.workspace_id)
LEFT JOIN system.access.workspaces_latest w
  ON  w.workspace_id = coalesce(b.workspace_id, u.workspace_id)
WHERE coalesce(u.usage_date, b.usage_date) IS NOT NULL;

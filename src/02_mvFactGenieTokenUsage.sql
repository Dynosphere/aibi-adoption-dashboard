-- Genie LLM token-usage cost (the pay-as-you-go dimension introduced 2026-07-08).
--
-- This is a SEPARATE cost stream from dbsql_cost_per_query (warehouse/SQL cost).
-- Genie's LLM token billing lands in system.billing.usage with
-- billing_origin_product = 'GENIE' and usage_type = 'TOKEN' (measured in DBUs).
--
-- KEY: the genie product, channel and (for Agents) the space are carried in the
-- nested struct usage_metadata.genie = struct<surface, channel, agent_id>:
--   * surface  -> the Genie product: GENIE_CODE | GENIE_ONE | GENIE_AGENTS
--   * channel  -> UI | API
--   * agent_id -> the Genie space_id, populated ONLY for GENIE_AGENTS
--                 (NULL for Code / One, which are not space-scoped)
-- This means token cost CAN be split by product exactly, and for Genie Agents
-- attributed to an individual space (agent_id joins 1:1 to adb_genie_spaces).
-- Code / One token cost is per-user only (no space concept).
--
-- Other attribution keys: workspace_id, identity_metadata.run_as (user),
-- sku_name ('GENIE_FREE_USAGE' = free-tier ledger, unpriced $0; everything else
-- is a regional ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_* paid SKU).
-- Free-tier DBUs are $0 but tracked for headroom monitoring.
--
-- Grain: workspace x user x surface x channel x agent_id x tier x sku x day.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieTokenUsage')
PARTITIONED BY (usage_date, workspace_id)
comment 'Genie LLM token usage & cost (billing_origin_product=GENIE, usage_type=TOKEN, unit=DBU). Split by product surface (GENIE_CODE / GENIE_ONE / GENIE_AGENTS) via usage_metadata.genie.surface, by channel (UI/API), and for Agents by space via usage_metadata.genie.agent_id. Separate stream from warehouse cost (dbsql_cost_per_query).'
AS
WITH prices AS (
  SELECT
    sku_name,
    usage_unit,
    price_start_time,
    COALESCE(price_end_time, date_add(current_date(), 1)) AS coalesced_price_end_time,
    pricing.effective_list.default AS unit_price_usd
  FROM system.billing.list_prices
  WHERE currency_code = 'USD'
),
genie_tokens AS (
  SELECT
    u.workspace_id,
    u.identity_metadata.run_as                       AS user_email,
    -- Product surface: GENIE_CODE / GENIE_ONE / GENIE_AGENTS
    u.usage_metadata.genie.surface                   AS genie_surface,
    -- UI vs API
    u.usage_metadata.genie.channel                   AS genie_channel,
    -- The Genie space id (Agents only; NULL for Code / One)
    u.usage_metadata.genie.agent_id                  AS agent_id,
    u.product_features.genie.offering_type           AS genie_offering,
    CASE WHEN u.sku_name = 'GENIE_FREE_USAGE' THEN 'FREE' ELSE 'PAID' END AS tier,
    u.sku_name,
    -- Region parsed from the SRTI SKU (e.g. ..._US_EAST_N_VIRGINIA); free SKU has none.
    CASE
      WHEN u.sku_name = 'GENIE_FREE_USAGE' THEN NULL
      ELSE regexp_replace(u.sku_name, '^ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_', '')
    END                                               AS cloud_region,
    u.usage_date,
    u.usage_end_time,
    u.usage_quantity,
    u.usage_unit
  FROM system.billing.usage AS u
  WHERE u.billing_origin_product = 'GENIE'
    AND u.usage_type = 'TOKEN'
    AND u.record_type = 'ORIGINAL'
)
SELECT
  t.workspace_id,
  t.user_email,
  t.genie_surface,
  t.genie_channel,
  t.agent_id,
  -- Space name for Agents (NULL for Code / One). Left join so token rows are
  -- never dropped if a space isn't in the local catalogue.
  s.name                                                         AS space_name,
  t.genie_offering,
  t.tier,
  t.sku_name,
  t.cloud_region,
  t.usage_date,
  SUM(t.usage_quantity)                                          AS token_dbus,
  -- Free-tier DBUs are $0; paid DBUs are priced from list_prices.
  SUM(t.usage_quantity * COALESCE(p.unit_price_usd, 0))          AS token_cost_usd,
  COUNT(*)                                                       AS usage_records
FROM genie_tokens t
LEFT JOIN prices p
  ON t.sku_name = p.sku_name
  AND t.usage_unit = p.usage_unit
  AND t.usage_end_time BETWEEN p.price_start_time AND p.coalesced_price_end_time
LEFT JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_spaces') s
  ON t.agent_id = s.space_id
GROUP BY
  t.workspace_id, t.user_email, t.genie_surface, t.genie_channel, t.agent_id,
  s.name, t.genie_offering, t.tier, t.sku_name, t.cloud_region, t.usage_date;

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_cost_analysis_main_table') comment   'This table provides a granular cost attribution analysis for Databricks Genie spaces. It links individual SQL queries executed within Genie to the underlying SQL Warehouse compute costs. By calculating the "work proportion" of every query relative to the warehouse''s total activity, it estimates the specific DBU consumption and USD cost for each query statement.'
AS
WITH date_range AS (
  -- Determine the analysis window: from the earliest Genie message to today.
  SELECT 
    MIN(created_timestamp) AS start_date,
    CURRENT_DATE() AS end_date
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_messages')
),
warehouse_usage AS (
  -- Aggregate total DBU and dollar costs for each SQL warehouse in the date range.
  SELECT
    u.usage_metadata.warehouse_id,
    SUM(u.usage_quantity) AS total_billed_dbus, -- Total DBUs billed for the warehouse
    SUM(u.usage_quantity * p.pricing.default) AS total_billed_dollars -- Total cost in dollars
  FROM system.billing.usage u
  JOIN system.billing.list_prices p 
    ON u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_start_time < p.price_end_time OR p.price_end_time IS NULL)
  CROSS JOIN date_range dr
  WHERE u.usage_start_time >= dr.start_date -- Only usage within the Genie message window
    AND u.usage_end_time <= dr.end_date
    AND u.usage_unit = 'DBU'                -- Only DBU usage (compute)
    AND u.sku_name ILIKE '%SQL%'            -- Only SQL compute SKUs
  GROUP BY 1
),
query_base AS (
  -- Gather all Genie-related queries and compute their execution durations.
  SELECT
    statement_id, -- Unique query statement ID
    executed_by,
    statement_text,
    compute.warehouse_id,
    query_source.genie_space_id,
    start_time,
    (COALESCE(compilation_duration_ms, 0) + COALESCE(execution_duration_ms, 0)) AS total_accurate_duration_ms,
    SUM(COALESCE(compilation_duration_ms, 0) + COALESCE(execution_duration_ms, 0)) OVER (PARTITION BY compute.warehouse_id) AS total_warehouse_activity_ms
  FROM system.query.history
  CROSS JOIN date_range dr
  WHERE start_time >= dr.start_date -- Only queries within the Genie message window
    AND start_time <= dr.end_date
    AND compute.warehouse_id IS NOT NULL
),
allocated_metrics AS (
  -- Allocate warehouse costs to each query based on its share of total warehouse activity.
  SELECT
    q.statement_id,
    q.executed_by,
    q.statement_text,
    q.genie_space_id,
    q.start_time,
    q.warehouse_id,
    q.total_accurate_duration_ms,
    (q.total_accurate_duration_ms / NULLIF(q.total_warehouse_activity_ms, 0)) AS work_proportion,
    u.total_billed_dbus,
    u.total_billed_dollars
  FROM query_base q
  LEFT JOIN warehouse_usage u ON q.warehouse_id = u.warehouse_id -- Include queries even if billing data is missing
)
SELECT 
  statement_id,
  executed_by AS user_email,
  start_time,
  genie_space_id,
  warehouse_id,
  ROUND(total_accurate_duration_ms / 1000, 2) AS accurate_duration_seconds,
  ROUND(work_proportion * total_billed_dbus, 4) AS dbus_consumed,
  ROUND(work_proportion * total_billed_dollars, 4) AS cost_usd,
  statement_text AS sql_code
FROM allocated_metrics
WHERE genie_space_id is NOT NULL
ORDER BY start_time DESC
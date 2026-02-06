CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_cost_analysis_enhanced') 
COMMENT 'Granular cost attribution for Databricks Genie spaces, accounting for idle time and hourly fluctuations.'
AS
WITH 
-- 1. DEFINE BOUNDARIES: Auto-detect the time range based on available billing/query history
table_boundaries AS (
  SELECT 
    date_trunc('HOUR', LEAST(
      (SELECT MAX(event_time) FROM system.compute.warehouse_events),
      (SELECT MAX(end_time) FROM system.query.history),
      (SELECT MAX(usage_end_time) FROM system.billing.usage)
    )) AS selected_end_time,
    (date_trunc('HOUR', GREATEST(
      (SELECT MIN(event_time) FROM system.compute.warehouse_events),
      (SELECT MIN(start_time) FROM system.query.history),
      (SELECT MIN(usage_end_time) FROM system.billing.usage)
    )) + INTERVAL 1 HOUR)::timestamp AS selected_start_time
),

-- 2. GET HOURLY WAREHOUSE BILLING (The "Cost to distribute")
cpq_warehouse_usage AS (
  SELECT
    usage_metadata.warehouse_id AS warehouse_id,
    u.*
  FROM system.billing.usage AS u
  WHERE usage_metadata.warehouse_id IS NOT NULL
    AND usage_start_time >= (SELECT MIN(selected_start_time) FROM table_boundaries)
    AND usage_end_time <= (SELECT MAX(selected_end_time) FROM table_boundaries)
),

prices AS (
  SELECT coalesce(price_end_time, date_add(current_date, 1)) as coalesced_price_end_time, *
  FROM system.billing.list_prices
  WHERE currency_code = 'USD'
),

filtered_warehouse_usage AS (
    SELECT 
      u.warehouse_id,
      date_trunc('HOUR', u.usage_start_time) AS usage_start_hour,
      u.usage_quantity AS dbus,
      (CAST(p.pricing.effective_list.default AS FLOAT) * u.usage_quantity) AS usage_dollars
    FROM cpq_warehouse_usage AS u
    LEFT JOIN prices as p
      ON u.sku_name = p.sku_name
      AND u.usage_unit = p.usage_unit
      AND (u.usage_end_time BETWEEN p.price_start_time AND p.coalesced_price_end_time)
),

-- 3. GET QUERY HISTORY (We need ALL queries, not just Genie, to calculate the denominator correctly)
cpq_warehouse_query_history AS (
  SELECT
    statement_id,
    executed_by,
    statement_text,
    compute.warehouse_id AS warehouse_id,
    -- Calculate precise execution time excluding metadata overhead
    (COALESCE(CAST(total_task_duration_ms AS FLOAT) / 1000, 0) +
      COALESCE(CAST(result_fetch_duration_ms AS FLOAT) / 1000, 0) +
      COALESCE(CAST(compilation_duration_ms AS FLOAT) / 1000, 0)
    ) AS query_work_task_time,
    start_time,
    end_time,
    -- Normalize start/end times for calculation
    timestampadd(MILLISECOND , coalesce(waiting_at_capacity_duration_ms, 0) + coalesce(waiting_for_compute_duration_ms, 0) + coalesce(compilation_duration_ms, 0), start_time) AS query_work_start_time,
    timestampadd(MILLISECOND, coalesce(result_fetch_duration_ms, 0), end_time) AS query_work_end_time,
    -- Identify Genie Source
    CASE
      WHEN query_source.genie_space_id IS NOT NULL THEN 'GENIE SPACE'
      ELSE 'OTHER'
    END AS query_source_type,
    query_source.genie_space_id
  FROM system.query.history AS h
  WHERE statement_type IS NOT NULL
    AND start_time < (SELECT selected_end_time FROM table_boundaries)
    AND end_time > (SELECT selected_start_time FROM table_boundaries)
    AND total_task_duration_ms > 0
    AND compute.warehouse_id IS NOT NULL
),

-- 4. SPLIT QUERIES ACROSS HOURLY BUCKETS (Handling long-running queries)
hour_intervals AS (
  SELECT
    statement_id,
    warehouse_id,
    query_work_start_time,
    query_work_end_time,
    query_work_task_time,
    explode(
      sequence(
        0,
        floor((UNIX_TIMESTAMP(query_work_end_time) - UNIX_TIMESTAMP(date_trunc('hour', query_work_start_time))) / 3600)
      )
    ) AS hours_interval,
    timestampadd(hour, hours_interval, date_trunc('hour', query_work_start_time)) AS hour_bucket
  FROM cpq_warehouse_query_history
),

statement_proportioned_work AS (
    SELECT * , 
        GREATEST(0,
          UNIX_TIMESTAMP(LEAST(query_work_end_time, timestampadd(hour, 1, hour_bucket))) -
          UNIX_TIMESTAMP(GREATEST(query_work_start_time, hour_bucket))
        ) AS overlap_duration,
        CASE WHEN CAST(query_work_end_time AS DOUBLE) - CAST(query_work_start_time AS DOUBLE) = 0
        THEN 0
        ELSE query_work_task_time * (overlap_duration / (CAST(query_work_end_time AS DOUBLE) - CAST(query_work_start_time AS DOUBLE)))
        END AS proportional_query_work
    FROM hour_intervals
),

attributed_query_work_all AS (
    SELECT
      statement_id,
      hour_bucket,
      warehouse_id,
      SUM(proportional_query_work) AS attributed_query_work
    FROM statement_proportioned_work
    GROUP BY statement_id, warehouse_id, hour_bucket
),

-- 5. CALCULATE TOTAL WORK PER WAREHOUSE/HOUR
warehouse_time as (
  select
    warehouse_id,
    hour_bucket,
    SUM(attributed_query_work) as total_work_done_on_warehouse
  from attributed_query_work_all
  group by warehouse_id, hour_bucket
),

-- 6. ATTRIBUTE COSTS (Proportion of Work * Cost of Warehouse Hour)
history_with_pricing AS (
  SELECT
    a.statement_id,
    a.warehouse_id,
    a.hour_bucket,
    a.attributed_query_work,
    b.total_work_done_on_warehouse,
    wh.dbus AS total_warehouse_period_dbus,
    wh.usage_dollars AS total_warehouse_period_dollars,
    -- Logic: If I did 10% of the work, I pay 10% of the total bill (including the idle time inherent in the bill)
    CASE 
      WHEN b.total_work_done_on_warehouse = 0 THEN 0 
      ELSE a.attributed_query_work / b.total_work_done_on_warehouse 
    END AS query_task_time_proportion
  FROM attributed_query_work_all a
  INNER JOIN warehouse_time b ON a.warehouse_id = b.warehouse_id AND a.hour_bucket = b.hour_bucket
  LEFT JOIN filtered_warehouse_usage AS wh ON a.warehouse_id = wh.warehouse_id AND a.hour_bucket = wh.usage_start_hour
),

final_attribution AS (
  SELECT
    statement_id,
    (query_task_time_proportion * total_warehouse_period_dollars) AS query_attributed_dollars,
    (query_task_time_proportion * total_warehouse_period_dbus) AS query_attributed_dbus
  FROM history_with_pricing
)

-- 7. FINAL FILTER FOR GENIE OUTPUT
SELECT 
  q.genie_space_id,
  q.executed_by AS user_email,
  q.statement_id,
  q.start_time,
  q.warehouse_id,
  q.statement_text AS sql_code,
  SUM(fa.query_attributed_dbus) AS total_dbus_consumed,
  SUM(fa.query_attributed_dollars) AS total_cost_usd,
  MAX(q.query_work_task_time) as execution_duration_seconds
FROM cpq_warehouse_query_history q
JOIN final_attribution fa ON q.statement_id = fa.statement_id
WHERE q.query_source_type = 'GENIE SPACE' -- CRITICAL: Filter only for Genie here at the end
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY start_time DESC;
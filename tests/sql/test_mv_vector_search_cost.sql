-- tests/sql/test_mv_vector_search_cost.sql — runs post-deploy against the warehouse.
-- Grain uniqueness: no duplicate (endpoint_id, usage_date, workspace_id, sku_name).
SELECT assert_true(count(*) = 0, 'mvFactVectorSearchCost grain must be unique') AS grain_ok
FROM (
  SELECT endpoint_id, usage_date, workspace_id, sku_name, count(*) c
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost')
  GROUP BY 1,2,3,4 HAVING count(*) > 1
);

-- No negative cost.
SELECT assert_true(count(*) = 0, 'dbus/dollars must be non-negative') AS nonneg_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost')
WHERE dbus < 0 OR dollars < 0;

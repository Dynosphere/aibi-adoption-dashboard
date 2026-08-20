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

-- endpoint_name must never be NULL: some VECTOR_SEARCH billing lines (index-maintenance
-- ENTERPRISE_JOBS_SERVERLESS_COMPUTE SKUs) carry real cost but no endpoint_id/endpoint_name.
-- These are kept (totals must still reconcile to billing) and labelled as unattributed
-- rather than surfacing as a NULL bucket in the dashboard.
SELECT assert_true(count(*) = 0, 'endpoint_name must never be NULL (unattributed rows labelled)') AS name_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactVectorSearchCost')
WHERE endpoint_name IS NULL;

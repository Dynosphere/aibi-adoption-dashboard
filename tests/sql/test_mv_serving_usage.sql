-- tests/sql/test_mv_serving_usage.sql — runs post-deploy.
SELECT assert_true(count(*) = 0, 'mvFactServingUsage grain must be unique') AS grain_ok
FROM (
  SELECT endpoint_id, served_entity_id, usage_date, workspace_id, count(*) c
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
  GROUP BY 1,2,3,4 HAVING count(*) > 1
);
SELECT assert_true(count(*) = 0, 'token/cost columns must be non-negative') AS nonneg_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingUsage')
WHERE request_count < 0 OR input_tokens < 0 OR output_tokens < 0 OR dbus < 0 OR dollars < 0;

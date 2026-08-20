-- tests/sql/test_mv_app_usage.sql — runs post-deploy.
SELECT assert_true(count(*) = 0, 'mvFactAppUsage grain must be unique') AS grain_ok
FROM (
  SELECT app_id, usage_date, workspace_id, count(*) c
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage')
  GROUP BY 1,2,3 HAVING count(*) > 1
);
SELECT assert_true(count(*) = 0, 'app cost/counts must be non-negative') AS nonneg_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage')
WHERE dbus < 0 OR dollars < 0 OR lifecycle_events < 0 OR distinct_users < 0;

-- app_name must never be NULL: audit rows carry only the app name (no app_id) under
-- action-specific keys, and billing carries app_name on every row, so the resolved
-- app_name is always available. A NULL here means the old billing<->audit split
-- (the dashboard "Top apps" NULL bucket) has regressed.
SELECT assert_true(count(*) = 0, 'app_name must never be NULL (no split/NULL bucket)') AS name_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage')
WHERE app_name IS NULL;

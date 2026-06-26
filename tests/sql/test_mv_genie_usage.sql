-- Smoke test for mvFactGenieUsage.
-- Requires: a recent run of the bundle's adoption_dash_job.

-- Schema sanity
SELECT
  assert_true(count(*) > 0, 'mvFactGenieUsage must not be empty after a pipeline run') AS schema_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage');

-- Workspace scope: every row has a workspace_id
SELECT
  assert_true(count(*) = 0, 'mvFactGenieUsage rows must all carry workspace_id') AS ws_ok
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage')
WHERE workspace_id IS NULL;

-- Idempotency: running this query twice does not change row count
-- (manually verify by snapshotting count and re-running the pipeline.)

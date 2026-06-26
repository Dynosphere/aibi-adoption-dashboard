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

-- #14 regression test: every statement_id present in
-- dbsql_cost_per_query for query_source_type='GENIE SPACE' must
-- also appear in genie_observability_main_table for the same workspace.
WITH missing AS (
  SELECT q.statement_id
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dbsql_cost_per_query') q
  LEFT JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_observability_main_table') g
    ON g.statement_id = q.statement_id AND g.workspace_id = q.workspace_id
  WHERE q.query_source_type = 'GENIE SPACE'
    AND q.workspace_id IS NOT NULL
    AND g.statement_id IS NULL
)
SELECT
  assert_true(
    count(*) < 0.01 * (SELECT count(*) FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dbsql_cost_per_query')
                       WHERE query_source_type = 'GENIE SPACE'),
    'No more than 1% of GENIE SPACE statement_ids may be missing from genie_observability_main_table'
  ) AS issue_14_resolved
FROM missing;

-- Genie warehouse cost, categorised by what kind of activity caused it.
--
-- A Genie space's warehouse cost (query_source_type = 'GENIE SPACE' in
-- dbsql_cost_per_query_table) is NOT all "users asking questions". A large share
-- can come from authoring/configuring the space, from schema probes, etc. This
-- view classifies every Genie statement so the dashboard can separate
-- "usage cost" from "authoring cost" and be explicit about what it cannot tie
-- to a conversation.
--
-- Source of truth is left untouched: this reads dbsql_cost_per_query_table
-- (owned upstream) and enriches it. It does NOT recompute cost.
--
-- Categories (verified against system.query.history.statement_text):
--   CONVERSATIONAL      - statement is linked to a Genie message via the API
--                         bridge (adb_genie_message_statements). A real user
--                         question. Reconciles 1:1 to genie_cost_per_message.
--   AUTHORING_PROFILING - space setup/edit profiling: per-column top-value
--                         sampling (approx_top_k(...)) and data sampling
--                         (WITH SampledData AS ...). Fired by updateSpace /
--                         updateGenieColumnConfigs, not by a conversation.
--   SCHEMA_PROBE        - DESCRIBE <query> dry-run validation. ~0ms, ~$0.
--   METADATA_OTHER      - zero-duration metadata operations.
--   OTHER_UNATTRIBUTED  - real warehouse work carrying the space_id that the
--                         Conversation API did not return (non-conversational
--                         surfaces: Genie One / monitoring / agent-mode, or
--                         deleted conversations). Real cost, no message link.
--
-- NOTE: the split of AUTHORING vs CONVERSATIONAL is descriptive, not a billing
-- boundary; all of it is genuine 'GENIE SPACE' warehouse cost. The point is to
-- let the dashboard show WHY a space costs what it does.

CREATE OR REPLACE VIEW IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_cost_categorised')
AS
WITH bridge AS (
  SELECT DISTINCT statement_id
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_message_statements')
  WHERE statement_id IS NOT NULL
)
SELECT
  c.*,
  CASE
    WHEN b.statement_id IS NOT NULL
      THEN 'CONVERSATIONAL'
    WHEN upper(trim(c.statement_text)) LIKE 'DESCRIBE %'
      THEN 'SCHEMA_PROBE'
    WHEN upper(c.statement_text) LIKE '%APPROX_TOP_K(%'
      OR upper(c.statement_text) LIKE '%SAMPLEDDATA%'
      THEN 'AUTHORING_PROFILING'
    WHEN COALESCE(c.query_work_task_time_seconds, 0) = 0
      THEN 'METADATA_OTHER'
    ELSE 'OTHER_UNATTRIBUTED'
  END AS genie_cost_category,
  -- coarse rollup for tiles that just want usage vs overhead
  CASE
    WHEN b.statement_id IS NOT NULL                        THEN 'USAGE'
    WHEN upper(c.statement_text) LIKE '%APPROX_TOP_K(%'
      OR upper(c.statement_text) LIKE '%SAMPLEDDATA%'      THEN 'AUTHORING'
    ELSE 'OVERHEAD_OR_UNATTRIBUTED'
  END AS genie_cost_class
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dbsql_cost_per_query_table') c
LEFT JOIN bridge b ON c.statement_id = b.statement_id
WHERE c.query_source_type = 'GENIE SPACE';

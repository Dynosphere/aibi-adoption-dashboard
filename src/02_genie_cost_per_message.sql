-- Genie cost per user question (message grain).
--
-- Root cause this fixes (GitHub issue #14): a single Genie message runs MULTIPLE
-- SQL statements (exploratory queries in chat mode; more in agent mode). The old
-- genie_observability_main_table pipe-joined a message's statement_ids into one
-- scalar string, so joining on statement_id (equality) silently dropped every
-- multi-statement message. adb_genie_message_statements is the normalised
-- statement-grain bridge (one row per executed statement); we join it 1:1 to
-- dbsql_cost_per_query_table on statement_id and roll up to the message.
--
-- Grain: one row per Genie message (user question). message_cost_usd = sum of
-- the warehouse cost of every SQL statement that message executed.
--
-- Note: this is WAREHOUSE/SQL cost only. LLM token cost is a separate stream
-- (mvFactGenieTokenUsage) that has no statement_id and cannot join here.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_cost_per_message_table')
comment 'One row per Genie message (user question) with total attributed WAREHOUSE cost = sum of all SQL statements the message executed. Built by joining the statement bridge (adb_genie_message_statements) 1:1 to dbsql_cost_per_query_table on statement_id, then rolling up to message_id. Fixes issue #14. Token cost is separate (mvFactGenieTokenUsage).'
AS
WITH statement_cost AS (
  -- Statement-grain: bridge joined to the per-statement cost engine (1:1 on statement_id).
  SELECT
    s.space_id,
    s.space_name,
    s.conversation_id,
    s.message_id,
    s.statement_id,
    s.user_email,
    c.query_attributed_dollars_estimation AS statement_cost_usd,
    c.query_attributed_dbus_estimation    AS statement_dbus,
    CASE WHEN c.statement_id IS NOT NULL THEN 1 ELSE 0 END AS has_cost_record
  FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_message_statements') s
  LEFT JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.dbsql_cost_per_query_table') c
    ON s.statement_id = c.statement_id
)
SELECT
  sc.space_id,
  sc.space_name,
  sc.conversation_id,
  sc.message_id,
  MAX(m.user_question)                             AS user_question,
  MAX(sc.user_email)                               AS user_email,
  MAX(m.created_timestamp)                         AS created_datetime,
  MAX(m.feedback_rating)                           AS feedback_rating,
  COUNT(sc.statement_id)                           AS num_statements,
  SUM(1 - sc.has_cost_record)                      AS num_statements_missing_cost,
  SUM(COALESCE(sc.statement_cost_usd, 0))          AS message_cost_usd,
  SUM(COALESCE(sc.statement_dbus, 0))              AS message_dbus
FROM statement_cost sc
LEFT JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_messages') m
  ON sc.message_id = m.message_id
GROUP BY sc.space_id, sc.space_name, sc.conversation_id, sc.message_id
ORDER BY message_cost_usd DESC;

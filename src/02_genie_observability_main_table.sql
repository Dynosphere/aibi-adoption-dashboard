CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_observability_main_table') comment 'Comprehensive observability table for Genie AI/BI spaces. Contains detailed message-level data including user questions, AI responses, generated SQL queries, execution metadata, user feedback ratings, and error information. Used for monitoring Genie usage, analyzing query patterns, and tracking user engagement across all accessible Genie spaces.'
AS
SELECT 
  s.space_id,
  s.name AS space_name,
  m.message_id,
  m.conversation_id,
  CAST(m.user_id AS STRING) AS user_id,
  u.user_name AS user_email,
  m.status,
  m.created_timestamp,
  m.last_updated_timestamp,
  m.content AS user_question,
  NULL AS ai_response,
  NULL AS sql_query,
  m.query_id AS statement_id,
  NULL AS suggested_questions,
  m.num_attachments,
  NULL AS feedback_rating,
  m.error_message AS error_type,
  m.error_message
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_messages') m
INNER JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_spaces') s
  ON m.space_id = s.space_id
INNER JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_conversations') c
  ON m.conversation_id = c.conversation_id
  AND m.space_id = c.space_id
LEFT JOIN system.access.users u
  ON CAST(m.user_id AS BIGINT) = u.id
ORDER BY m.created_timestamp DESC
CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_observability_main_table') comment 'Comprehensive observability table for Genie AI/BI spaces. Contains detailed message-level data including user questions, AI responses, generated SQL queries, execution metadata, user feedback ratings, and error information. Used for monitoring Genie usage, analyzing query patterns, and tracking user engagement across all accessible Genie spaces.'
AS
SELECT 
  m.space_id,
  m.space_name,
  m.message_id,
  m.conversation_id,
  m.user_id,
  m.user_email,
  m.status,
  m.created_timestamp AS created_datetime,
  m.last_updated_timestamp,
  m.user_question,
  m.ai_response,
  m.sql_query,
  m.statement_id,
  m.suggested_questions,
  m.num_attachments,
  m.feedback_rating,
  m.error_type,
  m.error_message
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_messages') m
ORDER BY m.created_timestamp DESC
-- genie_observability_main_table — incremental MERGE on audit + query.history.
-- Replaces the V2 SDK-derived view that suffered from statement_id mismatches (#14).
-- Source: system.access.audit (service_name IN ('aibiGenie','genieChat')),
-- joined to system.query.history on genie_space_id within a ±5 minute temporal window.
-- Window: rebuilds the last 7 days every run; older rows are stable history.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_observability_main_table') (
  space_id          STRING  COMMENT 'Genie space identifier (coalesces request_params.space_id and request_params.spaceId).',
  space_title       STRING  COMMENT 'Title of the Genie space at ingest time (from adb_genie_spaces).',
  conversation_id   STRING  COMMENT 'Conversation identifier from request_params.conversation_id.',
  message_id        STRING  COMMENT 'Message identifier from request_params.message_id.',
  statement_id      STRING  COMMENT 'SQL warehouse statement_id from system.query.history, joined within ±5 min of the audit event. Resolves issue #14 (V2 SDK-attachment join silently dropped statement_ids).',
  user_email        STRING  COMMENT 'Email of the user who triggered the event (from user_identity.email).',
  created_datetime  TIMESTAMP COMMENT 'Timestamp of the audit event (event_time).',
  workspace_id      BIGINT  COMMENT 'Databricks workspace identifier.',
  workspace_name    STRING  COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  surface           STRING  COMMENT '''agents'' for Genie Agents (service_name=''aibiGenie'') or ''chat'' for Genie One (service_name=''genieChat'').',
  action_name       STRING  COMMENT 'Audit action_name (e.g. genieStartConversationMessage, updateConversationMessageFeedback).',
  feedback_rating   STRING  COMMENT 'Value of request_params.feedback_rating where present.'
) USING DELTA
  PARTITIONED BY (workspace_id, surface)
  COMMENT 'V3 genie_observability_main_table. Incremental Delta table populated via MERGE from system.access.audit (service_name IN aibiGenie/genieChat) joined to system.query.history.genie_space_id within ±5 minutes. Grain: (space_id, message_id, workspace_id, surface, action_name). Source window: last 7 days. Closes #14.';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_observability_main_table') tgt
USING (
  WITH events AS (
    SELECT
      coalesce(request_params.space_id, request_params.spaceId)  AS space_id,
      request_params.conversation_id                              AS conversation_id,
      request_params.message_id                                   AS message_id,
      user_identity.email                                         AS user_email,
      event_time                                                  AS created_datetime,
      workspace_id,
      CASE service_name
        WHEN 'aibiGenie' THEN 'agents'
        WHEN 'genieChat' THEN 'chat'
      END                                                         AS surface,
      action_name,
      request_params.feedback_rating                              AS feedback_rating
    FROM system.access.audit
    WHERE service_name IN ('aibiGenie', 'genieChat')
      AND event_date >= current_date() - INTERVAL 7 DAYS
      AND request_params.message_id IS NOT NULL
  ),
  with_stmt AS (
    -- Join message-level events to executed SQL via system.query.history.genie_space_id
    -- + a temporal window (message-emit ± 5 minutes). This is the join that V2 got
    -- wrong (it relied on attachments.statement_id from the SDK, which silently drops
    -- statement IDs in some attachments).
    SELECT
      e.*,
      q.statement_id
    FROM events e
    LEFT JOIN system.query.history q
      ON q.genie_space_id = e.space_id
     AND q.workspace_id   = e.workspace_id
     AND q.statement_start_time BETWEEN e.created_datetime - INTERVAL 5 MINUTES
                                    AND e.created_datetime + INTERVAL 5 MINUTES
  ),
  with_space AS (
    SELECT s.*, sp.title AS space_title, w.workspace_name
    FROM with_stmt s
    LEFT JOIN IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_spaces') sp
      ON sp.space_id     = s.space_id
     AND sp.workspace_id = s.workspace_id
    LEFT JOIN system.access.workspaces_latest w
      ON w.workspace_id  = s.workspace_id
  )
  SELECT * FROM with_space
) src
ON  tgt.space_id      = src.space_id
AND tgt.message_id    = src.message_id
AND tgt.workspace_id  = src.workspace_id
AND tgt.surface       = src.surface
AND tgt.action_name   = src.action_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

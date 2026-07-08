-- mvFactGenieFeedback — incremental MERGE on system.access.audit.
-- Source: audit rows where service_name IN ('aibiGenie', 'genieChat')
-- AND action_name is one of the 5 user-feedback events.
-- Window: watermark-driven incremental read.
--   lower bound = watermark_ts from dim_pipeline_watermarks (or TIMESTAMP '2024-01-01 00:00:00' on first run)
--   upper bound = current_timestamp() - INTERVAL 15 MINUTES (audit ingestion lag safety)
-- After the fact MERGE, a second MERGE advances the watermark per (source_name, workspace_id)
-- to MAX(event_time) observed in the source window — never further than the upper bound.
--
-- workspace_id resolution: keyed from audit rows directly (choice G — no bundle parameter
-- needed). Works for both single-workspace and shared-catalog multi-workspace deployments.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieFeedback') (
  space_id         STRING  COMMENT 'Genie space identifier (coalesces request_params.space_id and request_params.spaceId).',
  space_title      STRING  COMMENT 'Title of the Genie space at ingest time (from adb_genie_spaces; may be NULL if space not yet in dim).',
  conversation_id  STRING  COMMENT 'Conversation identifier from request_params.conversation_id.',
  message_id       STRING  COMMENT 'Message identifier from request_params.message_id.',
  action_name      STRING  COMMENT 'Audit action_name for the feedback event (one of 5 feedback action types).',
  comment_type     STRING  COMMENT 'Comment type from request_params.comment_type (populated for comment events; NULL for rating events).',
  feedback_rating  STRING  COMMENT 'Feedback rating from request_params.feedback_rating (populated for rating events; NULL for comment events).',
  user_email       STRING  COMMENT 'Email address of the user who submitted feedback (from user_identity.email).',
  event_time       TIMESTAMP COMMENT 'Timestamp of the audit event.',
  workspace_id     BIGINT  COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name   STRING  COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  surface          STRING  COMMENT '`agents` for Genie Agents (service_name=''aibiGenie'') or `chat` for Genie One Chat (service_name=''genieChat'').'
) USING DELTA
  PARTITIONED BY (workspace_id, surface)
  COMMENT 'V3 mvFactGenieFeedback. Incremental Delta table populated via MERGE from system.access.audit. Captures user feedback events (thumbs up/down, comments) for both Genie Agents (aibiGenie) and Genie One Chat (genieChat). Grain: (space_id, message_id, workspace_id, action_name). Source window: watermark-driven — reads event_time > last watermark up to NOW()-15min; bootstrap epoch is 2024-01-01.';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieFeedback') tgt
USING (
  WITH watermark AS (
    -- Per-workspace watermarks for this source. Left-join against audit so
    -- workspaces that have never been processed get NULL (→ epoch bootstrap).
    -- Fixes SCALAR_SUBQUERY_TOO_MANY_ROWS in shared-catalog deployments where
    -- multiple workspaces write watermarks for the same source_name.
    SELECT workspace_id, watermark_ts
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
    WHERE source_name = 'mvFactGenieFeedback'
  ),
  audit AS (
    SELECT
      coalesce(a.request_params.space_id, a.request_params.spaceId)  AS space_id,
      a.request_params.conversation_id                               AS conversation_id,
      a.request_params.message_id                                    AS message_id,
      a.action_name,
      a.request_params.comment_type                                  AS comment_type,
      a.request_params.feedback_rating                               AS feedback_rating,
      a.user_identity.email                                          AS user_email,
      a.event_time,
      a.workspace_id,
      CASE a.service_name
        WHEN 'aibiGenie' THEN 'agents'
        WHEN 'genieChat' THEN 'chat'
      END AS surface
    FROM system.access.audit a
    LEFT JOIN watermark w
      ON w.workspace_id = a.workspace_id
    WHERE a.service_name IN ('aibiGenie', 'genieChat')
      AND a.action_name IN (
        'updateConversationMessageFeedback',
        'createConversationMessageComment',
        'updateConversationMessageComment',
        'genieSendMessageFeedback',
        'updateGenieChatConversationFeedback'
      )
      -- Watermark lower bound: scan the partition that contains the watermark date
      -- (INTERVAL 1 DAY overhang guards against day-boundary clock skew).
      AND a.event_date >= date(coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')) - INTERVAL 1 DAY
      -- Watermark lower bound on the precise timestamp column, per workspace.
      AND a.event_time > coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
      -- Watermark upper bound: exclude rows that may not yet be fully ingested.
      AND a.event_time <= current_timestamp() - INTERVAL 15 MINUTES
  ),
  spaces AS (
    SELECT space_id, name AS space_title, workspace_id
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_spaces')
  ),
  ws AS (
    SELECT workspace_id, workspace_name FROM system.access.workspaces_latest
  )
  SELECT
    a.space_id,
    s.space_title,
    a.conversation_id,
    a.message_id,
    a.action_name,
    a.comment_type,
    a.feedback_rating,
    a.user_email,
    a.event_time,
    a.workspace_id,
    w.workspace_name,
    a.surface
  FROM audit a
  LEFT JOIN spaces s ON s.space_id = a.space_id AND s.workspace_id = a.workspace_id
  LEFT JOIN ws w     ON w.workspace_id = a.workspace_id
) src
ON  tgt.space_id     = src.space_id
AND tgt.message_id   = src.message_id
AND tgt.workspace_id = src.workspace_id
AND tgt.action_name  = src.action_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Advance the watermark to MAX(event_time) actually processed in the window above,
-- grouped per workspace_id so that shared-catalog / multi-workspace deployments each
-- get their own watermark row.  We take MAX(event_time) capped at the upper bound we
-- used for the fact MERGE — this avoids advancing the watermark past data we didn't read.
-- On first run, no row exists → WHEN NOT MATCHED inserts the initial watermark.
MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks') tgt
USING (
  WITH watermark AS (
    SELECT workspace_id, watermark_ts
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
    WHERE source_name = 'mvFactGenieFeedback'
  )
  SELECT
    'mvFactGenieFeedback'                                        AS source_name,
    a.workspace_id,
    least(
      max(a.event_time),
      current_timestamp() - INTERVAL 15 MINUTES
    )                                                            AS watermark_ts,
    current_timestamp()                                          AS updated_at
  FROM system.access.audit a
  LEFT JOIN watermark w ON w.workspace_id = a.workspace_id
  WHERE a.service_name IN ('aibiGenie', 'genieChat')
    AND a.action_name IN (
      'updateConversationMessageFeedback',
      'createConversationMessageComment',
      'updateConversationMessageComment',
      'genieSendMessageFeedback',
      'updateGenieChatConversationFeedback'
    )
    AND a.event_date >= date(coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')) - INTERVAL 1 DAY
    AND a.event_time > coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
    AND a.event_time <= current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY a.workspace_id
) src
ON  tgt.source_name  = src.source_name
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET
  watermark_ts = greatest(tgt.watermark_ts, src.watermark_ts),
  updated_at   = src.updated_at
WHEN NOT MATCHED THEN INSERT *;

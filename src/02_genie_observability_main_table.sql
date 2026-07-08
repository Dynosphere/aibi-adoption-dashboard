-- genie_observability_main_table — incremental MERGE on audit + query.history.
-- Replaces the V2 SDK-derived view that suffered from statement_id mismatches (#14).
-- Source: system.access.audit (service_name IN ('aibiGenie','genieChat')),
-- joined to system.query.history on genie_space_id within a ±5 minute temporal window.
-- Window: watermark-driven incremental read.
--   lower bound = watermark_ts from dim_pipeline_watermarks (or TIMESTAMP '2024-01-01 00:00:00' on first run)
--   upper bound = current_timestamp() - INTERVAL 15 MINUTES (audit ingestion lag safety)
-- After the fact MERGE, a second MERGE advances the watermark per (source_name, workspace_id)
-- to MAX(event_time) observed in the source window — never further than the upper bound.
--
-- workspace_id resolution: keyed from audit rows directly (choice G — no bundle parameter
-- needed). Works for both single-workspace and shared-catalog multi-workspace deployments.

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
  COMMENT 'V3 genie_observability_main_table. Incremental Delta table populated via MERGE from system.access.audit (service_name IN aibiGenie/genieChat) joined to system.query.history.genie_space_id within ±5 minutes. Grain: (space_id, message_id, workspace_id, surface, action_name). Source window: watermark-driven — reads event_time > last watermark up to NOW()-15min; bootstrap epoch is 2024-01-01. Closes #14.';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_observability_main_table') tgt
USING (
  WITH watermark AS (
    -- Per-workspace watermarks for this source. Left-joined into audit so a
    -- workspace with no prior watermark row falls back to the epoch. Fixes
    -- SCALAR_SUBQUERY_TOO_MANY_ROWS on shared-catalog deployments where
    -- multiple workspaces write watermarks for the same source_name.
    SELECT workspace_id, watermark_ts
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
    WHERE source_name = 'genie_observability_main_table'
  ),
  events AS (
    SELECT
      coalesce(a.request_params.space_id, a.request_params.spaceId) AS space_id,
      a.request_params.conversation_id                              AS conversation_id,
      a.request_params.message_id                                   AS message_id,
      a.user_identity.email                                         AS user_email,
      a.event_time                                                  AS created_datetime,
      a.workspace_id,
      CASE a.service_name
        WHEN 'aibiGenie' THEN 'agents'
        WHEN 'genieChat' THEN 'chat'
      END                                                           AS surface,
      a.action_name,
      a.request_params.feedback_rating                              AS feedback_rating
    FROM system.access.audit a
    LEFT JOIN watermark w
      ON w.workspace_id = a.workspace_id
    WHERE a.service_name IN ('aibiGenie', 'genieChat')
      AND a.event_date >= date(coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')) - INTERVAL 1 DAY
      AND a.event_time > coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
      AND a.event_time <= current_timestamp() - INTERVAL 15 MINUTES
      AND a.request_params.message_id IS NOT NULL
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
      ON q.query_source.genie_space_id = e.space_id
     AND q.workspace_id                = e.workspace_id
     AND q.start_time BETWEEN e.created_datetime - INTERVAL 5 MINUTES
                          AND e.created_datetime + INTERVAL 5 MINUTES
  ),
  with_space AS (
    SELECT s.*, sp.name AS space_title, w.workspace_name
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
    WHERE source_name = 'genie_observability_main_table'
  )
  SELECT
    'genie_observability_main_table'                             AS source_name,
    a.workspace_id,
    least(
      max(a.event_time),
      current_timestamp() - INTERVAL 15 MINUTES
    )                                                            AS watermark_ts,
    current_timestamp()                                          AS updated_at
  FROM system.access.audit a
  LEFT JOIN watermark w ON w.workspace_id = a.workspace_id
  WHERE a.service_name IN ('aibiGenie', 'genieChat')
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

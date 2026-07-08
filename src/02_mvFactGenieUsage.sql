-- mvFactGenieUsage — incremental MERGE on system.access.audit.
-- Source: audit rows where service_name IN ('aibiGenie','genieChat'),
-- aggregated to (space_id, usage_date, workspace_id, user_email) grain.
-- Window: watermark-driven incremental read.
--   lower bound = watermark_ts from dim_pipeline_watermarks (or TIMESTAMP '2024-01-01 00:00:00' on first run)
--   upper bound = current_timestamp() - INTERVAL 15 MINUTES (audit ingestion lag safety)
-- After the fact MERGE, a second MERGE advances the watermark per (source_name, workspace_id)
-- to MAX(event_time) observed in the source window — never further than the upper bound.
--
-- workspace_id resolution: keyed from audit rows directly (choice G — no bundle parameter
-- needed). Works for both single-workspace and shared-catalog multi-workspace deployments.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage') (
  space_id          STRING COMMENT 'Genie space identifier (coalesces request_params.space_id and request_params.spaceId).',
  space_title       STRING COMMENT 'Title of the Genie space at ingest time (from adb_genie_spaces).',
  usage_date        DATE COMMENT 'Calendar date of the underlying audit events.',
  workspace_id      BIGINT COMMENT 'Databricks workspace identifier; matches system.access.workspaces_latest.workspace_id.',
  workspace_name    STRING COMMENT 'Workspace name resolved from system.access.workspaces_latest.',
  user_email        STRING COMMENT 'Email of the user who triggered the event (from user_identity.email).',
  message_count     BIGINT COMMENT 'Count of audit events for this (space, date, workspace, user, surface).',
  conversation_count BIGINT COMMENT 'Distinct conversations the user participated in for this (space, date, workspace, surface).',
  distinct_user_surfaces BIGINT COMMENT 'Count of unique (user_email, surface) pairs in this (space_id, usage_date, workspace_id) partition. NOT a count of unique users — a single user using both Genie Agents and Genie One Chat counts as 2.',
  surface           STRING COMMENT '`agents` for Genie Agents (service_name=''aibiGenie'') or `chat` for Genie One (service_name=''genieChat'').'
) USING DELTA
  PARTITIONED BY (workspace_id, surface)
  COMMENT 'V3 mvFactGenieUsage. Incremental Delta table populated via MERGE from system.access.audit (service_name IN aibiGenie/genieChat). Grain: (space_id, usage_date, workspace_id, user_email, surface). Source window: watermark-driven — reads event_time > last watermark up to NOW()-15min; bootstrap epoch is 2024-01-01.';

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage') tgt
USING (
  WITH watermark AS (
    -- Per-workspace watermarks for this source. Left-join against audit so
    -- workspaces that have never been processed get NULL (→ epoch bootstrap).
    -- Fixes SCALAR_SUBQUERY_TOO_MANY_ROWS in shared-catalog deployments where
    -- multiple workspaces write watermarks for the same source_name.
    SELECT workspace_id, watermark_ts
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks')
    WHERE source_name = 'mvFactGenieUsage'
  ),
  audit AS (
    SELECT
      coalesce(a.request_params.space_id, a.request_params.spaceId)  AS space_id,
      a.event_date                                                    AS usage_date,
      a.workspace_id,
      a.user_identity.email                                           AS user_email,
      a.action_name,
      a.request_params.conversation_id                                AS conversation_id,
      CASE a.service_name
        WHEN 'aibiGenie' THEN 'agents'
        WHEN 'genieChat' THEN 'chat'
      END AS surface
    FROM system.access.audit a
    LEFT JOIN watermark w
      ON w.workspace_id = a.workspace_id
    WHERE a.service_name IN ('aibiGenie', 'genieChat')
      -- Watermark lower bound: scan the partition that contains the watermark date
      -- (INTERVAL 1 DAY overhang guards against day-boundary clock skew).
      AND a.event_date >= date(coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')) - INTERVAL 1 DAY
      -- Watermark lower bound on the precise timestamp column, per workspace.
      AND a.event_time > coalesce(w.watermark_ts, TIMESTAMP '2024-01-01 00:00:00')
      -- Watermark upper bound: exclude rows that may not yet be fully ingested.
      AND a.event_time <= current_timestamp() - INTERVAL 15 MINUTES
      AND coalesce(a.request_params.space_id, a.request_params.spaceId) IS NOT NULL
  ),
  spaces AS (
    SELECT space_id, name AS space_title, workspace_id
    FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_spaces')
  ),
  ws AS (
    SELECT workspace_id, workspace_name FROM system.access.workspaces_latest
  ),
  agg AS (
    SELECT
      a.space_id,
      a.usage_date,
      a.workspace_id,
      a.user_email,
      a.surface,
      count(*)                              AS message_count,
      count(DISTINCT a.conversation_id)     AS conversation_count
    FROM audit a
    GROUP BY ALL
  )
  SELECT
    g.space_id,
    s.space_title,
    g.usage_date,
    g.workspace_id,
    w.workspace_name,
    g.user_email,
    g.message_count,
    g.conversation_count,
    -- distinct_user_surfaces: count of unique (user, surface) pairs per (space, date, workspace)
    count(g.user_email) OVER (PARTITION BY g.space_id, g.usage_date, g.workspace_id) AS distinct_user_surfaces,
    g.surface
  FROM agg g
  LEFT JOIN spaces s ON s.space_id = g.space_id AND s.workspace_id = g.workspace_id
  LEFT JOIN ws w     ON w.workspace_id = g.workspace_id
) src
ON tgt.space_id      = src.space_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
AND tgt.user_email   = src.user_email
AND tgt.surface      = src.surface
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
    WHERE source_name = 'mvFactGenieUsage'
  )
  SELECT
    'mvFactGenieUsage'                                           AS source_name,
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

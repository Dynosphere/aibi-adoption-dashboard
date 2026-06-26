-- mvFactGenieUsage — incremental MERGE on system.access.audit.
-- Source: audit rows where service_name IN ('aibiGenie','genieChat'),
-- aggregated to (space_id, usage_date, workspace_id, user_email) grain.
-- Window: rebuilds the last 7 days every run; older rows are stable history.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage') (
  space_id          STRING,
  space_title       STRING,
  usage_date        DATE,
  workspace_id      BIGINT,
  workspace_name    STRING,
  user_email        STRING,
  message_count     BIGINT,
  conversation_count BIGINT,
  distinct_users    BIGINT,
  surface           STRING  -- 'agents' (aibiGenie) or 'chat' (genieChat)
) USING DELTA
  PARTITIONED BY (workspace_id, surface);

MERGE INTO IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage') tgt
USING (
  WITH audit AS (
    SELECT
      coalesce(request_params.space_id, request_params.spaceId)        AS space_id,
      event_date                                                        AS usage_date,
      workspace_id,
      user_identity.email                                               AS user_email,
      action_name,
      request_params.conversation_id                                    AS conversation_id,
      CASE service_name
        WHEN 'aibiGenie' THEN 'agents'
        WHEN 'genieChat' THEN 'chat'
      END AS surface
    FROM system.access.audit
    WHERE service_name IN ('aibiGenie', 'genieChat')
      AND event_date >= current_date() - INTERVAL 7 DAYS
      AND request_params.space_id IS NOT NULL
  ),
  spaces AS (
    SELECT space_id, title AS space_title, workspace_id
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
    -- distinct_users is per (space, date) — computed via a window after the aggregate
    count(g.user_email) OVER (PARTITION BY g.space_id, g.usage_date, g.workspace_id) AS distinct_users,
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

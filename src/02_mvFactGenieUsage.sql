-- Genie REACH signal — from system.access.audit ONLY.
--
-- Scope note (why this table is deliberately narrow): audit is the only source
-- for "reach" — a user OPENING/viewing a Genie space, whether or not they went
-- on to ask a question. Everything about DEPTH (questions asked, ratings,
-- comments, generated SQL, cost) comes from the medallion tables
-- (adb_genie_messages + adb_genie_message_statements/comments), NOT here, so we
-- don't double-source the same metric from two lineages.
--
-- Grain: space x workspace x day. Metrics kept are audit-only:
--   * space_views       -> getSpace / genieGetSpace  (a space was opened)
--   * distinct_viewers   -> distinct users who opened the space
--   * spaces_created     -> genieCreateSpace
-- Chats, messages, feedback counts are intentionally NOT here — get those from
-- the medallion (adb_genie_conversations / adb_genie_messages / feedback_rating).
--
-- NOTE: action_name values verified against live audit (2026-08). The old V2
-- version filtered on 'createSpace'/'createConversation' which never match the
-- real 'genie*'-prefixed action names, so those counts were always 0/wrong.

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactGenieUsage')
comment 'Genie REACH metrics from system.access.audit only (space views/opens, distinct viewers, spaces created), at space x workspace x day grain. Depth metrics (questions, ratings, comments, cost) come from the medallion tables, not here.'
AS
SELECT
  g.space_id,
  g.name                                             AS genie_space,
  au.workspace_id,
  w.workspace_name,
  au.event_date                                      AS activity_date,
  COUNT_IF(au.action_name IN ('getSpace', 'genieGetSpace'))              AS space_views,
  COUNT(DISTINCT CASE WHEN au.action_name IN ('getSpace','genieGetSpace')
                      THEN au.user_identity.email END)                    AS distinct_viewers,
  COUNT_IF(au.action_name = 'genieCreateSpace')                          AS spaces_created
FROM IDENTIFIER(:catalog_name || '.' || :schema_name || '.adb_genie_spaces') g
LEFT JOIN system.access.audit au
  ON au.request_params.space_id = g.space_id
LEFT JOIN system.access.workspaces_latest w
  ON au.workspace_id = w.workspace_id
WHERE au.service_name = 'aibiGenie'
  AND au.event_time > now() - INTERVAL 180 DAY
GROUP BY g.space_id, g.name, au.workspace_id, w.workspace_name, au.event_date;

-- V3 one-time housekeeping: drop V2 tables that are retired or renamed in this release.
-- Safe to run on any deploy; DROP TABLE IF EXISTS is a no-op after the first run.
-- This task runs early in the workflow (depends on Ingest_Metadata so the schema exists)
-- and before any CP4 fact tasks so the legacy table definitions cannot interfere with
-- the new CREATE TABLE IF NOT EXISTS statements.

-- mvFactModelUsage measured UC model-registry browsing events (getModel, getModelVersion),
-- not actual inference consumption. Dropped entirely in V3 per architecture decision 4a:
-- discovery-event counting is out of scope for an adoption consumption dashboard.
DROP TABLE IF EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactModelUsage');

-- mvFactServingEndpointUsage is superseded by mvFactServingUsage (this task).
-- The V2 table used a CREATE OR REPLACE with a different schema (audit-based,
-- no token or cost columns) so the V3 CREATE TABLE IF NOT EXISTS would be a no-op
-- if the old table were left in place. Drop it here so the V3 table is created fresh.
DROP TABLE IF EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactServingEndpointUsage');

-- mvFactAppUsage V2 used CREATE OR REPLACE TABLE ... AS SELECT with completely different
-- columns (app_id, workspace_id, workspace_name, num_deploys, num_gets, num_updates,
-- num_starts, num_stops, num_users, event_date) — audit-only, no billing cost, different
-- grain and shape. The V3 schema adds dbus, dollars, lifecycle_events, distinct_users and
-- changes the grain to (app_id, usage_date, workspace_id). Without this DROP, the V3
-- CREATE TABLE IF NOT EXISTS would be a no-op and the MERGE would fail on missing columns.
DROP TABLE IF EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.mvFactAppUsage');

-- genie_observability_main_table V2 was SDK-message-derived (user_question, ai_response,
-- sql_query, attachments.statement_id, …) and is the root cause of #14. V3 rebuilds it from
-- system.access.audit + system.query.history.genie_space_id with a different schema
-- (workspace_id, surface, action_name, space_title; no SDK message body columns). Without
-- this DROP, CREATE TABLE IF NOT EXISTS is a no-op and the V3 MERGE fails / never lands.
DROP TABLE IF EXISTS IDENTIFIER(:catalog_name || '.' || :schema_name || '.genie_observability_main_table');

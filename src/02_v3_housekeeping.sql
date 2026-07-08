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

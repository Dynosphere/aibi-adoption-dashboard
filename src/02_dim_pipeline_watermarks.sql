-- dim_pipeline_watermarks — one row per (source_name, workspace_id).
-- Tracks the highest event_time successfully processed per fact source so that
-- subsequent pipeline runs read only NEW audit rows rather than rescanning the
-- full 7-day (or 365-day) window on every invocation.
--
-- workspace_id resolution strategy (choice G — no extra parameter required):
--   Watermark rows are keyed by workspace_id sourced directly from the
--   system.access.audit rows being processed. Because each audit row already
--   carries workspace_id, the watermark MERGE groups by workspace_id from the
--   data itself. This handles both single-workspace deployments (one row per
--   source) and multi-workspace shared-catalog deployments (one row per
--   source × workspace) without any bundle variable or subquery against
--   system.access.workspaces_latest.
--
-- Bootstrap behaviour:
--   First run: no watermark row exists → consuming queries COALESCE to
--   TIMESTAMP '2024-01-01 00:00:00' and process all history from that date.
--   The watermark MERGE (at the end of each MV SQL file) inserts on first
--   run and updates on subsequent runs. The table therefore auto-populates;
--   no manual seed step is needed.
--
-- Idempotency guarantee:
--   If the fact MERGE succeeds but the watermark update fails, the next run
--   re-processes the same window — safe because the fact MERGE uses a natural
--   key that makes re-inserts idempotent.

CREATE TABLE IF NOT EXISTS
  IDENTIFIER(:catalog_name || '.' || :schema_name || '.dim_pipeline_watermarks') (
    source_name   STRING    NOT NULL COMMENT 'Logical name of the fact table reading from a system source (e.g. mvFactGenieUsage).',
    workspace_id  BIGINT    NOT NULL COMMENT 'Workspace identifier — allows multi-workspace deployments sharing a single catalog to maintain per-workspace watermarks.',
    watermark_ts  TIMESTAMP NOT NULL COMMENT 'Highest event_time successfully processed for this (source_name, workspace_id) pair. Next run reads only event_time > watermark_ts.',
    updated_at    TIMESTAMP NOT NULL COMMENT 'Wall-clock timestamp when this watermark row was last written (current_timestamp() at MERGE time).'
  ) USING DELTA
  COMMENT 'V3 dim_pipeline_watermarks. One row per (source_name, workspace_id). Records the highest event_time processed per fact source per workspace so that pipeline runs are incremental rather than full-rescans. Bootstrap: on first run for a given source/workspace the consuming query COALESCEs the missing watermark to TIMESTAMP ''2024-01-01 00:00:00''. Auto-populated by the watermark MERGE appended to each CP2 MV SQL file.';

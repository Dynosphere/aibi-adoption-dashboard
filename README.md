# Databricks Adoption Dashboard

The **Databricks Adoption Dashboard** provides an **out-of-the-box capability** to analyse and visualise adoption metrics across your organisation’s **AI/BI Dashboards, Genie Spaces, Apps, and Models**.  
It helps data teams, business stakeholders, and platform owners quickly understand how Databricks is being used, where adoption is growing, and where additional enablement might be needed.

## Key Features
- 📊 **Unified View** – Track usage and adoption trends across dashboards, Genie spaces, AI apps, and machine-learning models in one place.  
- ⚡ **Plug-and-Play** – Pre-built notebooks, data models, and visualisations to get started with minimal setup.  
- 🔍 **Drill-Down Analysis** – Drill into specific dashboards and genie spaces (coming soon) to view granular details


## Quick Start
1. Create a git folder in your Databricks workspace OR download the zip and import to your workspace
2. You need to update the variables for your parameters. There are two approaches here:
    1. If you're deploying into one workspace, we recommend updating `/deployment_resources/variables.yml` defaults directly with your `warehouse_id`, `catalog_name` and `schema_name`.
    2. If you're deploying to many workspaces, you should set these values as variable overrides during deployment as part of your CICD process. See [this doc link](https://docs.databricks.com/aws/en/dev-tools/bundles/variables#set-a-variables-value)
3. Deploy the Asset Bundle
4. Run the adoption_dashboard_workflow for the first time to populate your data.
5. Set the adoption_dashboard_workflow job to run as needed for your frequency.
6. You're now good to go!

Tip: Works seamlessly with Unity Catalog and Databricks SQL Warehouses.

⸻

## Genie cost attribution: what reconciles and what doesn't

Genie cost splits into **two independent dimensions** with different grains and
sources of truth. They **cannot be combined at query grain** and should not be
expected to sum to one number.

### 1. Warehouse / SQL compute cost
- **Source of truth:** `dbsql_cost_per_query_table`, filtered to
  `query_source_type = 'GENIE SPACE'`. This carries `statement_id` and
  `query_source_id` (= the Genie `space_id`).
- **Space-level cost** is a simple, complete aggregation:
  ```sql
  SELECT query_source_id AS space_id, SUM(query_attributed_dollars_estimation)
  FROM dbsql_cost_per_query_table
  WHERE query_source_type = 'GENIE SPACE' AND workspace_id = <this workspace>
  GROUP BY query_source_id
  ```
- **Per-question cost** (`genie_cost_per_message_table`) is derived by joining
  the API-sourced statement bridge (`adb_genie_message_statements`) to
  `dbsql_cost_per_query_table` **on `statement_id`**, then rolling up to
  `message_id`. The driver is the bridge (LEFT side); cost is the RIGHT side.

  **Important — the per-message rollup is an API-covered *subset*, not the space
  total.** The Genie Conversation API does not return every statement Genie
  executes, so per-message cost can be materially lower than the space cost from
  `dbsql_cost_per_query_table`. Use `dbsql_cost_per_query_table` for totals and
  the per-message table only for drill-down. Example reconciliation from a test
  workspace (per space):

  | Space | Space cost (truth) | Σ per-message cost | Reconciles? |
  |---|---|---|---|
  | Space A | $1.1032 | $1.1032 | ✅ exact |
  | Space B | $0.0416 | $0.0416 | ✅ exact |
  | Space C | $1.05 | $0.0936 | ❌ only 13 of 114 statements were conversational |

  When the API returns a space's conversations completely, the rollup reconciles
  to the penny (Spaces A and B). Gaps are **not** a cost-logic bug — they come
  from warehouse statements that were never part of a listable conversation, so
  the Conversation API cannot return them. See the next section for the dominant
  cause. `DESCRIBE`/schema-probe statements are ~0ms and contribute negligible
  cost, so they do not explain reconciliation gaps.

### Space authoring / profiling cost — a large, non-conversational cost category

**A Genie space incurs significant warehouse cost when it is *created or edited*,
entirely separate from users asking questions — and this cost is invisible to
any conversation-based view.** When you set up or update a space (columns,
instructions, sample values, example/curated questions, dashboard), Genie runs
profiling SQL against the underlying tables to build value indexes and grounding
metadata. These statements:

- carry the space's `genie_space_id`, so they **do** appear in
  `dbsql_cost_per_query_table` as `'GENIE SPACE'` cost;
- are **not** conversations, so they never appear in `list_conversations` /
  `adb_genie_message_statements` and **cannot** be rolled up to a message;
- show up in `system.access.audit` as `updateSpace`, `genieUpdateSpace`,
  `updateGenieColumnConfigs` (service `aibiGenie`), not as message actions.

The profiling queries are recognisable in `system.query.history.statement_text`
as per-column top-value sampling, e.g.:

```sql
SELECT item.item AS value
FROM (SELECT explode(approx_top_k(`hotel_name`, 1024)) AS item FROM (...));
-- one per column: hotel_code, department, source_currency, brand, city, ...
WITH SampledData AS (SELECT `hotel_code`, `hotel_name`, `business_date`, ... )
```

**Worked example (Space C above).** All 114 statements carry the space's
`genie_space_id`. They break down as:

| Category | Statements | In Conversation API? | Cost impact |
|---|---|---|---|
| Conversational (real user questions) | 13 | ✅ yes | the $0.09 that reconciles |
| Space authoring / profiling (`approx_top_k`, `SampledData`) | 65 | ❌ no | the bulk of the $1.05 |
| `DESCRIBE` schema probes | 30 | ❌ no | ~0ms, ~$0 |
| Other zero-duration metadata ops | 6 | ❌ no | ~$0 |

So ~91% of Space C's warehouse cost came from **authoring the space**, not from
users querying it. Note this cannot be split into "Genie Code created it" vs
"edited in the space UI" from the system tables: both paths log the same
`updateSpace` / `updateGenieColumnConfigs` actions under `client_application =
'Databricks SQL Genie Space'`. The signal is "space was authored/configured",
not which tool drove it.

**Implication for dashboards:** treat **authoring/profiling cost** and **usage
(conversational) cost** as distinct. A newly built or heavily-edited space can
look "expensive" while having almost no user traffic. Report space totals from
`dbsql_cost_per_query_table`; use the per-message rollup only for the
conversational slice.

### The `genie_cost_categorised` view — cost by activity type

To make this queryable, `src/02_genie_cost_categorised.sql` builds the
`genie_cost_categorised` view: every `'GENIE SPACE'` row from
`dbsql_cost_per_query_table` enriched with a `genie_cost_category` (and a coarser
`genie_cost_class`). It does **not** recompute cost — it only labels it, and the
categorised total equals the raw space total exactly.

| `genie_cost_category` | Meaning | `genie_cost_class` |
|---|---|---|
| `CONVERSATIONAL` | Linked to a Genie message (real user question) | `USAGE` |
| `AUTHORING_PROFILING` | Space setup/edit: `approx_top_k(...)` column profiling + `SampledData` | `AUTHORING` |
| `SCHEMA_PROBE` | `DESCRIBE` dry-run validation (~0ms, ~$0) | `OVERHEAD_OR_UNATTRIBUTED` |
| `METADATA_OTHER` | Zero-duration metadata ops | `OVERHEAD_OR_UNATTRIBUTED` |
| `OTHER_UNATTRIBUTED` | Real space cost the Conversation API didn't return (Genie One / monitoring / agent-mode / deleted convos) | `OVERHEAD_OR_UNATTRIBUTED` |

Example — cost by category per space (workspace-scoped), showing authoring can
dominate:

```sql
SELECT s.name AS space, v.genie_cost_category,
       COUNT(*) AS statements,
       ROUND(SUM(v.query_attributed_dollars_estimation), 4) AS cost_usd
FROM genie_cost_categorised v
LEFT JOIN adb_genie_spaces s ON v.query_source_id = s.space_id
WHERE v.workspace_id = <this workspace>
GROUP BY ALL ORDER BY space, cost_usd DESC;
```

Real result from a test space: `AUTHORING_PROFILING` = $0.9564 (64 statements)
vs `CONVERSATIONAL` = $0.0936 (13 statements) — i.e. ~91% of that space's cost
was authoring it, not using it.

- **Scope note:** `dbsql_cost_per_query_table` (via `system.query.history` /
  `system.billing.usage`) is **account-wide** — it sees Genie spaces across every
  workspace in the metastore, including ones your Genie API token cannot list.
  Always filter on `workspace_id` when comparing against API-sourced tables,
  which are scoped to workspaces you can access.

### 2. LLM token cost (pay-as-you-go, from 2026-07-08)
- **Source:** `mvFactGenieTokenUsage`, built from
  `system.billing.usage WHERE billing_origin_product = 'GENIE'` and
  `usage_type = 'TOKEN'` (measured in DBUs).
- **Product / space attribution lives in the nested struct
  `usage_metadata.genie = struct<surface, channel, agent_id>`:**
  - `surface` → the Genie product: **`GENIE_CODE` | `GENIE_ONE` | `GENIE_AGENTS`**
    — so token cost splits by product **exactly**.
  - `channel` → **`UI` | `API`**.
  - `agent_id` → the Genie **`space_id`**, populated **only for `GENIE_AGENTS`**
    (NULL for Code / One, which are not space-scoped). It joins 1:1 to
    `adb_genie_spaces`, so **per-space token cost is exact for Agents** — no
    estimation. Code / One token cost is per-user only.
  - (Do **not** use `product_features.genie.offering_type` for the product split
    — it is flat `PAYGO`. The real signal is `usage_metadata.genie`.)
- `usage_type = 'TOKEN'` is the *category*; `usage_quantity` is in **DBUs**
  (`usage_unit = 'DBU'`) — Databricks pre-converts tokens to DBUs, so raw token
  counts are **not** exposed. DBUs → $ via `system.billing.list_prices`.
- The `GENIE_FREE_USAGE` SKU is the free-tier allowance (unpriced, $0); paid
  usage is regional `ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_*` SKUs.
  **Free vs paid is not a sequential "150 DBUs then switch" cap** — free and
  paid rows co-occur in the same hour and users routinely exceed 150 free DBUs
  (Genie One / Agents are fully waived through 2027-01-31; only Genie Code is
  subject to the ~150 DBU/user/month allowance). Trust the `sku_name` split for
  free vs charged; do not reconstruct the allowance as a running cap.

Token billing is **account-wide**, so filter on `workspace_id` for a single-
workspace view. Grain: `workspace / user / surface / channel / agent_id (space) /
tier / sku / day`.

### TODO / known gaps

- **Agent-mode verification.** The message→statement bridge is grain-robust but
  has only been validated against chat mode; agent mode (Responses API) was
  disabled on the test workspace. Confirm statement/message capture once a
  workspace with the Responses API + deep-research enabled is available.

### Optimisation avenues

The ingest (`01_get_metadata.ipynb`) is a nested API fan-out with no redundant
calls — each space, conversation, message and unique user is fetched exactly
once (user-email lookups are cached in `USER_CACHE`, and cell 11 reuses the
in-memory `messages` list rather than re-fetching).

**Implemented** (previously the ingest could take *hours* on a busy workspace;
the comment fan-out was the long pole in `Ingest_Metadata`):

- **Comments are only fetched for messages that have feedback.** Cell 11 filters
  to `feedback_rating != 'NONE'` before calling `GET .../messages/{id}/comments`.
  A comment / thumbs-down reason cannot exist without a rating, so this removes
  ~90%+ of the calls on a typical workspace with zero loss of coverage.
- **Every independent per-item REST loop now fans out over a shared bounded
  thread pool** (`parallel_map`, defined in the imports cell) instead of looping
  serially. This covers the Genie conversation fetch, the message fetch, the
  comment fetch, dashboard schedules, dashboard subscriptions, and the
  `get_permissions` lookups for models / serving endpoints / apps — i.e. all of
  the round-trip-bound work. Genie list endpoints are throttled tighter, so those
  fan-outs use `GENIE_MAX_WORKERS=8`; the Permissions API tolerates the default 8.
  Per-item errors are collected, not raised, so one 403/404 never aborts a batch.
  Lower the worker counts if you hit API rate limits.

Levers today: `enable_genie_feedback_comments=false` skips comments entirely;
`skip_get_conversations=true` skips messages+comments.

**Still open (larger, architectural — see the V3 branch for reference
implementations in `src/includes/`):**

- **Incremental ingest via watermarks.** The notebook drops and rebuilds all
  tables each run, and the `02_*.sql` MVs re-scan a 180-day `system.*` window
  every invocation. A `dim_pipeline_watermarks` table (one row per
  `source × workspace_id`, tracking the highest `event_time` processed) lets each
  run read only new rows. Biggest remaining lever on large metastores — this is
  SQL/Photon compute cost, not API cost.
- **Idempotent `MERGE` instead of drop-and-overwrite.** Switching the writes from
  `mode("overwrite")` to a workspace-scoped `MERGE` (keyed on the natural key +
  `workspace_id`) makes reruns idempotent, avoids full table rewrites, and lets
  two deployments safely share one catalog without clobbering each other's rows.

⸻

## Requirements

- Databricks Runtime 13.x or later
- Unity Catalog enabled
- Access to your organisation’s usage and audit logs via system tables


## Contributing

Contributions are welcome!

Please open an issue or submit a pull request to propose enhancements, bug fixes, or new visualisations.

## Disclaimer

This code is not endorsed by or affiliated in any way with Databricks. Use it at your own risk and review everything before using it.


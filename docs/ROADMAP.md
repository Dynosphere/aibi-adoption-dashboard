# Post-v3 upgrade roadmap

v3 is a released baseline: a medallion Genie cost-observability model plus multi-asset cost
facts (serving, vector search, apps), with the data-correctness fixes from PR #21 (NULL
billing-line buckets, serving cost undercapture, and the empty `adb_apps` crawl).

This backlog captures the planned upgrades. **Do them in order** — each part unblocks the next:

```
Part 1 (rename)  ──►  Part 2 (crawl perf / incremental)  ──►  Part 3 (semantic layer)  ──►  Part 4 (dashboard)
   cheap now,           needs stable names before             wraps stable facts            consumes the metric
   before state          adding watermark state                                              views for speed + clarity
```

Each part is independently shippable as its own PR. Parts 1 and the de-hardcode are **breaking
changes** — see "Migration" at the end.

---

## Part 1 — Asset name normalisation

**Goal:** consistent, cloud-neutral, self-describing table names in one schema, with real metadata.

**Why first:** the pipeline is currently a stateless full rebuild (drop-and-recreate every run),
so a rename is **code-only + rerun, no data migration**. Once Part 2 adds watermark/MERGE state,
renaming means migrating persisted state — much more expensive. Rename while it's free.

**Current state — three inconsistent conventions in one schema:**
- Crawl tables: `adb_*` (11 tables). `adb` reads as *Azure Databricks* but here means *Adoption
  Dashboard* — misleading for a cloud-agnostic solution. No table/column comments.
- Cost/fact tables: `mvFact*` (6). Camel-case; `mv` implies *materialized view* but they are plain
  Delta tables (misleading); `Fact` prefix.
- Cost/fact tables: `dbsql_cost_per_query_table`, `genie_cost_per_message_table` (redundant
  `_table` suffix), `genie_cost_categorised` (a view).
- Dashboard datasets: 8 have auto-generated hash names (`e2b9e81f`, `a5510c39`, …).

**Naming rule:** flat, short, descriptive names by default; use `dim_`/`fact_` **only** where the
object genuinely is a dimension or a fact. Applied honestly, that lands on:
- `dim_*` (entity catalogs): `genie_spaces`, `dashboards`, `apps`, `models`, `serving_endpoints`
- `fact_*` (grain + additive measures): `serving_usage`, `vector_search_cost`, `app_usage`,
  `genie_usage`, `genie_token_cost`, `dashboard_usage`, `dbsql_cost_per_query`,
  `genie_cost_per_message`
- flat (bridge / detail / view): `genie_conversations`, `genie_messages`,
  `genie_message_statements` (a bridge), `genie_message_comments`, `genie_cost_by_category` (view)

**Tasks:**
- [ ] Land everything in one schema. Decide `adoption` vs `dbx_adoption` (lean: plain `adoption` —
      the UC catalog already scopes it).
- [ ] Rename per the rule above; drop the `adb_` prefix, the `mv` misnomer, and the `_table` suffix.
- [ ] Rename the 8 hash-named dashboard datasets to `<page>_<content>` slugs.
- [ ] Add table + column `COMMENT`s to the crawl tables (they have none today — this also feeds
      Genie One / Catalog Explorer and sets up Part 3).
- [ ] Remove the hardcoded catalog/schema in two dashboard datasets (`apps_views`, `uc_models` —
      grep `field_eng_slc`); use the bundle `dataset_catalog` / `dataset_schema` like the others.
- [ ] Update every reference atomically: the crawl notebook (`01_get_metadata.ipynb` — drop-list,
      `schemas` dict, each `saveAsTable`), all `src/02_*.sql` (reads **and** CREATE targets), and
      the dashboard dataset queries. A partial rename breaks joins.

**Done when:** one run produces the new-named, commented tables; the dashboard renders; no
`adb_`/`mvFact`/`_table` names remain; no hardcoded catalog/schema remains.

---

## Part 2 — Crawl & pipeline performance pass

**Goal:** stop rebuilding everything from scratch every run; make large-metastore runs tractable.

**Findings (verified this cycle):**
- Every table is `CREATE OR REPLACE` / `INSERT OVERWRITE` — a full rebuild each run. `lookback_days`
  defaults to **365**, so the cost facts re-scan a year of `system.*` every invocation.
- `mvFactGenieUsage` and `mvFactDashboardUsage` **hardcode a 180-day window** (not wired to
  `lookback_days`) — inconsistent and unbounded by the knob.
- The crawl notebook is the wall-clock long pole. A full run on a busy workspace (e2-demo) sat in
  `Ingest_Metadata` for **67+ minutes**. The cost is the per-item REST fan-outs — `registered_models`
  / serving-endpoint / app permission lookups, and the Genie conversation/message/comment walk.
  Note `adb_apps` is the **last** crawl cell, so a slow models/endpoints section blocks it entirely.
- `genie_cost_categorised` is an **unmaterialized view** that `LIKE`-matches multi-KB
  `statement_text` — it re-runs on every dashboard load.

**Tasks:**
- [ ] Incremental load via watermarks: a `pipeline_watermarks` table (one row per
      `source × workspace_id`, highest `event_time`/`usage_date` processed) so each run reads only
      new rows. Biggest lever on large metastores (SQL/Photon compute, not API).
- [ ] Idempotent `MERGE` (keyed on natural key + `workspace_id`) instead of drop-and-overwrite —
      makes reruns idempotent and lets two deployments share a catalog safely. (Requires Part 1's
      stable names.)
- [ ] Wire `lookback_days` into `mvFactGenieUsage` / `mvFactDashboardUsage` (remove the hardcoded
      180-day windows); standardise `INTERVAL` → `date_sub`.
- [ ] Crawl: consider splitting the monolithic notebook so independent sections (spaces, dashboards,
      models, endpoints, apps) are separate, independently-retryable tasks — so a slow/failed section
      never blocks the others (e.g. `adb_apps`). Investigate paginated/bulk permission APIs to cut the
      per-item fan-out; the existing `parallel_map` already bounds concurrency.
- [ ] Materialise `genie_cost_by_category` (was `genie_cost_categorised`) or precompute the
      classification, so the dashboard isn't pattern-matching `statement_text` on every load.

**Done when:** a rerun on a large metastore processes only new data; no full-year rescans; the crawl
no longer serialises everything behind one long task.

---

## Part 3 — Semantic layer definition

**Goal:** a governed measure layer (UC Metric Views) over the stable facts, so measures are named,
reusable, re-aggregation-safe, and discoverable by Genie One and external BI.

**Context (verify against current docs at build time):**
- UC Metric Views v1.1 require DBR 17.2+ / serverless SQL. `synonyms`, `display_name`, `format`
  improve Genie One auto-discovery and rendering.
- A metric view is **one `source` + declarative joins**. Materialization is experimental, and
  **parameterized metric views cannot be materialized**.

**Tasks:**
- [ ] Add `CREATE OR REPLACE VIEW … WITH METRICS LANGUAGE YAML` wrappers over the stable facts.
      Start with the clean single-source facts: `serving_usage`, `vector_search_cost`, `app_usage`,
      then `genie_usage` / `genie_token_cost`.
- [ ] Define measures (`SUM(dbus)`, `SUM(dollars)`, request/token counts, distinct users) and
      dimensions (workspace, entity_type/name, date) with `display_name`, `format`, `synonyms`.
- [ ] Deploy the metric views via the bundle alongside the facts (zero extra compute — they are views
      over the Delta facts the job already builds).

**Watch-outs:**
- Do **not** rewrite the multi-source KPI datasets (`ov_kpi`, `co_kpi`, `ov_leaderboard`, …) as metric
  views — they join 3–4 sources in one CTE, which is outside the single-source metric-view model.
  Metric views are **additive** on top of facts, not a replacement for cross-fact KPIs.
- Hands-on check needed: that `MEASURE(...)` in a dashboard dataset `queryLines` executes on the
  target SQL warehouse.

**Done when:** each stable fact has a governed metric view; Genie One can answer a measure question
("which foundation model costs most?") without a dashboard.

---

## Part 4 — Dashboard refresh + performance

**Goal:** modernise the AI/BI dashboard and make it fast — **largely solved by Part 3**: point
datasets at (materialized) metric views instead of re-running heavy CTEs on every load.

**Current state:** already 8 pages (Dashboards, Apps, Models, 4× Genie, Global Filters), a workspace
filter, and combo/area/pie widgets — further along than earlier notes suggested.

**Tasks:**
- [ ] Repoint dashboard datasets at the metric views from Part 3 where sensible; materialize the
      hot/expensive ones for load-time performance (subject to the parameterized-MV limitation).
- [ ] Add the missing **Vector Search page** (the fact exists from v3; no page yet).
- [ ] Adopt richer widgets where they add signal: `period` / `target` encodings on KPI counters,
      area for token trend, combo for dual-axis DBU vs $, heatmap for workspace × product density.
- [ ] Verify dataset cross-filtering / drill-through (the exact JSON schema was UNVERIFIED as of the
      2026-08 spike — needs a hands-on `bundle generate` diff).
- [ ] Confirm the hash-dataset rename and the `field_eng_slc` de-hardcode from Part 1 are reflected.

**Done when:** dashboard loads quickly off the semantic layer, all pages use clear dataset names, and
the widget set reflects current AI/BI capabilities.

---

## Cross-cutting: readiness for Databricks Solutions

- **Migration ("If you're migrating" — required in the README before shipping Parts 1/de-hardcode):**
  this is a major, intentionally not-backwards-compatible overhaul. Because the pipeline is stateless,
  migration is: redeploy the new bundle → rerun the job (new-named tables appear) → repoint any
  *custom* downstream queries/dashboards built on the old names → drop the old tables. The bundled
  dashboard moves with the repo, so most users only touch their own artifacts. (Gets harder after
  Part 2 adds state — another reason Part 1 goes first.)
- De-hardcode all environment-specific references; keep `variables.yml` defaults generic.
- Revisit the README/LICENSE disclaimer wording for an official-accelerator context.
- Grow test coverage: the SQL fixtures assert grain/non-negativity/NOT-NULL invariants today; add
  reconciliation checks and cover the crawl once incremental logic lands.

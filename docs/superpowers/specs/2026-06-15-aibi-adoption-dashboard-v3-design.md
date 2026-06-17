# aibi-adoption-dashboard V3 — Design

**Date:** 2026-06-15
**Author:** Sam Le Corre (with Brian)
**Status:** Draft for review
**Hard deadline:** 2026-07-06 (Genie pay-as-you-go go-live)
**Repo:** [Dynosphere/aibi-adoption-dashboard](https://github.com/Dynosphere/aibi-adoption-dashboard) (public)
**Workspace target during build:** `e2-demo-field-eng` (`o=1444828305810485`)
**Companion docs:** `docs/superpowers/spikes/spike-{a,b,c,d,e,f}-*.md`

---

## 1. Why V3 exists

V3 is driven by three forces:

1. **External deadline.** Genie / Genie Code / Genie One move to pay-as-you-go pricing on **2026-07-06**. Customers will start seeing LLM-cost line items they have never seen before. The adoption dashboard is the natural place to surface "how much am I spending? how much do I have left?" Without V3, the dashboard goes stale on day one of paygo.
2. **Architectural debt.** V2's metadata notebook is a sequential SDK polling loop that takes hours on large workspaces (issue #12) and joins through SDK message attachments that are unreliable (issue #14). Adding new asset types on top of that foundation makes the problem worse.
3. **Platform shift.** Since V2 shipped (~Nov 2025), Databricks has added: `system.serving.served_entities`, `system.serving.endpoint_usage`, `system.access.audit` GA-quality coverage of Genie, `system.billing.usage.usage_metadata.{app_id,endpoint_id}`, `system.access.workspaces_latest`, and (imminently) `system.billing.usage.billing_origin_product='AI/BI_GENIE'`. Most of the SDK fan-out V2 does is now redundant.

---

## 2. Goals & non-goals

### V3.0 goals

- **Fix the bugs.** Issue #11 (catalog/schema not auto-created), issue #14 (statement_id mismatch) — both gone.
- **Make the pipeline tractable on large workspaces.** Issue #12 — sequential SDK loops replaced by system tables for facts, bounded ThreadPoolExecutor for residual SDK dimensions.
- **Ship Genie paygo monitoring.** A first-class "cost & headroom" view that maps cleanly onto the post-July-6 billing tables, with no speculative LLM-cost simulator.
- **Modernise Apps & Models tracking.** Drop low-signal model-registry events; rebuild on `system.serving.*` + `system.billing.usage.usage_metadata`.
- **Add Quality tile** (CP5) — surfacing existing Genie benchmark accuracy + user feedback signal, not custom evals.
- **Future-proof for multi-workspace.** Carry `workspace_id` end-to-end so a customer with a shared UC catalog isn't a breaking change later.
- **Public-facing release quality.** Honest disclaimers about Public Preview / Beta dependencies; one rename absorbed (Genie Spaces → Genie Agents); deployment docs that work for a customer who's never seen the repo.

### Explicit non-goals

- **No "what would you have paid" Genie cost simulator.** LLM token usage is not in any data we have access to today; estimates would mislead.
- **No custom LLM judge / eval framework.** Genie already has benchmarks + Inspect mode in-product; V3 surfaces those, doesn't recreate them.
- **No Agent Bricks (KA/MAS) adoption page.** No `system.agents.*` table yet, MLflow trace UC tables still preview. Add a placeholder; revisit V3.x.
- **No "hub" deployment (single workspace fans out via account SP).** Defer to V3.x backlog; the data-model future-proofing in V3 is enough.
- **No migration to `databricks-solutions/` GitHub org.** Backlog item; doesn't gate V3.
- **No cross-Databricks-account support.** System tables are scoped to one account.

---

## 3. Timeline & checkpoints

Today is **2026-06-15 (Mon)**; Genie paygo go-live is **2026-07-06 (Mon)** — **21 calendar days / ~15 working days** away. Effort estimates below total **17.5–23.5 days of dev effort**. **The plan does not fit in 15 working days of one person's time.** We close the gap by one or more of: (a) starting CP0/CP1 immediately and running CP3-CP6 partly in parallel where dependencies allow, (b) accepting CP5 slip to V3.1, (c) Sam working a couple of weekend days, or (d) recruiting a second contributor (Mike / Krishnan, per the `#adoption-dashboard-customer` thread). The slip-fuse remains CP5; if CP2 (the biggest single phase) overruns, CP5 is the first thing to defer.

Each checkpoint is independently shippable to `main` and tagged `v3.0.0-rc<N>`. We tag `v3.0.0` after CP6.

| # | Checkpoint | Days | Resolves | Ships |
|---|---|---|---|---|
| **CP0** | Live system-table validation pass | 0.5 | — | `docs/v3-system-table-validation.md` — `DESCRIBE EXTENDED` output for every system table referenced. **Gates CP1.** |
| **CP1** | Stabilise + future-proof | 2–3 | #11, future-proof for multi-workspace | Try-create catalog/schema; `workspace_id` on every `adb_*`; switch `df.write.mode("overwrite")` → `MERGE` keyed on `(<entity_id>, workspace_id)`. |
| **CP2** | Genie observability refactor + perf | 4–6 | #12, #14 | Drop `adb_genie_conversations`, `adb_genie_messages` (primary), and `genie_observability_main_table`. Re-derive from `system.access.audit` + `system.query.history.genie_space_id`. Add bounded `ThreadPoolExecutor` to residual SDK lists. Add `enable_genie_deep_dive: false` bundle var that re-enables the SDK message-content loader as a separate, additive path. |
| **CP3** | Genie paygo | 2 | new | `mvFactGeniePaygoCost` (UNION ALL of warehouse-DBU and `system.billing.usage WHERE billing_origin_product IN ('GENIE','AI/BI_GENIE')`); new "Cost" page tile with July-6 date marker + 150-DBU/user reference line; optional `system.ai_gateway.usage` token tile (gated). |
| **CP4** | Apps & Models modernisation | 4–5 | new | Drop `mvFactModelUsage`. Rename `mvFactServingEndpointUsage` → `mvFactServingUsage` (custom + foundation models). Add `mvFactVectorSearchCost`. Rebuild `mvFactAppUsage` on `system.billing.usage.usage_metadata.app_id` + audit. Agent Bricks placeholder ("coming soon"). |
| **CP5** | Quality (Genie accuracy + feedback) | 3–4 | #13 | New "Quality" page. **Feedback tile** from `system.access.audit (service_name IN ('aibiGenie','genieChat'))` merged with a `surface` facet. **Benchmark accuracy tile** via SDK fan-out (`genie_list_eval_runs` / `genie_get_eval_run`) snapshotted daily into a Delta dim. |
| **CP6** | Cross-asset adoption + docs + polish | 2–3 | #15 + general | `mvFactAssetAdoption` (UNION ALL of all `mvFact*` projected to a common shape). README "Deploying across multiple workspaces" section. "Genie Agents (formerly Genie Spaces)" rename. Refresh screenshots. CHANGELOG. Tag `v3.0.0`. |

**Total:** 17–23 days. CP5 is the natural slip candidate if CP2 overruns.

---

## 4. Architectural principles

These are the design invariants — every CP must obey them.

1. **Dimensions live in the SDK; facts live in system tables.** SDK is the only public source of catalogue data (Genie space names, dashboard titles, app metadata). Facts (who used what, when, how much it cost) come from `system.access.audit`, `system.query.history`, `system.serving.*`, `system.billing.usage`.
2. **Facts are persisted, not re-derived.** Every `mvFact*` is an **incremental Delta table** (`MERGE INTO` keyed on the natural grain — typically `(asset_id, event_date, workspace_id)`). We never lean on a system-table view for our history. `system.access.audit` is 365d Public Preview; we cannot let our trend lines truncate when it rolls off.
3. **Writes are workspace-aware.** Every `adb_*` and `mvFact*` carries `workspace_id`. Writes are `MERGE`-keyed on `(<entity_id>, workspace_id)`. Two deployments writing to the same catalog don't collide.
4. **System-table dependencies are explicit.** Every Public Preview / Beta surface gets a banner on the relevant tile. We tell the user, not pretend.
5. **The deep-dive path is opt-in and additive.** The SDK message-content loader (`adb_genie_messages`) is gated behind `enable_genie_deep_dive: false`. When off, the pipeline succeeds and no dashboard widget breaks. When on, the deep-dive panel works exactly as in V2.
6. **Public-facing tone.** Honest, no internal jargon, no go/ links, no internal-only sources cited. AI Gateway Budgets are "Public Preview as of DAIS 2026; GA expected early July".

---

## 5. Data model

### V2 → V3 mapping

#### Dimension tables (`adb_*`, SDK-sourced)

| Object | V2 | V3 | Source change | Notes |
|---|---|---|---|---|
| `adb_genie_spaces` | SDK | **Keep**, add `workspace_id` | `w.genie.list_spaces()`; add `serialized_space` field (SDK ≥0.117) | Catalogue only — no system table covers Genie space directory yet. |
| `adb_genie_conversations` | SDK per space | **Drop** | — | Replaced by `system.access.audit (action_name IN ('genieCreateConversation','genieStartConversationMessage'))`. |
| `adb_genie_messages` | SDK per conversation | **Drop as primary**; keep as **opt-in deep-dive snapshot** | gated behind `enable_genie_deep_dive: false` bundle var | `mvFact*` no longer depend on this table. Deep-dive panel (in `src/standalone_resources/genie_observability/`) reads it when the flag is on; degrades to a "Deep dive disabled — set `enable_genie_deep_dive=true` to enable" placeholder when the flag is off. |
| `adb_dashboards` | SDK | **Keep**, add `workspace_id` | `w.lakeview.list()` (parallel) | No system-table catalogue exists. |
| `adb_dashboard_schedules` | SDK per dashboard | **Keep**, add `workspace_id`, parallelise | `ThreadPoolExecutor(max_workers=8)` | N+1 stays, but ceiling is dashboard count. |
| `adb_dashboard_subscriptions` | SDK per schedule | **Keep**, add `workspace_id`, parallelise | as above | |
| `adb_models` | SDK | **Drop** | — | UC registry events ≠ adoption. `mvFactServingUsage` covers real consumption via `system.serving.served_entities`. |
| `adb_serving_endpoints` | SDK | **Keep**, add `workspace_id` | filter `get_permissions()` calls to "active last 30d" via `system.access.table_lineage` | Permissions API is 100/s — fine threaded but not worth calling for every endpoint. |
| `adb_apps` | SDK | **Keep**, add `workspace_id`, accommodate new `space` field | `w.apps.list()` (parallel) | `system.apps.apps` dimension table is in progress but not yet public; re-evaluate at V3.x. |
| `adb_eval_runs` | — | **NEW** | `w.genie.genie_list_eval_runs(space_id)` per space, daily snapshot | Per-run summaries only (`run_id`, `space_id`, `created_at`, `created_by`, `accuracy`, `status`). Per-question detail expires at 7d upstream — not historised. |

#### Fact tables (`mvFact*`, system-table-sourced, **persisted as incremental Delta**)

| Object | V2 | V3 | Primary source | Secondary source |
|---|---|---|---|---|
| `mvFactDashboardUsage` | view | **Incremental Delta**, modernise | `system.access.audit (service_name='dashboards')` | `system.query.history.dashboard_id` for query-level cost. Add explicit `workspace_id` join. |
| `mvFactGenieUsage` | view | **Incremental Delta**, modernise | `system.access.audit (service_name='aibiGenie')` | `system.query.history.genie_space_id`. No longer joins to `adb_genie_messages`. |
| `mvFactAppUsage` | view | **Incremental Delta**, modernise | `system.access.audit (service_name='apps')` | `system.billing.usage.usage_metadata.app_id` for DBU cost. |
| `mvFactModelUsage` | view | **Drop** | — | Replaced by `mvFactServingUsage`. |
| `mvFactServingEndpointUsage` | view | **Rename → `mvFactServingUsage`**; incremental Delta | `system.serving.served_entities` (dim) + `system.access.audit (service_name='serving')` (events) | `system.serving.endpoint_usage` (optional, Public Preview, opt-in per endpoint) + `system.billing.usage` (cost). |
| `mvFactGeniePaygoCost` | — | **NEW**, incremental Delta | `system.billing.usage WHERE billing_origin_product IN ('GENIE','AI/BI_GENIE')` | UNION ALL with warehouse DBUs from `dbsql_cost_per_query WHERE query_source_type='GENIE SPACE'`. |
| `mvFactVectorSearchCost` | — | **NEW**, incremental Delta | `system.billing.usage WHERE billing_origin_product='VECTOR_SEARCH'` grouped by `usage_metadata.endpoint_name`/`endpoint_id` | No query-rate metrics (out of V3 scope). |
| `mvFactGenieFeedback` | — | **NEW**, incremental Delta | `system.access.audit (service_name IN ('aibiGenie','genieChat'))` filtered to feedback action_names | Joins to `dim_genie_space` for space name. Carries a `surface` column (`agents` / `chat`). |
| `mvFactAssetAdoption` | — | **NEW**, incremental Delta | UNION ALL of every per-asset `mvFact*` projected to `(asset_type, asset_id, asset_name, workspace_id, usage_date, dbu_cost, activity_count)` | Powers the headline "Adoption Overview" page. |

#### Standalone tables

| Object | V2 | V3 | Notes |
|---|---|---|---|
| `dbsql_cost_per_query` | SQL view on system tables | **Keep** | Already best-of-breed; reused by `mvFactGeniePaygoCost`. |
| `genie_observability_main_table` | SDK-message-derived view | **Keep the name; rewrite the body** to source from `system.access.audit` + `system.query.history.genie_space_id`. | Fixes #14. Same external contract (name + key columns: `space_id`, `conversation_id`, `message_id`, `statement_id`, `user_email`, `created_datetime`, plus a `surface` column for `agents`/`chat`). Downstream queries (`mvFactGeniePaygoCost`, dashboard widgets, `genie_observability_deep_dive` notebook) keep working without changes. Internal: any new columns are additive; any V2 columns we cannot reconstruct from audit + query.history are dropped and listed in CHANGELOG. |

#### Dimensions we don't need to build

- `dim_workspace` — `system.access.workspaces_latest` (already joined in every V2 MV).
- `dim_user` — derive from `user_identity.email` in audit / billing.
- `dim_date` — `date_trunc` on usage timestamps.

### Incremental MERGE pattern

Every `mvFact*` follows this shape (illustrative pseudocode):

```sql
MERGE INTO ${catalog}.${schema}.mvFactGenieUsage tgt
USING (
  SELECT
    space_id                       AS asset_id,
    event_date                     AS usage_date,
    workspace_id,
    -- aggregations
    COUNT(*)                       AS activity_count,
    COUNT(DISTINCT user_email)     AS active_users
  FROM system.access.audit
  WHERE service_name = 'aibiGenie'
    AND event_date >= current_date() - INTERVAL 7 DAYS  -- bounded window
  GROUP BY ALL
) src
ON  tgt.asset_id     = src.asset_id
AND tgt.usage_date   = src.usage_date
AND tgt.workspace_id = src.workspace_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Each fact declares its own bounded window (typically 7 days, longer for slow-changing facts). The pipeline can run hourly, daily, or on-demand — the result converges.

### Workspace-id sourcing

From inside `01_get_metadata.ipynb`, the canonical join is:

```sql
SELECT workspace_id, workspace_name, workspace_url
FROM system.access.workspaces_latest
WHERE workspace_url = '${spark.conf.get("spark.databricks.workspaceUrl")}'
```

Single source of truth. Falls back to `w.config.host` if the system table is unavailable.

---

## 6. Dashboard structure

V2 has one dashboard (`lh_adoption_dashboard.lvdash.json`) plus a drillthrough draft on the stale branch. V3 keeps a single primary dashboard, four pages:

| Page | Powered by | Highlights |
|---|---|---|
| **Adoption Overview** | `mvFactAssetAdoption` | One headline KPI row across asset types; "what's growing / what's stagnant" trend; cross-asset cost stack. |
| **Genie** | `mvFactGenieUsage` + `mvFactGeniePaygoCost` + `mvFactGenieFeedback` | Active spaces, top spaces, **cost (with July-6 marker + 150-DBU/user reference line)**, **quality (feedback up/down rate, benchmark accuracy line)**. |
| **Dashboards & Apps** | `mvFactDashboardUsage` + `mvFactAppUsage` | Lakeview views, top dashboards, schedule health, app usage + cost, app cold-start frequency. |
| **Models & Vector Search** | `mvFactServingUsage` + `mvFactVectorSearchCost` | Endpoints in service, FM vs custom mix, token volume (if AI Gateway), cost, VS endpoints + cost, Agent Bricks placeholder. |

A **deep-dive** sub-page (Genie observability) reads `adb_genie_messages` when `enable_genie_deep_dive=true`; degrades to a "Deep-dive disabled — enable via bundle variable" placeholder otherwise. The drillthrough dashboard (`lh_adoption_drillthrough_dashboard.lvdash.json` from the stale branch) is **out of scope**; revisit V3.x.

Every tile that depends on a Public Preview / Beta source gets a small banner saying so, with a link to the upstream docs.

---

## 7. Deployment & multi-workspace

### V3 default (Topology A — per-workspace)

Unchanged from V2. Bundle variables are still `my_warehouse_id`, `catalog_name`, `schema_name`, `skip_get_conversations`. A new variable `enable_genie_deep_dive` (default `false`) gates the SDK message-content loader.

### Multi-workspace path (Topology B — shared UC catalog)

Documented in README (new section "Deploying across multiple workspaces"). Customer:

1. Picks a shared catalog (`governance.adoption` or similar).
2. Grants `USE CATALOG`, `CREATE SCHEMA`, `MODIFY` on the schema to each workspace's deploy identity.
3. Sets `catalog_name` to the shared catalog in every workspace's bundle deployment.
4. Each workspace's job writes only its own `workspace_id`-tagged rows via the V3 `MERGE` pattern — no duplication.

README also points to **Account-level Genie One** as Databricks's native cross-workspace **discovery** surface, so customers who just want to *find* assets across workspaces don't need our dashboard for that part — V3 is the *analytics* layer on top.

### Out-of-scope for V3 (backlog)

- Topology C (single hub workspace fans out via account SP)
- Federated-identity auth recipe
- Cross-Databricks-account support
- `dim_workspace_status` (latest ingest health per workspace)

---

## 8. Public-facing concerns

- **Disclaimer banner per tile** for every Public Preview / Beta dependency: `system.access.audit`, `system.serving.endpoint_usage`, `system.ai_gateway.usage`, Genie Benchmark API, `system.access.assistant_events`.
- **README rename copy**: "Genie Agents (formerly Genie Spaces)" once, then "Genie Agents" throughout. "Genie One" replaces "Databricks One" in screenshots and copy.
- **No internal references.** No go/ links, no Confluence URLs, no internal Slack channel IDs. All citations go to `docs.databricks.com`, the public blog, or GitHub.
- **Disclaimer in README** remains: "This code is not endorsed by or affiliated in any way with Databricks." (Until / unless we migrate to `databricks-solutions/`.)
- **Audit-log outage note** (Public Preview): mention 2025-10-16 → 2025-11-19 audit-log outage as a caveat on Genie / Dashboard trend tiles for customers with data from that window.
- **`system.serving.endpoint_usage` enablement note**: a soft banner that says "Token-level metrics require usage tracking enabled on each endpoint" and links to the AI Gateway docs.

---

## 9. Open questions for the PM / wider team

These don't block CP0–CP2 but should be answered before CP3 / CP5 / launch:

1. **Final `billing_origin_product` label.** `GENIE` vs `AI/BI_GENIE`. Confirm with Weston Hutchins / Miranda Luna (AI/BI PMs) — the `IN (...)` clause in `mvFactGeniePaygoCost` future-proofs us either way, but the dashboard copy should pick one.
2. **`usage_metadata` shape for Genie records.** The sub-field that attributes a billing row back to `space_id` / `user_id` (likely `usage_metadata.ai_gateway.*` or a new Genie-specific struct). Will be visible in dogfood/staging this week.
3. **Genie Code vs Genie Agents split in billing.** Do they share `billing_origin_product='AI/BI_GENIE'`, or do they split via `product_features.genie.*`?
4. **Free-credit ledger.** Is there an API / table for "per-user remaining 150-DBU credit this month"? Without it the dashboard shows spend, not headroom.
5. **Benchmark API GA timing.** Beta as of June 2026. If it remains Beta on ship day, the accuracy tile carries a Beta banner.
6. **`genieChat` vs `aibiGenie` feedback widget shape.** Confirmed: merged with `surface` facet. PM (Elise Georis) to sign off on copy.
7. **Adoption-rate copy for benchmarks.** Internal PRD says ~7% of Genie spaces use benchmarks. Public copy should say "Many Genie spaces do not yet have benchmarks configured."
8. **AI Gateway Budgets surfacing.** Stays in CP3 (cost), not CP5 (quality).

---

## 10. Risks & mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `system.billing.usage` paygo rows don't appear on July 6 as expected | Low | Medium | `mvFactGeniePaygoCost` is additive — empty until rows land, no breakage. README explicitly says "post-July-6 the cost panel populates automatically." |
| `billing_origin_product` label final name unconfirmed | Medium | Low | `IN ('GENIE','AI/BI_GENIE')` future-proofs either way. |
| Benchmark API stays Beta on ship day | Medium | Low | Beta banner on accuracy tile; functionality unchanged. |
| `enable_genie_deep_dive=false` breaks an existing widget | Medium | **High** (per Sam's constraint) | CP2 explicit gate: the deep-dive widget must render a placeholder when the flag is off. Add a test that the bundle deploys + the dashboard loads with `enable_genie_deep_dive=false`. |
| CP2 (Genie observability refactor) blows up | Medium | High | CP5 is pre-identified as the slip candidate if CP2 overruns. Tag `v3.0.0-rc1` after CP1 so we have a safe rollback. |
| `system.access.audit` schema drift (Public Preview) | Low | Medium | Schema-evolution test in CP0; pin to current column list in MERGE source SELECTs. |
| Cosmetic rename ("Genie Spaces" → "Genie Agents") missed in a widget | Low | Low | Final pass in CP6; grep on `lvdash.json` and `README.md`. |

---

## 11. Decisions log (architectural forks, locked)

| Fork | Decision | Rationale |
|---|---|---|
| **Statement_id fix** | **1b** — refactor on `system.access.audit` + `system.query.history`, drop SDK message attachments as the cost-join source. | Patches don't fix #12; refactor fixes #12 and #14 together and removes 600+ lines from the metadata notebook. |
| **Cross-asset adoption fact** | **2a** — build `mvFactAssetAdoption` as a separate MV that UNION ALLs per-asset facts. | One source of truth for the headline page; per-asset MVs survive for deep-dives. |
| **`workspace_id` rollout** | **3a** — universal in V3, even on Topology A. | Pay 0.5 day now to avoid a breaking change at V3.x. |
| **Apps catalogue** | **4a** — keep `adb_apps` SDK loader. | Trivial cost; we want app names even when billing is zero. Re-evaluate when `system.apps.apps` is public. |
| **AI Gateway tiles** | **5a** — soft-prerequisite with banner. | Public-facing standard. Empty-with-explanation beats empty-with-mystery. |
| **Materialised aggregations** | **Incremental Delta with `MERGE INTO`** | Audit rolling off at 365d cannot truncate our history. |
| **`adb_genie_messages` SDK loader** | **Kept as opt-in** (`enable_genie_deep_dive: false`) | Deep-dive panel survives for the customers who use it; default deployment isn't slowed by SDK message fan-out. **Constraint: must not break pipeline or dashboard when off.** |
| **Feedback widget shape** | **Merged** with a `surface` column (`agents` / `chat`) | Simpler dashboard; users can facet by surface. |
| **CP5 slip-or-ship** | **Ship in V3.0**. Keep V3.1 slip live only if CP2 overruns. | Effort dropped to 3–4d; budget holds. |
| **Live `DESCRIBE EXTENDED` validation** | **CP0 prep task** (~0.5 day, gates CP1) | Kill schema ambiguity before anyone writes a MERGE. |
| **Migration to `databricks-solutions/`** | **Backlog** | Doesn't gate V3. Revisit post-launch. |

---

## 12. Appendix — references

- Spike A — system tables × SDK audit: `docs/superpowers/spikes/spike-a-system-tables-sdk.md`
- Spike B — Genie paygo: `docs/superpowers/spikes/spike-b-genie-paygo.md`
- Spike C — apps & models: `docs/superpowers/spikes/spike-c-apps-models.md`
- Spike D — data-model synthesis: `docs/superpowers/spikes/spike-d-data-model.md`
- Spike E — multi-workspace: `docs/superpowers/spikes/spike-e-multi-workspace.md`
- Spike F — CP5 + DAIS status: `docs/superpowers/spikes/spike-f-cp5-and-aigateway.md`
- GitHub issues: [#11](https://github.com/Dynosphere/aibi-adoption-dashboard/issues/11), [#12](https://github.com/Dynosphere/aibi-adoption-dashboard/issues/12), [#13](https://github.com/Dynosphere/aibi-adoption-dashboard/issues/13), [#14](https://github.com/Dynosphere/aibi-adoption-dashboard/issues/14), [#15](https://github.com/Dynosphere/aibi-adoption-dashboard/issues/15)
- Slack: `#adoption-dashboard-customer` (`C0B5T0TUQ6T`) and the Thames Water DM (`C0AU7M2RSQM`).
- FEIP-6644 (databricks-solutions migration — backlog).

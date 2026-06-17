# Spike S-E — Multi-workspace & account-level deployment patterns

**Author:** Sam Le Corre
**Date:** 2026-06-16
**Status:** Investigation complete; recommendation for V3.
**Scope:** `aibi-adoption-dashboard` (public-facing OSS, V3 target 2026-07-06).

---

## Summary

Recommendation for V3: **stay on Topology A (per-workspace deploy), but make two
small, low-risk data-model changes now** so customers can graduate to Topology B
(shared catalog across workspaces) without a breaking re-model. Concretely:
(1) carry `workspace_id` end-to-end through every `adb_*` metadata table
populated by `01_get_metadata.ipynb` (today only the system-table-sourced rows
carry it; SDK-sourced rows don't), and (2) include `workspace_id` in the
join/PK shape of every `mvFact*` so unioning two deployments' tables in a
shared catalog is a `UNION ALL` with no de-dup logic. Account-level topology C
(single hub workspace, SDK fans out to N workspaces) is feasible on GA features
today — account-level OAuth M2M service principal can call workspace APIs
across all workspaces the SP is assigned to — but adds two material costs we
should not pay before July 6: (i) customer admin overhead (SP creation +
per-workspace assignment + Genie/Lakeview perms) and (ii) hub-workspace
egress/quota concerns when iterating thousands of Genie messages. Defer C to a
V3.x backlog item.

Separate but important: **Databricks shipped "Account-level Genie One" in
2026** (unified homepage, cross-workspace search across dashboards, Genie
spaces, and Apps from a single entry point). This is the native GA answer to
Salman Khan's April 2026 question, and we should reference it in the README
rather than reinvent it. Our value-add over Genie One remains **adoption
metrics** (usage trends, who's using what, abandonment, feedback) — Genie One
is a discovery surface, not an analytics one.

---

## Topology comparison

| Option | Auth model | Where data lives | Pros | Cons |
|---|---|---|---|---|
| **A — Per-workspace deploy (today)** | Notebook-context (workspace user / job-run identity). Workspace-scoped, no SP. | Per-workspace catalog/schema (defaults `users.<schema>`). One copy of `adb_*` + `mvFact*` per workspace. | Zero customer config beyond bundle variables. No account admin needed. Already shipping. Each workspace's data stays put (compliance-friendly). | No cross-workspace consolidation. N workspaces → N deploys. Dashboard only shows local activity. This is the Salman Khan complaint. |
| **B — Per-workspace deploy → shared UC catalog** | Same workspace-scoped auth as A for the metadata notebook. Shared catalog reached via UC's GA cross-workspace metastore-sharing. | One central catalog (e.g. `governance.adoption`). Each workspace writes to the same catalog with `workspace_id` as a partition/PK component. Dashboard reads the union. | No new auth pattern; same SDK calls; UC handles cross-workspace plumbing. System tables already account-scoped → `mvFact*` union cheaply. | Customer must grant write to the shared catalog from each workspace. Today our notebook does `mode("overwrite")` which would clobber other workspaces' rows — needs (a) `workspace_id` column on every `adb_*` table and (b) switch to `MERGE`/partition-overwrite keyed on `workspace_id`. |
| **C — Single hub + account SP fan-out** | Account-level OAuth M2M SP, assigned to every target workspace (workspace user + workspace admin for permissions reads). | One catalog in the hub. Hub job iterates `accounts.workspaces.list()` and, per workspace, instantiates `WorkspaceClient(host=ws_url, …)` with the same SP creds. | Single deploy, single catalog, single dashboard. Cleanest UX. | Highest customer setup cost: account admin must create SP, assign it to every workspace, grant per-workspace perms on every Genie space / Lakeview / App / serving endpoint. Hub must reach every workspace (PrivateLink/firewall blockers). All API quota burned in one workspace. Genie `list_conversation_messages` very expensive at scale. |

---

## System-table scoping — account vs workspace

System tables are an **account-scoped** product (one pipeline per Databricks
account, delta-shared into each UC-enabled workspace), but individual tables
vary in whether the *event itself* is account- or workspace-bound.

| Table | Granularity (per docs) | Has `workspace_id`? | Behaviour |
|---|---|---|---|
| `system.access.audit` | "Regional for workspace-level events. Global for account-level events." | Yes. **Account-level events record `workspace_id = 0`.** UC and other account-scoped services emit at `workspace_id = 0`. | From any workspace, `SELECT DISTINCT workspace_id FROM system.access.audit` returns every workspace. This is what makes B/C tractable. |
| `system.access.workspaces_latest` | Account-level dim of all workspaces (name + ID). | Yes (PK). | Already used as the join target in every `mvFact*` — confirms the dim already exists; no need to build `dim_workspace`. |
| `system.billing.usage` | **Global** — usage rows for every workspace in the account, routed to all regions. | Yes. | Cross-workspace by default. |
| `system.billing.list_prices` | Global. | N/A. | Cross-workspace by default. |
| `system.query.history` | Regional. | Yes. | Returns rows for every workspace in the region (Public Preview). `02_dbsql_cost_per_query.sql` already groups on `workspace_id`. |
| `system.compute.warehouse_events` | Regional. | Yes (via `warehouse_id`). | Cross-workspace within region. |
| `system.lakeflow.jobs`, `system.lakeflow.job_run_timeline` | Account/regional. | Yes. | Cross-workspace. |

**Practical implication:** every `system.*` source we depend on already
returns multi-workspace data from any single workspace. The blocker for B/C
is **not** the system tables — it's the SDK-sourced `adb_*` tables
(Genie spaces, dashboards, apps, models, serving endpoints) which are
populated workspace-by-workspace via `WorkspaceClient` and have **no
`workspace_id` column today**. Closing that gap is the real ask.

---

## Auth — public-facing pattern for B/C

Confirmed GA pattern (per `docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m`
and the Databricks OAuth architecture docs):

- **Account-level OAuth M2M service principal** is the recommended
  public-facing pattern. An account-level OAuth token can call workspace-level
  REST APIs **for any workspace the SP is assigned to**.
- The SP does **not** need to be account admin to call workspace APIs (account
  admin is only required for account-level APIs like creating workspaces).
  For our use case (list Genie spaces, Lakeview dashboards, apps, registered
  models, serving endpoints) it needs to be a workspace user in each target
  workspace, plus workspace admin (or per-object perms) for admin-only calls
  such as `get_permissions` on serving endpoints / models / apps.
- PATs are workspace-scoped and need one per workspace → anti-pattern at scale.
- Federated identity (workload identity federation / Azure SP) is a valid
  alternative on Azure and works with the same SDK auth chain, but is
  incremental over the M2M SP story rather than replacing it.

**For V3 (Topology A) we ship none of this.** Notebook-context auth is fine.
We mention the SP pattern in the README as the migration path.

---

## Data-model future-proofing — concrete V3 changes

Small, low-risk, no impact on Topology A. Unlocks B (and C) without re-model.

### 1. Carry `workspace_id` on every `adb_*` table

Today these tables have no `workspace_id` column — they implicitly contain
only the rows for the workspace the notebook ran in. Add `workspace_id` (and
optionally `workspace_url`) to each schema, sourced from
`w.config.host` or a join to `system.access.workspaces_latest WHERE
workspace_url = host` at the top of `01_get_metadata.ipynb`. Tables to update:

- `adb_genie_spaces`, `adb_genie_conversations`, `adb_genie_messages`
- `adb_dashboards`, `adb_dashboard_schedules`, `adb_dashboard_subscriptions`
- `adb_models`
- `adb_serving_endpoints`
- `adb_apps`

### 2. Change `mvFact*` join shape to include `workspace_id`

Most MVs already `GROUP BY ... workspace_id, w.workspace_name` (good), but the
join to `adb_*` is on the entity id alone — in Topology B two workspaces that
share an id would collide. Make the join explicit:

```sql
FROM ... D LEFT JOIN system.access.audit au
  ON au.request_params.dashboard_id = D.dashboard_id
 AND au.workspace_id = D.workspace_id     -- new
```

One-line change per MV. No-op in Topology A.

### 3. Switch table writes from `overwrite` to a workspace-aware merge

Required for B, harmless for A. Either:
- partition `adb_*` by `workspace_id` and use `INSERT OVERWRITE PARTITION`, or
- switch to `MERGE INTO` keyed on `(<entity_id>, workspace_id)`.

Today's `df.write.mode("overwrite")` would wipe other workspaces' rows in a
shared catalog.

### 4. Reuse `system.access.workspaces_latest` as `dim_workspace`

Don't build our own. Already joined in every `mvFact*`. Keep using it.

### 5. README — surface the topology choice

Add a short "Deploying across multiple workspaces" section pointing to
(a) "per-workspace, shared catalog" as the recommended consolidation pattern
and (b) **Account-level Genie One** as Databricks's native cross-workspace
discovery surface (so users who only want to *find* assets across workspaces
don't need our dashboard for that part).

---

## Backlog — explicitly deferred

| Item | Why deferred |
|---|---|
| Topology C reference deployment (hub + account SP) | High customer setup cost; needs account admin to bootstrap; API quota concerns; not needed if B suffices. Revisit V4 if a customer asks. |
| Federated-identity auth recipe | Incremental over OAuth M2M SP. Add when a customer hits it. |
| Cross-Databricks-account support | System tables are scoped to one account. Would need Delta Sharing between accounts. Far out. |
| Async/parallel SDK fan-out across workspaces | Only relevant for C. |
| `dim_workspace_status` (latest ingest health per workspace) | Nice-to-have; not blocking. |

---

## Open questions

1. **Default to writing `workspace_id` always, or behind a `multi_workspace_mode` bundle variable?** I'd default to always — the column is useful in single-workspace deployments too (breadcrumbs, error messages).
2. **What's the canonical `workspace_id` source from inside the notebook?** Options: `dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get()` (ugly), or join `system.access.workspaces_latest` by host URL. Prefer the latter — single source of truth.
3. **Does Account-level Genie One expose any read API we could piggyback on instead of per-workspace `WorkspaceClient` iteration?** Worth checking — if yes, that's the lowest-friction path to B without touching auth.
4. **Should `mvFact*` become metric views (UC governed) once we touch them?** Out of scope here, possibly a separate spike.
5. **In Topology B, who owns the shared catalog?** Customer's UC admin. We should document the minimum grants (`USE CATALOG`, `CREATE SCHEMA`, `MODIFY` on the schema) for the deploy identity in each workspace.

---

## References

- System tables reference (table-by-table granularity): https://docs.databricks.com/aws/en/admin/system-tables/
- Audit log system table reference ("Account-level audit logs record workspace_id as 0"): https://docs.databricks.com/aws/en/admin/system-tables/audit-logs
- Billable usage system table reference ("Global ... routed to all regions"): https://docs.databricks.com/aws/en/admin/system-tables/billing
- Workspaces system table reference: https://docs.databricks.com/aws/en/admin/system-tables/workspaces
- Service principal OAuth M2M: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m
- Account-level cost-management usage dashboards: https://docs.databricks.com/aws/en/admin/account-settings/usage
- AI/BI and Genie One 2026 release notes (Account-level Genie One — cross-workspace dashboards, Genie spaces, Apps from a single entry point): https://docs.databricks.com/aws/en/ai-bi/release-notes/2026
- Community FinOps reference repo (drcaiomoreno/databricks-finops-system-tables): https://github.com/drcaiomoreno/databricks-finops-system-tables
- Original ask in `#aibi-adoption-dashboard` Slack (`C0AU7M2RSQM`, ~April 2026, Salman Khan / Thames Water).

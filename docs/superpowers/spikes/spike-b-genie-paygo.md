# Spike S-B — Genie PayGo data source + inferable-today model

**Date:** 2026-06-15
**Author:** Sam Le Corre (spike)
**Status:** Draft — input for V3 / CP3 scoping
**Decision deadline:** before Genie PayGo go-live on 2026-07-06

## Summary

CP3 should ship against real `system.billing.usage` data (filtered on `billing_origin_product = 'AI/BI_GENIE'`), with a banner explaining the data is empty/sparse until 2026-07-06 and a Public Preview disclaimer on `system.ai_gateway.usage` if we use it for per-message token attribution. Do **not** build a "what you would have paid" inferred view — the only billable Genie surface today is the downstream SQL warehouse, which is unchanged by PayGo; the new charge is LLM-token cost that has no proxy in current data and any estimate would mislead users.

## (a) Real data source post-July-6

### Confirmed: `system.billing.usage`, `billing_origin_product = 'AI/BI_GENIE'`

- Genie (Spaces, Code, Databricks One) usage lands in the same global billable-usage system table customers already query.
  - Sources: Repsol roadmap note (2026-05-28) — *"Tag `billing_origin_product = 'AI/BI_GENIE'` en `system.billing.usage`"* — and Fonterra Throughput Decision Briefing (Confluence 6249349150).
  - Caveat: the canonical [Genie Products Pricing FAQ (go/geniepricing)](https://docs.google.com/document/d/15VSQ95Kejw0bPzz24g7OKoAFmM6vYkYpkNJT4pXV52E) snippet still uses the legacy label `GENIE`. The rename appears to coincide with PAYG go-live. Confirm with Weston Hutchins / Miranda Luna before merge.
- Pipeline: `model-serving-proxy → lumberjack → preprocessing (10-min windows) → abacus → system.billing.usage` (see Confluence [go/aigatewaybilling](https://databricks.atlassian.net/wiki/spaces/UN/pages/5077270730)). Latency ~2-3h, occasionally longer.
- Granularity: per (workspace, user, endpoint, AI Gateway feature) at 10-minute aggregation. Attribution to user via `identity_metadata.run_as`; attribution to Genie space via `usage_metadata.*` (exact subfield TBD until first PAYG records land — likely `usage_metadata.ai_gateway.*` per the Repsol note).

### Per-token detail: `system.ai_gateway.usage`

- Public, **regional, 365-day retention**, enabled by default on AI Gateway-fronted endpoints. Per-request rows with endpoint, requester, status, latency, token counts, routing info.
- Docs URL is `/ai-gateway/usage-tracking-beta` but the official [System tables reference](https://docs.databricks.com/aws/en/admin/system-tables/) lists it without a preview tag — treat as **Public Preview** in user-facing copy.
- Useful for per-message LLM cost attribution and a "free-credit burn-down" widget — only after Genie endpoints are fully behind AI Gateway V2 (already happening per [AI Gateway System Overview](https://databricks.atlassian.net/wiki/spaces/UN/pages/6182830092)).

### $10 free-credit accounting

- **150 DBUs (~$10.50 in US East) per user per month**, shared across Genie Spaces + Genie Code + Genie/Databricks One — *not* per-product.
- Enforced via **Unity AI Gateway Budgets**, GA at DAIS (2026-06-15 to 2026-06-18). Surfacing primitives:
  - [Manage budgets and cost controls for Genie](https://docs.databricks.com/aws/en/genie/budgets)
  - [Create and monitor budgets](https://docs.databricks.com/aws/en/admin/account-settings/budgets)
- **No public system table for the per-user free-credit ledger yet.** Reconciliation happens inside the abacus pipeline (pre-credit usage routes to `billable_usage_test`, not `billable_usage`). Per-user remaining credit only exposed via the Budgets UI/API today.

### Companion telemetry (already public)

- `system.access.assistant_events` — **Public Preview**, regional, tracks Genie Code messages (does not include Genie Spaces).
- `system.access.audit` with `service_name = 'aibiGenie'` — already used by `02_mvFactGenieUsage.sql`.

## (b) Inferred-today model

### What we *can* compute today

The repo already exposes `dbsql_cost_per_query` which attributes SQL warehouse DBUs per `statement_id`, with `query_source_type = 'GENIE SPACE'` and `query_source_id = genie_space_id` (`src/02_dbsql_cost_per_query.sql:88-90, 433`). `genie_observability_main_table` exposes the corresponding `statement_id` per message. Joining them produces warehouse-DBU cost per Genie message:

```sql
-- Genie SQL-warehouse cost attribution (pre-July-6 baseline)
SELECT
  g.space_id,
  g.space_name,
  g.user_email,
  date_trunc('month', g.created_datetime) AS usage_month,
  COUNT(*)                                     AS messages,
  SUM(cpq.query_attributed_dbus_estimation)    AS warehouse_dbus,
  SUM(cpq.query_attributed_dollars_estimation) AS warehouse_dollars
FROM {{catalog}}.{{schema}}.genie_observability_main_table g
LEFT JOIN {{catalog}}.{{schema}}.dbsql_cost_per_query_table cpq
  ON cpq.statement_id = g.statement_id
GROUP BY ALL;
```

### What we *cannot* compute and must not pretend to

PayGo's new charge is the **LLM-token cost** of Genie's reasoning model (Claude / GPT-5 / Llama via PPT). Today this LLM call is **free** and lands no rows in `system.billing.usage`. `system.ai_gateway.usage` also has no Genie attribution today because Genie's internal endpoints aren't customer-facing AI Gateway endpoints. Token counts per message are **not** in `genie_observability_main_table`.

A "what you would have paid" simulator would have to (1) estimate input tokens from question + space metadata, (2) estimate output tokens from `ai_response` + `sql_query`, (3) apply an assumed model + price. All three are guesses; combined error easily 3-5×. The internal Genie Paygo Cost Estimator (go/genie-paygo-sim) is being built by Product for exactly this — point customers there for forecasts.

### What we *can* honestly publish pre-July-6

- **Warehouse-DBU cost per Genie space / per user** (the SQL above), labelled *"SQL warehouse cost only; LLM cost begins charging 2026-07-06"*.
- **Free-credit reference** as a static line: *"150 DBUs / ~$10.50 per active Genie user per month"* with link to the AI Gateway Budgets docs.
- **Usage volume**: messages, conversations, spaces, active users (from `genie_observability_main_table` + `system.access.audit`).

## Recommendation for CP3

**Build A (real-data, dual-source):**

1. New MV `mvFactGeniePaygoCost` that `UNION ALL`s:
   - Warehouse DBUs from `dbsql_cost_per_query` filtered to `query_source_type = 'GENIE SPACE'` (pre- and post-July-6, unchanged).
   - LLM DBUs from `system.billing.usage WHERE billing_origin_product IN ('GENIE', 'AI/BI_GENIE')` (post-July-6 only; the `IN (...)` future-proofs the label rename).
2. Optional per-message token detail via `system.ai_gateway.usage` joined on requester + window — gate behind a config flag and label "Public Preview" in the dashboard.
3. Dashboard widget: stacked bar with two series (warehouse $ + LLM $), pre-July-6 the LLM series is 0; a date marker at 2026-07-06 with explanatory tooltip; a free-credit reference line per active user.
4. **Do not** ship a synthetic "what would you have paid" series.

Effort: ~1.5d MV + ~0.5d widget. Low risk — warehouse half is already proven; `AI/BI_GENIE` half is additive (empty until July 6, then populates automatically).

## Open questions / things to confirm with PM

1. **`billing_origin_product` final label.** `GENIE` vs `AI/BI_GENIE`. Confirm with Weston Hutchins / Miranda Luna.
2. **`usage_metadata` shape for Genie records.** Need the exact field — `endpoint_name`, `ai_gateway.endpoint_id`, or a new Genie-specific struct — to attribute records back to `space_id` / `user_id`. Likely visible in dogfood/staging this week.
3. **Genie Code vs Genie Spaces splits.** They share the 150 DBU pool — do they share a `billing_origin_product` value, or split via a `product_features.genie.*` sub-tag?
4. **Free-credit ledger access.** Is there an API or table for "per-user remaining credit this month"? Without it the dashboard can only show *spend*, not *headroom*.
5. **`system.ai_gateway.usage` GA timeline.** Docs URL still says `-beta`. If it doesn't GA before public-facing CP3 ship, don't depend on it for default widgets.
6. **Public-facing copy.** This is the public Dynosphere repo — confirm whether we can name AI Gateway / Budgets features that are still in Public Preview.

## Sources

- [Genie Products Pricing FAQ (go/geniepricing)](https://docs.google.com/document/d/15VSQ95Kejw0bPzz24g7OKoAFmM6vYkYpkNJT4pXV52E) — gdrive (snippet via Glean)
- [Genie Code — public docs](https://docs.databricks.com/aws/en/genie-code/)
- [What's coming? (Databricks on AWS)](https://docs.databricks.com/aws/en/release-notes/whats-coming)
- [Manage budgets and cost controls for Genie](https://docs.databricks.com/aws/en/genie/budgets)
- [AI Gateway Billing FAQ](https://databricks.atlassian.net/wiki/spaces/UN/pages/5130977327)
- [AI Gateway Billing (go/aigatewaybilling)](https://databricks.atlassian.net/wiki/spaces/UN/pages/5077270730)
- [AI Gateway System Overview](https://databricks.atlassian.net/wiki/spaces/UN/pages/6182830092)
- [Monitor usage for Unity AI Gateway endpoints](https://docs.databricks.com/aws/en/ai-gateway/usage-tracking-beta) — `system.ai_gateway.usage`
- [System tables reference](https://docs.databricks.com/aws/en/admin/system-tables/) — `system.access.assistant_events`, `system.ai_gateway.usage`
- [Introducing AI spend controls with Unity AI Gateway](https://www.databricks.com/blog/introducing-ai-spend-controls-unity-ai-gateway)
- Field corroboration: Repsol roadmap note (2026-05-28), Fonterra Throughput Decision Briefing (Confluence 6249349150), `#aibi-genie` Slack threads (Akshay / Weston, 2026-05/06)
- Internal cost estimator (do not link externally): go/genie-paygo-sim

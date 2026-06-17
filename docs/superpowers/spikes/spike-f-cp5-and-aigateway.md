# Spike S-F — CP5 reshape (Genie accuracy + feedback) and AI Gateway DAIS status

**Date:** 2026-06-15 (DAIS day 1) · **Author:** Sam Le Corre · **Status:** Findings only — read-only spike, no source changes · **Target:** V3 (before 2026-07-06 Genie PAYG)

## Summary

- **CP5 should ship as two thin tiles, not one heavy "accuracy" page.** Genie benchmark eval data is real, durable, and accessible per-space via the SDK / REST / CLI (`genie_list_eval_runs`, `genie_list_eval_results`, `genie_get_eval_result_details`) — but there is **no system table** for it, so we fan out per-space via SDK. User feedback is the easier win: `system.access.audit` already carries `updateConversationMessageFeedback`, `createConversationMessageComment`, etc., joinable to `space_id` + `message_id` + `user_email`. Effort drops from V3-original 4–6d to **~3–4d** if we keep both tiles tight and avoid building a custom eval-results historisation pipeline.
- **DAIS day-1 (today) net change for V3 is small.** Confirmed today: **Genie One GA**, **Genie Agents GA** (Genie Spaces rename), **Genie Ontology PuPr**, **Chat in Genie One GA**. **Unity AI Gateway Budgets is *not* GA today** — DAIS announcement is "PuPr at DAIS, GA expected early July" (per go/geniepricing and the FE DAIS resource page); this matches the V3 plan. `system.ai_gateway.usage` and `system.serving.endpoint_usage` status are unchanged (still Public Preview). **No new public system tables for Genie evals, MLflow traces, or Apps were announced today.** Genie Pay-as-you-go go-live remains 2026-07-06 with `billing_origin_product = 'AI/BI_GENIE'`. No V3 architecture change required from today's announcements.
- **One naming change to absorb across V3:** "Genie Spaces" → **Genie Agents** (go/whichgenie, 2026-06-15). "Genie" (the business-user UX, formerly Databricks One) → **Genie One**. Public-facing copy should say "Genie Agents (formerly Genie Spaces)" once in the dashboard intro, then use the new names; SDK/audit identifiers (`service_name='aibiGenie'`, `space_id`) are unchanged.

## Q1 — Genie accuracy + feedback findings

### 1a. Genie evaluation / benchmark API

**SDK + CLI surface (databricks-sdk-py v0.98+, CLI ≥ April 2026; see [genie command group](https://docs.databricks.com/aws/en/dev-tools/cli/reference/genie-commands)):**

| Operation | Inputs | Notes |
|---|---|---|
| `genie_create_eval_run(space_id, ...)` | `space_id`; request body selects all or a subset of benchmark question IDs and a mode (Chat / Agent) | Starts an eval run (runs in background). |
| `genie_list_eval_runs(space_id, page_size, page_token)` | per-space | Returns timestamped run summaries. Maps to the "Evaluations" tab in the UI. |
| `genie_get_eval_run(space_id, eval_run_id)` | per-run | Returns run status + aggregate accuracy. |
| `genie_list_eval_results(space_id, eval_run_id, page_size, page_token)` | per-run | Lists per-question results. |
| `genie_get_eval_result_details(space_id, eval_run_id, result_id)` | per-question | Full detail: model output, ground truth, assessment, error explanation. |

**Response shape (confirmed in `databricks/sdk/service/dashboards.py`, last touched 2026-06-11):**

- `GenieEvalAssessment` enum: `GOOD | BAD | NEEDS_REVIEW` — exactly the three buckets the UI ("Test and monitor a Genie Space" docs) shows.
- `EvaluationStatusType` enum used on `eval_run_status` — completed / paused / in-progress / unsuccessful / needs-review.
- Per-result fields visible in SDK: `benchmark_question_id`, `eval_run_status`, `expected_response: List[GenieEvalResponse]`, `manual_*` review fields, and the `actual` response. `GenieEvalResponse` carries the response content (text or SQL).
- Per-run rollup: an `accuracy` percentage across all benchmark questions in the run (matches the public-docs "Accuracy" column in the Evaluations tab).

**Public docs (`https://docs.databricks.com/aws/en/genie/monitor`, last updated 2026-06-11):**

- Customers set up benchmarks **in the Genie Space UI** ("Benchmarks" tab) — up to **500 questions per space**, with an optional gold-standard SQL answer (Chat mode) or an evaluation note (Agent mode).
- Two modes:
  - **Chat mode:** SQL-result comparison against the provided SQL Answer (deterministic).
  - **Agent mode:** LLM judge with optional evaluation note (LLM-as-judge).
- Customers run benchmarks via the UI ("Run benchmarks") or via the API (Benchmark APIs entered **Beta in March 2026**, still Beta today).
- **Retention caveat:** "The results of these responses appear in the evaluation details **for one week**. After one week, the results are no longer visible. The generated SQL statement and the example SQL statement remain." Per-run *summaries* (timestamp, accuracy %, execution status, created_by) persist longer in the Evaluations tab; only the detailed per-question response payload expires at 7 days.
- **Internal note:** an internal field PRD ("Genie Benchmarks v2") reports only **~7% of active Genie accounts** use benchmarks today — so a CP5 accuracy tile will be empty for the long tail of spaces. Plan copy accordingly.

**System-table coverage:** **none.** `system.access.audit` records `genieRunBenchmark` / `genieCreateEvalRun` style action_names (visible in eval-run audit traces), but there is **no `system.evaluation.*` or `system.access.genie_evals` table** published today and nothing announced for DAIS 2026. The internal eval harness ([Genie Evaluation System Architecture](https://databricks.atlassian.net/wiki/spaces/UN/pages/4805558322)) routes to MLflow + Logfood + a CloudFront eval viewer — internal-only, not a customer-facing data source.

### 1b. Genie feedback signal — `system.access.audit`

**Feedback-relevant `action_name` values when `service_name = 'aibiGenie'`:**

| action_name | Trigger | Useful `request_params` |
|---|---|---|
| `updateConversationMessageFeedback` | User submits a feedback rating on a Genie response (thumbs up/down). | `space_id`, `conversation_id`, `message_id`, `feedback_rating` |
| `createConversationMessageComment` | User adds a feedback comment ("Fix it" / "Request review") on a message. The newer `comment_type` field (added 2025) distinguishes "FIX_IT" vs "REQUEST_REVIEW" vs free comment. | `space_id`, `conversation_id`, `message_id`, `comment_type`, comment text fields |
| `updateConversationMessageComment` | User edits an existing comment. | same as above |
| `listConversationMessageComments` | Read access — useful for distinguishing producer vs consumer activity. | `space_id`, `conversation_ids[]`, `message_ids[]`, `user_ids[]`, `comment_types[]` |
| `genieSendMessageFeedback` | Legacy / API-only equivalent of the rating action (still emitted; used in older customer dashboards e.g. the Fonterra and Eurobank examples). | `space_id`, `feedback_rating`, `message_id` |

**Genie One (Chat) parallel events** (added 2026-05-14 — see AI/BI release notes):
`mcpToolInvocation`, `steerGenieChatConversation`, **`updateGenieChatConversationFeedback`**, `cancelGenieChatConversation`, `getGenieChatConversation`, plus four scheduled-task ops. Service name is `genieChat` (not `aibiGenie`). For V3 we should either (a) include both services in the feedback view via `service_name IN ('aibiGenie','genieChat')` or (b) split into two tiles. The two share a 150-DBU pool but are distinct activity streams.

**Attribution:** all five feedback events carry `user_identity.email`, `request_params.space_id`, `request_params.conversation_id`, and `request_params.message_id`. We can join `message_id` to `system.query.history.genie_space_id` for the SQL leg (already used in Spike A), but **`request_params.feedback_rating` is the metric** — no need to join.

**`system.access.assistant_events` (Public Preview, regional, 365-day retention):** tracks **Genie Code** (the dev-tool agent), **not** Genie Spaces / Genie Agents. Not a substitute for `system.access.audit` here.

### 1c. Recommended CP5 shape

**Ship two tiles, both small, on a new dashboard page "Quality":**

1. **User feedback tile (default — works for every customer day 1).**
   - Source: `system.access.audit` filtered on `service_name IN ('aibiGenie','genieChat')` and feedback action_names above.
   - Widget A (KPI row): "Thumbs up rate (30d)", "Comments / Fix-it rate (30d)", "Spaces with negative feedback (30d)".
   - Widget B (table): top spaces by negative feedback, with `space_id`, `feedback_count`, `negative_share`, `top_commenter`.
   - Caveat banner: "Audit log is in Public Preview; missing rows possible during 2025-10-16 → 2025-11-19 outage." (Same banner Spike A flagged.)
   - **Effort: 0.5d MV + 0.5d widget.** Pattern identical to existing `02_mvFactGenieUsage.sql`.

2. **Benchmark accuracy tile (opt-in for spaces with benchmarks).**
   - Source: SDK fan-out — `w.genie.genie_list_eval_runs(space_id)` per space in `dim_genie_space`, then `genie_get_eval_run(space_id, eval_run_id)` for run-level accuracy. Bounded `ThreadPoolExecutor(max_workers=5)`, same pattern as Spike A recommends. Each space typically has **0-20 eval runs**, so this is cheap (~1-2 calls/space, vs. the conversation/message N+1 we removed).
   - Persist run summaries (`run_id`, `space_id`, `created_at`, `created_by`, `accuracy`, `status`) into a dim table; refresh daily. **Do not** persist per-question detail (7-day retention upstream + low value for an overview dashboard).
   - Widget A (line chart): per-space accuracy over time (run timestamp on x-axis, accuracy % on y-axis).
   - Widget B (table): spaces with stale or no benchmarks ("last run >30d", "no benchmarks defined") — proactive nudge.
   - Caveat banner: "Many Genie spaces do not yet have benchmarks configured. See [Test and monitor a Genie Space](https://docs.databricks.com/aws/en/genie/monitor)."
   - **Effort: 1d notebook addition (per-space eval-run pull) + 1d widget. Benchmark APIs are still Beta — flag this in the public-facing copy.**

**What NOT to ship in CP5:**

- **No custom LLM judge / re-run engine.** Customers already have benchmarks + Inspect mode in-product; we're a monitoring dashboard, not an eval framework.
- **No per-question detail page.** 7-day retention makes it unreliable as a historical view, and the Evaluations tab in Genie already does this well.
- **No "accuracy of the space" calculated from feedback ratios.** Conflates two signals — keep them as separate widgets so users understand which is which (per Sam's framing).
- **No DSPy/Chaos Llama recommendations.** Internal-only field tooling, not for public repo.

**Effort total: ~3–4 days** (vs. original V3 ticket #13 at 4–6d). Reshape saves ~1–2d because we drop the "build accuracy gathering SDK" line — it already exists.

## Q2 — DAIS day-1 status check (announcements relevant to V3)

**Confirmed today (2026-06-15) — relevant to V3:**

- **Genie One — GA** (workspace + account level). Already in our scope. No schema impact.
- **Genie Agents — GA** (rename of Genie Spaces). Public copy must say "Genie Agents (formerly Genie Spaces)" once; identifiers unchanged.
- **Chat in Genie One — GA** (was Public Preview Apr 29). Adds the `genieChat` audit-event family Spike A already accounted for.
- **Genie Ontology — PuPr.** Auto-generated context graph. **Not** customer-API-accessible per PM Elise Georis (2026-06-12) — agents only consume it via MCP. **Out of scope for V3.**
- **Documents in Genie One — GA.** Adds drafted documents; out of scope for V3.

**Confirmed today — *not* GA but still V3-adjacent:**

- **Unity AI Gateway Budgets — "Announcing / PuPr at DAIS, Budgets GA expected early July."** Matches Spike S-B's assumption. **No action.** Keep public-facing copy as "AI Gateway Budgets (Public Preview as of DAIS; GA early July 2026)."
- **`system.ai_gateway.usage` — status unchanged** (still Public Preview).
- **`system.serving.endpoint_usage` — status unchanged** (still Public Preview, 90-day retention, requires opt-in per endpoint).
- **`system.access.assistant_events` — status unchanged** (Public Preview, Genie Code only).
- **Genie ZeroOps (formerly Autopilot) — PrPr in July.** Not in V3 scope.

**Explicitly NOT announced today (despite DAIS):**

- No GA for `system.ai_gateway.usage`, `system.serving.endpoint_usage`, or `system.access.assistant_events`.
- No `system.mlflow.traces` or `system.evaluation.*` table.
- No `system.apps.apps` (apps system table) public announcement.
- No Genie pay-as-you-go schema/SKU change — still arriving 2026-07-06 with `billing_origin_product = 'AI/BI_GENIE'`.
- No public Genie evaluations system table.

**Net change for V3: zero schema/architecture changes.** Two cosmetic renames (Genie Spaces → Genie Agents; Databricks One UX → Genie One) are the only content edits required.

**Things to watch later this week (Jun 16-18):**

- DAIS day-2/3 keynotes may surface a Budgets GA announcement. If it ships during DAIS, the "PuPr" disclaimer in CP3 can be downgraded to "GA at DAIS 2026-06-XX" — copy edit only.

## Open questions / things to confirm with PM

1. **Eval-run persistence beyond 7 days.** Public docs say per-question results expire at 7 days. If V3 wants a >7d accuracy trend, we **must** historise via the SDK fan-out (the recommended path) — confirm this is acceptable as the only V3-visible accuracy source.
2. **Benchmark API Beta status by ship date.** APIs were Beta in March 2026 and still Beta today (no GA noted in June release notes). Confirm with Hanlin Sun (PM, Benchmarks) whether they're expected to GA before V3 ships. If they remain Beta on ship day, the accuracy tile needs a "Public Preview" disclaimer.
3. **`genieChat` vs `aibiGenie` in feedback widgets.** Merge or split? My recommendation: **merge** with a `surface` column (`spaces` / `chat`) and let users facet. PM should sign off — Genie Agents and Genie One are pitched separately at DAIS, so separate widgets may be more on-message.
4. **Adoption-rate copy.** Internal PRD says ~7% of spaces use benchmarks — internal number, can't surface verbatim. Acceptable to say "Many Genie spaces do not yet have benchmarks configured"? PM to confirm tone.
5. **Genie Ontology / Inspect mode**: deliberately scoped out of CP5 because they're either non-API (Ontology) or already in-product (Inspect). Confirm PM agrees and doesn't want a "uses Inspect" badge per-space.
6. **AI Gateway Budgets surfacing on the Quality page?** Budget headroom relates more to CP3 (cost) than CP5 (quality). Confirm we leave it in CP3.

## Sources

- [Test and monitor a Genie Space](https://docs.databricks.com/aws/en/genie/monitor) — public docs, last updated 2026-06-11.
- [genie command group | Databricks on AWS](https://docs.databricks.com/aws/en/dev-tools/cli/reference/genie-commands)
- [databricks-sdk-py v0.98+ commit](https://github.com/databricks/databricks-sdk-py/commit/afc25964aea4a3424c88b5427bb34717dcea5beb)
- `databricks/sdk/service/dashboards.py` — `GenieEvalAssessment`, `GenieEvalResponse`, `EvaluationStatusType`, `benchmark_question_id`, `eval_run_status`, `expected_response`, `manual_*` fields.
- [Audit log reference](https://docs.databricks.com/aws/en/admin/account-settings/audit-logs)
- [Monitor Genie Spaces usage with audit logs and alerts](https://docs.databricks.com/aws/en/ai-bi/admin/audit)
- [AI/BI and Genie One release notes 2026](https://docs.databricks.com/aws/en/ai-bi/release-notes/2026)
- [System tables reference](https://docs.databricks.com/aws/en/admin/system-tables/)
- [DAIS 2026 FE Resource Directory](https://databricks.atlassian.net/wiki/spaces/FE/pages/6446252033)
- [Genie Products Pricing FAQ (go/geniepricing)](https://docs.google.com/document/d/15VSQ95Kejw0bPzz24g7OKoAFmM6vYkYpkNJT4pXV52E)
- [URGENT - Fonterra Genie API Throughput Decision Briefing](https://databricks.atlassian.net/wiki/spaces/FE/pages/6249349150)
- Spike S-A (`spike-a-system-tables-sdk.md`) — SDK fan-out pattern.
- Spike S-B (`spike-b-genie-paygo.md`) — `system.ai_gateway.usage`, Budgets, billing.

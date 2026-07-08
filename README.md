# Databricks Adoption Dashboard

The **Databricks Adoption Dashboard** provides an **out-of-the-box capability** to analyse and visualise adoption metrics across your organisation’s **AI/BI Dashboards, Genie Spaces, Apps, and Models**.  
It helps data teams, business stakeholders, and platform owners quickly understand how Databricks is being used, where adoption is growing, and where additional enablement might be needed.

## Key Features
- 📊 **Unified View** – Track usage and adoption trends across dashboards, **Genie Agents (formerly Genie Spaces)**, AI apps, and machine-learning models in one place.  
- ⚡ **Plug-and-Play** – Pre-built notebooks, data models, and visualisations to get started with minimal setup.  
- 🔍 **Drill-Down Analysis** – Drill into specific dashboards and Genie Agents (coming soon) to view granular details


## Quick Start
1. Create a git folder in your Databricks workspace OR download the zip and import to your workspace
2. You need to update the variables for your parameters. There are two approaches here:
    1. If you're deploying into one workspace, we recommend updating `/deployment_resources/variables.yml` defaults directly with your `warehouse_id`, `catalog_name` and `schema_name`.
    2. If you're deploying to many workspaces, you should set these values as variable overrides during deployment as part of your CICD process. See [this doc link](https://docs.databricks.com/aws/en/dev-tools/bundles/variables#set-a-variables-value)
3. Deploy the Asset Bundle
4. Run the adoption_dashboard_workflow for the first time to populate your data.
5. Set the adoption_dashboard_workflow job to run as needed for your frequency.
6. You're now good to go!

**Optional configuration:**
- Set `enable_genie_deep_dive=true` to populate the deep-dive Genie observability panel (slower; pulls message content via the SDK).
- Set `force_reinit=true` to force reinitialize the data models and schemas (useful for fresh deployments or schema resets).

Tip: Works seamlessly with Unity Catalog and Databricks SQL Warehouses.

⸻

## Deploying across multiple workspaces

The dashboard ships in **per-workspace** mode by default — one deployment per workspace, one copy of the data in each workspace's catalog. For customers with several workspaces who want a single consolidated view, deploy each instance pointed at a **shared Unity Catalog** schema.

1. Pick a shared catalog (e.g. `governance.adoption`).
2. Grant `USE CATALOG`, `CREATE SCHEMA`, `MODIFY` on the schema to each workspace's deploy identity.
3. Set `catalog_name` to the shared catalog in every workspace's bundle deployment.
4. Each workspace's job MERGEs only its own `workspace_id`-tagged rows — no duplication.

> **Tip:** Account-level **Genie One** already provides cross-workspace **discovery** (single search across dashboards, Genie Agents, and Apps from one entry point). The adoption dashboard is the **analytics** layer on top.

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


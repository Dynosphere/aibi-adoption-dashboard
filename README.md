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

## Requirements

- Databricks Runtime 13.x or later
- Unity Catalog enabled
- Access to your organisation’s usage and audit logs via system tables


## Contributing

Contributions are welcome!

Please open an issue or submit a pull request to propose enhancements, bug fixes, or new visualisations.

## Disclaimer

This code is not endorsed by or affiliated in any way with Databricks. Use it at your own risk and review everything before using it.


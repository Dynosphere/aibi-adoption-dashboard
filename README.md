# Databricks Adoption Dashboard

The **Databricks Adoption Dashboard** provides an **out-of-the-box capability** to analyse and visualise adoption metrics across your organisation’s **AI/BI Dashboards, Genie Spaces, Apps, and Models**.  
It helps data teams, business stakeholders, and platform owners quickly understand how Databricks is being used, where adoption is growing, and where additional enablement might be needed.

## Key Features
- 📊 **Unified View** – Track usage and adoption trends across dashboards, Genie spaces, AI apps, and machine-learning models in one place.  
- ⚡ **Plug-and-Play** – Pre-built notebooks, data models, and visualisations to get started with minimal setup.  
- 🔍 **Drill-Down Analysis** – Drill into specific dashboards and genie spaces (coming soon) to view granular details


## Quick Start
1. Create a git folder in your Databricks workspace OR download the zip and import to your workspace
2. Update the variable overrides with your datawarehouse id to be used to run the audit queries
3. Deploy the Asset Bundle
4. Run the adoption_dashboard_workflow job, this will update the necessary parameters
5. Open the Adoption Dashboard dashboard, click publish. Open the Adaoption Dashbroad drillthrough dashboard, click publish
6. Set the adoption_dashboard_workflow job to run as needed for your frequency.
7. Run the adoption_dashboard_workflow for the first time to populate your data.
8. You're now good to go!

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


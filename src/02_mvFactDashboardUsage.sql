CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name||'.'||:schema_name||'.mvFactDashboardUsage')
AS
SELECT
D.dashboard_id,
D.display_name dashboard_name,
au.event_date viewed_date,
au.workspace_id workspace_id,
w.workspace_name workspace_name,
COUNT(event_date) AS num_views,
sum(case when action_name = 'getDashboard' then 1 else 0 end) DraftDashboardViews,
sum(case when action_name = 'getPublishedDashboard' then 1 else 0 end) PublishedDashboardViews
FROM IDENTIFIER(:catalog_name||'.'||:schema_name||'.adb_dashboards') D LEFT JOIN system.access.audit au
on au.request_params.dashboard_id = D.dashboard_id
inner join system.access.workspaces_latest w on au.workspace_id = w.workspace_id
WHERE service_name = 'dashboards'
  and action_name IN ('getDashboard', 'getPublishedDashboard')
  AND event_time > now() - interval '180 day'
  group by 
  d.dashboard_id,
  D.display_name,
  au.event_date,
  au.workspace_id,
  w.workspace_name




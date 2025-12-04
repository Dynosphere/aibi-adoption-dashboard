CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name||'.'||:schema_name||'.mvFactAppUsage')
AS
SELECT
A.name app_name,
A.id app_id,
au.event_date viewed_date,
au.workspace_id workspace_id,
w.workspace_name workspace_name,
COUNT(event_date) AS num_views,
sum(case when action_name like '%getApp%' or action_name like '%getApplication%' then 1 else 0 end) AS num_app_accesses,
sum(case when action_name like '%deployApp%' or action_name like '%deployApplication%' then 1 else 0 end) AS num_deployments,
sum(case when action_name like '%updateApp%' or action_name like '%updateApplication%' then 1 else 0 end) AS num_updates
FROM IDENTIFIER(:catalog_name||'.'||:schema_name||'.adb_apps') A LEFT JOIN system.access.audit au
on (au.request_params.app_name = A.name 
    OR au.request_params.name = A.name
    OR au.request_params.app_id = A.id
    OR au.request_params.request_object_id = A.id
    OR au.request_params.application_id = A.id)
left join system.access.workspaces_latest w on au.workspace_id = w.workspace_id
WHERE service_name = 'apps'
  AND event_time > now() - interval '180 day'
group by 
  A.name,
  A.id,
  au.event_date,
  au.workspace_id,
  w.workspace_name


CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name||'.'||:schema_name||'.mvFactModelUsage')
AS
SELECT
M.full_name model_full_name,
M.name model_name,
M.catalog_name,
M.schema_name,
au.event_date viewed_date,
au.workspace_id workspace_id,
w.workspace_name workspace_name,
COUNT(event_date) AS num_views,
sum(case when action_name like '%getModel%' or action_name like '%getRegisteredModel%' then 1 else 0 end) AS num_model_accesses,
sum(case when action_name like '%getModelVersion%' or action_name like '%getRegisteredModelVersion%' then 1 else 0 end) AS num_model_version_accesses,
sum(case when action_name like '%predict%' or action_name like '%inference%' then 1 else 0 end) AS num_inferences
FROM IDENTIFIER(:catalog_name||'.'||:schema_name||'.adb_models') M LEFT JOIN system.access.audit au
on (au.request_params.model_name = M.full_name 
    OR au.request_params.full_name = M.full_name
    OR au.request_params.name = M.name
    OR au.request_params.registered_model_id = M.full_name)
left join system.access.workspaces_latest w on au.workspace_id = w.workspace_id
WHERE (service_name = 'model-registry' OR service_name = 'models' OR service_name LIKE '%model%')
  AND event_time > now() - interval '180 day'
group by 
  M.full_name,
  M.name,
  M.catalog_name,
  M.schema_name,
  au.event_date,
  au.workspace_id,
  w.workspace_name


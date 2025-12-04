CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name||'.'||:schema_name||'.mvFactServingEndpointUsage')
AS
SELECT
SE.name endpoint_name,
SE.id endpoint_id,
au.event_date viewed_date,
au.workspace_id workspace_id,
w.workspace_name workspace_name,
COUNT(event_date) AS num_views,
sum(case when action_name like '%getServingEndpoint%' or action_name like '%getEndpoint%' then 1 else 0 end) AS num_endpoint_accesses,
sum(case when action_name like '%query%' or action_name like '%invoke%' or action_name like '%predict%' or action_name like '%inference%' then 1 else 0 end) AS num_inference_requests
FROM IDENTIFIER(:catalog_name||'.'||:schema_name||'.adb_serving_endpoints') SE LEFT JOIN system.access.audit au
on (au.request_params.endpoint_name = SE.name 
    OR au.request_params.name = SE.name
    OR au.request_params.serving_endpoint_id = SE.id
    OR au.request_params.endpoint_id = SE.id)
left join system.access.workspaces_latest w on au.workspace_id = w.workspace_id
WHERE (service_name = 'serving-endpoints' OR service_name = 'serving' OR service_name LIKE '%serving%')
  AND event_time > now() - interval '180 day'
group by 
  SE.name,
  SE.id,
  au.event_date,
  au.workspace_id,
  w.workspace_name


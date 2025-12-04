CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name|| '.'|| :schema_name ||'.mvFactGenieUsage')
SELECT
g.space_Id,
g.name genie_space,
au.event_date viewed_date,
au.workspace_id workspace_id,
w.workspace_name workspace_name,
COUNT(event_date) AS num_views,
sum(case when action_name = 'createSpace' then 1 else 0 end) AS num_spaces_created,
sum(case when action_name = 'getSpace' then 1 else 0 end) AS num_spaces_accessed,
sum(case when action_name = 'createConversation' then 1 else 0 end) AS num_chats_created,
sum(case when action_name ='genieSendMessageFeedback' then 1 else 0 end) As Feedback_raised
FROM IDENTIFIER(:catalog_name|| '.'|| :schema_name ||'.adb_genie_spaces') g LEFT JOIN system.access.audit au
on au.request_params.space_id = g.space_id
left join system.access.workspaces_latest w on au.workspace_id = w.workspace_id
WHERE service_name = 'aibiGenie'
AND event_time > now() - interval '180 day'
group by 
 g.space_id,
  g.name,
  au.event_date,
  au.workspace_id,
  w.workspace_name

--select * from system.access.audit where service_name = 'aibigenie'


# V3 system-table validation
**Workspace:** https://<your-workspace>.cloud.databricks.com
**Warehouse:** Shared endpoint (`8baced1ff014912d`)
**Generated:** by `scripts/validate_system_tables.py`

This document is the **source of truth** for column names/types when V3 MERGE statements reference system tables. Update it whenever the spec adds a new system-table dependency.

## `system.access.audit` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | The Databricks account identifier associated with the audit event. |
| `workspace_id` | `string` | The Databricks workspace in which the audit event took place. For account-level events, the workspace_id will be 0. |
| `version` | `string` | Audit log schema version |
| `event_time` | `timestamp` | Timestamp of the event |
| `event_date` | `date` | The calendar date when the event or action occurred. Useful for filtering and aggregating events by day. |
| `source_ip_address` | `string` | The IP address from which the request originated. |
| `user_agent` | `string` | The user agent string or identifier describing the client, browser, or tool that initiated the request (e.g., web browser, API client, CLI) |
| `session_id` | `string` | Unique identifier for the session in which the request was made. Sessions group related actions by a user or service over a period of time |
| `user_identity` | `struct<email:string,subject_name:string>` | A map of key/value pairs that can be used to identify the person or service principal that initiated the request. Contains keys such as email and subject_name. An email of System-User is used for background system tasks. |
| `service_name` | `string` | The name of the Databricks service that generated the audit event. See [service documentation](https://docs.databricks.com/aws/admin/account-settings/audit-logs#audit-log-services) for details. |
| `action_name` | `string` | The name of the action that has been performed as part of the audit event. Action names vary depending on the Databricks service (service_name). See [documentation of actions per service](https://docs.databricks.com/aws/admin/account-settings/audit-logs) for details. |
| `request_id` | `string` | A unique identifier of the request. |
| `request_params` | `map<string,string>` | A map of key/value pairs with the request parameters. Request parameters vary by request type. See [documentation of request parameters per action](https://docs.databricks.com/aws/admin/account-settings/audit-logs) for details. |
| `response` | `struct<status_code:int,error_message:string,result:string>` | Struct of response, includes status_code, error_messages, and where applicable a result string. status_code is an HTTP status code. |
| `audit_level` | `string` | Indicates whether the audit event is at the workspace or account level. Either `ACCOUNT_LEVEL` or `WORKSPACE_LEVEL`. |
| `event_id` | `string` | A unique identifier of the audit event. |
| `identity_metadata` | `struct<run_by:string,run_as:string,acting_resource:string,run_by_display_name:string,run_as_display_name:string>` | A struct with the identities from the audit event. This includes `run_by` (the identity who initiated the event) and `run_as` (the identity being used for authorisation purposes). See [Auditing group dedicated compute activity documentation](https://docs.databricks.com/aws/compute/group-access#auditing-group-dedicated-compute-activity) for more details. |
| `Catalog` | `system` |  |
| `Database` | `access` |  |
| `Table` | `audit` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Tue Jul 04 10:59:48 UTC 2023` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The audit logs table records detailed information about actions and events occurring within a Databricks workspace or account. It is used for security auditing, compliance, and monitoring user and system activity. Each row represents a single event, such as a user login, data access, permission change, or system operation. See documentation of services and actions at https://docs.databricks.com/aws/admin/account-settings/audit-logs` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1770143957000, delta.lastUpdateVersion=945613, delta.minReaderVersion=1, delta.minWriterVersion=4]` |  |
| `Statistics` | `32449250438991 bytes, 229743971240 rows` |  |
| `Location` | `uc-deltasharing://system.access.audit#system.access.audit` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.access.workspaces_latest` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | The ID of the parent account of this workspace. |
| `workspace_id` | `string` | The ID of the workspace. |
| `workspace_name` | `string` | The human-readable name of the workspace. |
| `workspace_url` | `string` | URL of the databricks workspace. |
| `create_time` | `timestamp` | Timestamp of when the workspace was created. |
| `status` | `string` | Lifecycle status of the workspace: NOT_PROVISIONED \| PROVISIONING \| RUNNING \| FAILED \| BANNED |
| `Catalog` | `system` |  |
| `Database` | `access` |  |
| `Table` | `workspaces_latest` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Tue Jul 15 13:28:04 UTC 2025` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The workspaces system table is a slow-changing dimension table of metadata for all the workspaces in your account.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableDeletionVectors=false, delta.lastCommitTimestamp=1766565083000, delta.lastUpdateVersion=5171, delta.minReaderVersion=1, delta.minWriterVersion=2, delta.workloadBasedColumns.optimizerStatistics=`workspace_id`]` |  |
| `Statistics` | `390521701 bytes, 171536 rows` |  |
| `Location` | `uc-deltasharing://system.access.workspaces_latest#system.access.workspaces_latest` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.access.assistant_events` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | ID of the account. |
| `workspace_id` | `string` | ID of the workspace. |
| `event_id` | `string` | A unique ID for this event. |
| `event_time` | `timestamp` | Time that the event happened. Timezone information is recorded at the end of the value with +00:00 representing UTC. |
| `event_date` | `date` | Date that the event happened. |
| `user_agent` | `string` | Origination of request. |
| `initiated_by` | `string` | Email of the user initiating the request. |
| `Catalog` | `system` |  |
| `Database` | `access` |  |
| `Table` | `assistant_events` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Tue Jul 09 22:11:57 UTC 2024` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `Records all chat or inline Assistant interactions in order to see engagement and adoption.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1776384439000, delta.lastUpdateVersion=192906, delta.minReaderVersion=1, delta.minWriterVersion=4, delta.workloadBasedColumns.optimizerStatistics=`workspace_id`,`account_id`]` |  |
| `Statistics` | `2838424146 bytes, 5216334 rows` |  |
| `Location` | `uc-deltasharing://system.access.assistant_events#system.access.assistant_events` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.access.table_lineage` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | The id of the Databricks account. |
| `metastore_id` | `string` | The id of the Unity Catalog metastore. |
| `workspace_id` | `string` | The id of the workspace |
| `entity_type` | `string` | The type of entity the lineage transaction was captured from. The value is NOTEBOOK, JOB, PIPELINE, DASHBOARD_V3 (Dashboard), DBSQL_DASHBOARD (Legacy dashboard), DBSQL_QUERY, OR NULL. |
| `entity_id` | `string` | The ID of the entity the lineage transaction was captured from. If entity_type is NULL, entity_id is NULL. |
| `entity_run_id` | `string` | id to describe the unique run of the entity, or NULL. This differs for each entity type:

Notebook: command_run_id

Job: job_run_id

Databricks SQL query: statement_id

Dashboard: statement_id

Legacy dashboard: statement_id

Pipeline: pipeline_update_id

If entity_type is NULL, entity_run_id is NULL. Records with statement_id and job_run_id can be joined with the query history and jobs system tables respectively. |
| `source_table_full_name` | `string` | Three-part name to identify the source table. |
| `source_table_catalog` | `string` | The catalog of the source table. |
| `source_table_schema` | `string` | The schema of the source table. |
| `source_table_name` | `string` | The name of the source table. |
| `source_path` | `string` | Location in cloud storage of the source table, or the path if it’s reading from cloud storage directly. |
| `source_type` | `string` | The type of the source. The value is TABLE, PATH, VIEW, MATERIALIZED_VIEW, METRIC_VIEW, or STREAMING_TABLE. |
| `target_table_full_name` | `string` | Three-part name to identify the target table. |
| `target_table_catalog` | `string` | The catalog of the target table. |
| `target_table_schema` | `string` | The schema of the target table. |
| `target_table_name` | `string` | The name of the target table. |
| `target_path` | `string` | Location in cloud storage of the target table. |
| `target_type` | `string` | The type of the target. The value is TABLE, PATH, VIEW, MATERIALIZED_VIEW, METRIC_VIEW, or STREAMING_TABLE. |
| `created_by` | `string` | The user who generated this lineage. This can be a Databricks username, a Databricks service principal ID, a Databricks group name, “System-User”, or NULL if the user information cannot be captured. |
| `event_time` | `timestamp` | The timestamp when the lineage was generated. Timezone information is recorded at the end of the value with +00:00 representing UTC. |
| `event_date` | `date` | The date when the lineage was generated. This is a partitioned column. |
| `record_id` | `string` | Primary key of each row, it is auto-generated and cannot be joined with any tables |
| `event_id` | `string` | One query or one spark job run could append multiple lineage rows, this event_id is a unique id to group the rows that belong to the same event. This is generated in the pipeline and cannot be joined with any tables. |
| `statement_id` | `string` | A foreign key to join with query history system table. It is set when a query is from a warehouse or serverless warehouse. |
| `entity_metadata` | `struct<job_info:struct<job_id:string,job_run_id:string>,dashboard_id:string,legacy_dashboard_id:string,notebook_id:string,sql_query_id:string,dlt_pipeline_info:struct<dlt_pipeline_id:string,dlt_update_id:string>,genie_space_id:string,alert_id:string>` | It is a list of ids of the query context which is joinable with other system tables. |
| `direct_access` | `boolean` | Indicates whether the lineage relationship is a direct access. False if the access is indirect (e.g. via view expansion). |
| `Catalog` | `system` |  |
| `Database` | `access` |  |
| `Table` | `table_lineage` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Tue Jul 04 10:59:48 UTC 2023` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The table lineage system table includes a record for each read or write event (Spark operation) on a UC table or path. This includes but is not limited to job, notebook, and dashboard runs. This table can be joined with other system tables such as Query History and Jobs to get additional information about the read/write operation. Please refer to the [documentation](https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html#limitations) for limitations.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1775351288000, delta.lastUpdateVersion=452397, delta.minReaderVersion=1, delta.minWriterVersion=4]` |  |
| `Statistics` | `2436251687369 bytes, 28403795999 rows` |  |
| `Location` | `uc-deltasharing://system.access.table_lineage#system.access.table_lineage` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.query.history` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | ID of the account. |
| `workspace_id` | `string` | The ID of the workspace where the query was run. |
| `statement_id` | `string` | The ID that uniquely identifies the execution of the statement. You can use this ID to find the statement execution in the Query History UI. |
| `executed_by` | `string` | The email address or username of the user who ran the statement. |
| `session_id` | `string` | The Spark session ID. |
| `execution_status` | `string` | The statement termination state. Possible values are:

FINISHED: execution was successful

FAILED: execution failed with the reason for failure described in the accompanying error message

CANCELED: execution was canceled |
| `compute` | `struct<type:string,cluster_id:string,warehouse_id:string>` | A struct that represents the type of compute resource used to run the statement and the ID of the resource where applicable. The type value will be WAREHOUSE. |
| `executed_by_user_id` | `string` | The ID of the user who ran the statement. |
| `statement_text` | `string` | Text of the SQL statement. If you have configured customer-managed keys, statement_text is empty. |
| `statement_type` | `string` | The statement type. For example: ALTER, COPY, and`INSERT`. |
| `error_message` | `string` | Message describing the error condition. If you have configured customer-managed keys, error_message is empty. |
| `client_application` | `string` | Client application that ran the statement. For example: Databricks SQL, Tableau, and Power BI. |
| `client_driver` | `string` | The connector used to connect to Databricks to run the statement. For example: Databricks SQL Driver for Go, Databricks ODBC Driver, Databricks JDBC Driver. |
| `total_duration_ms` | `bigint` | Total execution time of the statement in milliseconds ( excluding result fetch time ). |
| `waiting_for_compute_duration_ms` | `bigint` | Time spent waiting for compute resources to be provisioned in milliseconds. |
| `waiting_at_capacity_duration_ms` | `bigint` | Time spent waiting in queue for available compute capacity in milliseconds. |
| `execution_duration_ms` | `bigint` | Time spent executing the statement in milliseconds. |
| `compilation_duration_ms` | `bigint` | Time spent loading metadata and optimizing the statement in milliseconds. |
| `total_task_duration_ms` | `bigint` | The sum of all task durations in milliseconds. This time represents the combined time it took to run the query across all cores of all nodes. It can be significantly longer than the wall-clock duration if multiple tasks are executed in parallel. It can be shorter than the wall-clock duration if tasks wait for available nodes. |
| `result_fetch_duration_ms` | `bigint` | Time spent, in milliseconds, fetching the statement results after the execution finished. |
| `start_time` | `timestamp` | The time when Databricks received the request. Timezone information is recorded at the end of the value with +00:00 representing UTC. |
| `end_time` | `timestamp` | The time the statement execution ended, excluding result fetch time. Timezone information is recorded at the end of the value with +00:00 representing UTC. |
| `update_time` | `timestamp` | The time the statement last received a progress update. Timezone information is recorded at the end of the value with +00:00 representing UTC. |
| `read_partitions` | `bigint` | The number of table partitions read after table partition pruning. |
| `pruned_files` | `bigint` | The number of pruned files during table partition and file pruning. |
| `read_files` | `bigint` | The number of files read after table partition and file pruning. |
| `read_rows` | `bigint` | The number of rows output from scans after table partition, file, in file and row pruning. |
| `produced_rows` | `bigint` | Total number of rows returned by the statement. |
| `read_bytes` | `bigint` | The number of bytes of files read after IO pruning. |
| `read_io_cache_percent` | `tinyint` | The percentage of bytes of persistent data read from the IO cache. |
| `from_result_cache` | `boolean` | TRUE indicates that the statement result was fetched from the cache. |
| `spilled_local_bytes` | `bigint` | Size of data, in bytes, temporarily written to disk while executing the statement. |
| `written_bytes` | `bigint` | The size in bytes of persistent data written to cloud object storage. |
| `shuffle_read_bytes` | `bigint` | The total amount of data in bytes sent over the network. |
| `query_source` | `struct<job_info:struct<job_id:string,job_run_id:string,job_task_run_id:string>,legacy_dashboard_id:string,dashboard_id:string,alert_id:string,notebook_id:string,sql_query_id:string,genie_space_id:string,pipeline_info:struct<pipeline_id:string,update_id:string>>` | A struct that contains key-value pairs representing one or more Databricks entities that were involved in the execution of this statement, such as jobs, notebooks, or dashboards. This field only records Databricks entities and are not sorted by execution order. Statement executions that contain multiple IDs indicate that the execution was triggered by multiple entities: for example, an Alert may trigger on a Job result and call a SQL Query, so all three IDs will be populated within query_source. |
| `executed_as_user_id` | `string` | The ID of the user or service principal whose privilege was used to run the statement. |
| `executed_as` | `string` | The name of the user or service principal whose privilege was used to run the statement. |
| `written_rows` | `bigint` | Total number of rows of persistent data written to cloud object storage. |
| `written_files` | `bigint` | The number of files of persistent data written to cloud object storage. |
| `cache_origin_statement_id` | `string` | Statement id of query that inserted result in cache when result is fetched from cache, otherwise store the query's statement id. |
| `query_parameters` | `struct<named_parameters:map<string,struct<exprs:array<struct<tag:string,data_type:struct<type_name:string,type_ddl:string,redacted_type_ddl:string>,literal:struct<data_type:struct<type_name:string,type_ddl:string,redacted_type_ddl:string>,int_value:int,long_value:bigint,double_value:double,float_value:float,byte_value:int,short_value:int,string_value:string,boolean_value:boolean,decimal_value:struct<value:string,precision:int,scale:int>,binary_value:binary,timestamp_value:bigint,timestamp_ntz_value:bigint,date_value:bigint,calendar_interval_value:struct<months:int,days:int,microseconds:bigint>,day_time_interval_value:bigint,year_month_interval_value:int>,seq_expression:struct<children:array<bigint>>,function:struct<function_name:string,arguments:array<bigint>>,alias:struct<expr:bigint,name:array<string>,metadata:string>,sort_order:struct<child:bigint,direction:string,null_ordering:string>,lambda_function:struct<function:bigint,arguments:array<struct<name_parts:array<string>>>>,window:struct<window_function:bigint,partition_spec:array<bigint>,order_spec:array<bigint>>,unresolved_extract_value:struct<child:bigint,extraction:bigint>,update_fields:struct<struct_expression:bigint,field_name:string,value_expression:bigint>,unresolved_named_lambda_variable:struct<name_parts:array<string>>,named_argument_expression:struct<key:string,value:bigint>,id:bigint>>>>,pos_parameters:array<struct<exprs:array<struct<tag:string,data_type:struct<type_name:string,type_ddl:string,redacted_type_ddl:string>,literal:struct<data_type:struct<type_name:string,type_ddl:string,redacted_type_ddl:string>,int_value:int,long_value:bigint,double_value:double,float_value:float,byte_value:int,short_value:int,string_value:string,boolean_value:boolean,decimal_value:struct<value:string,precision:int,scale:int>,binary_value:binary,timestamp_value:bigint,timestamp_ntz_value:bigint,date_value:bigint,calendar_interval_value:struct<months:int,days:int,microseconds:bigint>,day_time_interval_value:bigint,year_month_interval_value:int>,seq_expression:struct<children:array<bigint>>,function:struct<function_name:string,arguments:array<bigint>>,alias:struct<expr:bigint,name:array<string>,metadata:string>,sort_order:struct<child:bigint,direction:string,null_ordering:string>,lambda_function:struct<function:bigint,arguments:array<struct<name_parts:array<string>>>>,window:struct<window_function:bigint,partition_spec:array<bigint>,order_spec:array<bigint>>,unresolved_extract_value:struct<child:bigint,extraction:bigint>,update_fields:struct<struct_expression:bigint,field_name:string,value_expression:bigint>,unresolved_named_lambda_variable:struct<name_parts:array<string>>,named_argument_expression:struct<key:string,value:bigint>,id:bigint>>>>,truncated:boolean>` | Query parameters values associated with the query. Only one of named_parameters or pos_parameters can be populated. |
| `query_tags` | `map<string,string>` | The user-supplied custom tags associated with this statement execution. |
| `pruned_files_bytes` | `bigint` | The number of bytes of files pruned after table partition and file pruning. |
| `read_files_bytes` | `bigint` | The number of bytes of files read after table partition and file pruning. |
| `Catalog` | `system` |  |
| `Database` | `query` |  |
| `Table` | `history` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Mon Feb 12 22:16:50 UTC 2024` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The query history table, located at system.query.history, includes records for every SQL statement run using SQL warehouses. The table includes account-wide records from all workspaces in the same region from which you access the table.` |  |
| `Table Properties` | `[delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1781283254000, delta.lastUpdateVersion=519000, delta.minReaderVersion=1, delta.minWriterVersion=4, delta.workloadBasedColumns.optimizerStatistics=`workspace_id`,`account_id`,`statement_id`,`update_time`,`start_time`,`total_task_duration_ms`,`statement_type`,`end_time`]` |  |
| `Statistics` | `8798768870320 bytes, 24278287889 rows` |  |
| `Location` | `uc-deltasharing://system.query.history#system.query.history` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.compute.warehouse_events` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | The ID of the Databricks account. |
| `workspace_id` | `string` | The ID of the workspace where the warehouse is deployed. |
| `warehouse_id` | `string` | The ID of SQL warehouse the event is related to. |
| `event_type` | `string` | The type of warehouse event. Possible values are SCALED_UP, SCALED_DOWN, STOPPING, RUNNING, STARTING, and STOPPED. |
| `cluster_count` | `int` | The number of clusters that are actively running. |
| `event_time` | `timestamp` | Timestamp of when the event took place in UTC. |
| `Catalog` | `system` |  |
| `Database` | `compute` |  |
| `Table` | `warehouse_events` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Mon Oct 30 17:36:13 UTC 2023` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The Warehouse events table table captures events related to SQL warehouses. For example, starting, stopping, running, scaling up and down` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1734433440000, delta.lastUpdateVersion=168322, delta.minReaderVersion=1, delta.minWriterVersion=4]` |  |
| `Statistics` | `27111679682 bytes, 213662765 rows` |  |
| `Location` | `uc-deltasharing://system.compute.warehouse_events#system.compute.warehouse_events` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.billing.usage` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | ID of the account this report was generated for |
| `workspace_id` | `string` | ID of the Workspace this usage was associated with |
| `record_id` | `string` | Unique ID for this usage record |
| `sku_name` | `string` | Name of the SKU |
| `cloud` | `string` | Cloud this usage is relevant for. Possible values are AWS, AZURE, and GCP. |
| `usage_start_time` | `timestamp` | The start time relevant to this usage record. Timezone information is recorded at the end of the value with +00:00 representing UTC timezone. |
| `usage_end_time` | `timestamp` | The end time relevant to this usage record. Timezone information is recorded at the end of the value with +00:00 representing UTC timezone. |
| `usage_date` | `date` | Date of the usage record, this field can be used for faster aggregation by date |
| `custom_tags` | `map<string,string>` | Tags applied by the users to this usage. Includes compute resource tags and jobs tags. |
| `usage_unit` | `string` | Unit this usage is measured in. Possible values include DBUs. |
| `usage_quantity` | `decimal(38,18)` | Number of units consumed for this record. |
| `usage_metadata` | `struct<cluster_id:string,job_id:string,warehouse_id:string,instance_pool_id:string,node_type:string,job_run_id:string,notebook_id:string,dlt_pipeline_id:string,endpoint_name:string,endpoint_id:string,dlt_update_id:string,dlt_maintenance_id:string,run_name:string,job_name:string,notebook_path:string,central_clean_room_id:string,source_region:string,destination_region:string,app_id:string,app_name:string,metastore_id:string,private_endpoint_name:string,storage_api_type:string,budget_policy_id:string,ai_runtime_pool_id:string,... 25 more fields>` | System-provided metadata about the usage, including IDs for compute resources and jobs (if applicable). See [Analyze usage metadata](https://docs.databricks.com/en/admin/system-tables/billing.html#usage-metadata). |
| `identity_metadata` | `struct<run_as:string,created_by:string,owned_by:string,run_by:string>` | System-provided metadata about the identities involved in the usage. See [Analyze identity metadata](https://docs.databricks.com/en/admin/system-tables/billing.html#identity-metadata). |
| `record_type` | `string` | Whether the record is original, a retraction, or a restatement. The value is ORIGINAL unless the record is related to a correction. See [Analyze correction records](https://docs.databricks.com/en/admin/system-tables/billing.html#record-type). |
| `ingestion_date` | `date` | Date the record was ingested into the usage table. |
| `billing_origin_product` | `string` | The product that originated the usage. Some products can be billed as different SKUs. For possible values, see [View information about the product associated with the usage](https://docs.databricks.com/en/admin/system-tables/billing.html#features). |
| `product_features` | `struct<jobs_tier:string,sql_tier:string,dlt_tier:string,is_serverless:boolean,is_photon:boolean,serving_type:string,networking:struct<connectivity_type:string>,ai_runtime:struct<compute_type:string>,model_serving:struct<offering_type:string>,ai_gateway:struct<feature_type:string>,performance_target:string,serverless_gpu:struct<workload_type:string>,agent_bricks:struct<problem_type:string,workload_type:string>,ai_functions:struct<ai_function:string>,apps:struct<compute_size:string>,lakeflow_connect:struct<task_type:string,zerobus_request_type:string>,lakebase:struct<storage_type:string,compute_type:string>,ai_bi_genie:struct<capability_type:string,offering_type:string>,genie:struct<offering_type:string>>` | Details about the specific product features used. |
| `usage_type` | `string` | The type of usage attributed to the product or workload for billing purposes. Possible values are COMPUTE_TIME, STORAGE_SPACE, NETWORK_BYTES, API_CALLS, TOKEN, or GPU_TIME. |
| `Catalog` | `system` |  |
| `Database` | `billing` |  |
| `Table` | `usage` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Sat Apr 06 05:23:50 UTC 2024` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The [usage table](https://docs.databricks.com/admin/system-tables/billing.html) gives you access to account-wide billable usage data. The data is centralized and routed to all the regions.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1781565039000, delta.lastUpdateVersion=168991, delta.minReaderVersion=1, delta.minWriterVersion=4, delta.workloadBasedColumns.deltaFileStatistics=`usage_metadata`.`sharing_materialization_id`,`usage_metadata`.`budget_policy_id`]` |  |
| `Statistics` | `154113358128 bytes, 403726439 rows` |  |
| `Location` | `uc-deltasharing://system.billing.usage#system.billing.usage` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.billing.list_prices` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | ID of the account this report was generated for |
| `price_start_time` | `timestamp` | The time this price became effective in UTC |
| `price_end_time` | `timestamp` | The time this price stopped being effective in UTC |
| `sku_name` | `string` | Name of the SKU |
| `cloud` | `string` | Name of the Cloud this price is applicable to. Possible values are AWS, AZURE, and GCP. |
| `currency_code` | `string` | The currency this price is expressed in |
| `usage_unit` | `string` | The unit of measurement that is monetized. |
| `pricing` | `struct<default:decimal(38,18),promotional:struct<default:decimal(38,18)>,effective_list:struct<default:decimal(38,18)>>` | A structured data field that includes pricing info at the published list price rate. The key default will always return a single price that can be used for simple long-term estimates. The key promotional represents a temporary promotional price that all customers get which could be used for cost estimation during the temporary period. The key effective_list resolves list and promotional price, and contains the effective list price used for calculating the cost. |
| `Catalog` | `system` |  |
| `Database` | `billing` |  |
| `Table` | `list_prices` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Thu Aug 17 02:20:11 UTC 2023` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The [pricing table](https://docs.databricks.com/admin/system-tables/pricing.html) gives you access to a historical log of SKU pricing. A record gets added each time there is a change to a SKU price.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.lastCommitTimestamp=1751948891000, delta.lastUpdateVersion=16424, delta.minReaderVersion=1, delta.minWriterVersion=2]` |  |
| `Statistics` | `1811039520 bytes, 104208541 rows` |  |
| `Location` | `uc-deltasharing://system.billing.list_prices#system.billing.list_prices` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.serving.served_entities` — OK

| col_name | data_type | comment |
|---|---|---|
| `served_entity_id` | `string` | The unique ID of the served entity. |
| `account_id` | `string` | The Databricks account ID of the workspace associated with the serving endpoint. |
| `workspace_id` | `string` | The workspace ID for the workspace in which the serving endpoint exists. |
| `created_by` | `string` | The name of the creator. Can be a user, service principal, or group name. |
| `endpoint_name` | `string` | The name of the serving endpoint. |
| `endpoint_id` | `string` | The unique ID of the serving endpoint. |
| `served_entity_name` | `string` | The name of the served entity. |
| `entity_type` | `string` | Type of the entity that is served. Can be FEATURE_SPEC, EXTERNAL_MODEL, FOUNDATION_MODEL, or CUSTOM_MODEL |
| `entity_name` | `string` | The underlying name of the entity. Different from the served_entity_name which is a user provided name. For example, entity_name is the name of the Unity Catalog model. |
| `entity_version` | `string` | The version of the served entity. |
| `endpoint_config_version` | `int` | The version of the endpoint configuration. |
| `task` | `string` | The task type. Can be llm/v1/chat, llm/v1/completions, or llm/v1/embeddings. |
| `external_model_config` | `struct<provider:string>` | Configurations for external models. For example, {Provider: OpenAI} |
| `foundation_model_config` | `struct<min_provisioned_throughput:bigint,max_provisioned_throughput:bigint>` | Configurations for foundation models. For example,{min_provisioned_throughput: 2200, max_provisioned_throughput: 4400} |
| `custom_model_config` | `struct<min_concurrency:int,max_concurrency:int,compute_type:string>` | Configurations for custom models. For example,{ min_concurrency: 0, max_concurrency: 4, compute_type: CPU } |
| `feature_spec_config` | `struct<min_concurrency:int,max_concurrency:int,compute_type:string>` | Configurations for feature specifications. For example, { min_concurrency: 0, max_concurrency: 4, compute_type: CPU } |
| `change_time` | `timestamp` | Timestamp of change for the served entity. |
| `endpoint_delete_time` | `timestamp` | Timestamp of entity deletion. The endpoint is the container for the served entity. After the endpoint is deleted, the served entity is also deleted. |
| `Catalog` | `system` |  |
| `Database` | `serving` |  |
| `Table` | `served_entities` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Thu Oct 03 19:13:34 UTC 2024` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `This system table records all entities that were served through Databricks model serving as well as the configurations at different config versions.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1773236786000, delta.lastUpdateVersion=160864, delta.minReaderVersion=1, delta.minWriterVersion=4]` |  |
| `Statistics` | `1166171417 bytes, 741783 rows` |  |
| `Location` | `uc-deltasharing://system.serving.served_entities#system.serving.served_entities` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.serving.endpoint_usage` — OK

| col_name | data_type | comment |
|---|---|---|
| `workspace_id` | `string` | The workspace ID for the workspace in which the serving endpoint exists. |
| `account_id` | `string` | The Databricks account ID of the workspace associated with the serving endpoint. |
| `client_request_id` | `string` | The user provided request identifier that can be specified in the model serving request body. |
| `databricks_request_id` | `string` | A Databricks generated request identifier attached to all model serving requests. |
| `requester` | `string` | The name or email of the user or service principal whose permissions are used for the invocation request of the serving endpoint. |
| `status_code` | `int` | The HTTP status code that was returned from the model. |
| `request_time` | `timestamp` | The timestamp at which the request is received. |
| `input_token_count` | `bigint` | The token count of the input. |
| `output_token_count` | `bigint` | The token count of the output. |
| `input_character_count` | `bigint` | The character count of the input string or prompt. |
| `output_character_count` | `bigint` | The character count of the output string of the response. |
| `usage_context` | `map<string,string>` | The user provided map containing identifiers of the end user or the customer application that makes the call to the endpoint. See [Further define usage with usage_context](https://docs.databricks.com/en/ai-gateway/configure-ai-gateway-endpoints.html#usage-context). |
| `request_streaming` | `boolean` | Whether the request is in stream mode. |
| `served_entity_id` | `string` | The unique ID used to join with the system.serving.served_entities dimension table to lookup information about the endpoint and served entity. |
| `Catalog` | `system` |  |
| `Database` | `serving` |  |
| `Table` | `endpoint_usage` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Thu Oct 03 19:13:34 UTC 2024` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `This system table records per request usage information for model serving. For instance, it can be used to attribute how many LLM tokens were sent to different served entities. To look up more information about the served entity that received the request, use the served_entity_id as the join key with system.serving.served_entities.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1773841694000, delta.lastUpdateVersion=151811, delta.minReaderVersion=1, delta.minWriterVersion=4]` |  |
| `Statistics` | `58948985348 bytes, 1028289178 rows` |  |
| `Location` | `uc-deltasharing://system.serving.endpoint_usage#system.serving.endpoint_usage` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.ai_gateway.usage` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | The account id |
| `workspace_id` | `string` | The workspace id of the AI Gateway endpoint |
| `request_id` | `string` | The API proxy generated request identifier (or AI Gateway generated UUID if proxy ID doesn't exist) |
| `schema_version` | `int` | The version of the event schema (to support schema evolution) |
| `endpoint_id` | `string` | The UUID of the AI Gateway entity |
| `endpoint_name` | `string` | The name of the top level AI Gateway entity |
| `endpoint_tags` | `map<string,string>` | The static tags configured on the endpoint, for the purpose of usage table aggregation and visualization |
| `endpoint_metadata` | `struct<creator:string,creation_time:timestamp,last_updated_time:timestamp,destinations:array<struct<name:string,traffic_percent:double,type:string>>,inference_table:string,fallbacks:array<struct<name:string,traffic_percent:double,type:string>>>` | The endpoint level metadata |
| `event_time` | `timestamp` | The timestamp at which request is received |
| `latency_ms` | `bigint` | The latency from AI Gateway received the client request to the completion of forwarding the proxy response |
| `time_to_first_byte_ms` | `bigint` | The latency from AI Gateway received the client request to receiving the first byte array of the proxy response |
| `destination_type` | `string` | The destination type |
| `destination_name` | `string` | The name of the destination object - the endpoint name or the PPT UC model name |
| `destination_id` | `string` | The ID of the destination - the endpoint ID or the PPT UC model id |
| `destination_model` | `string` | The foundation model name (e.g., 'gpt-5-mini', 'meta-llama-3.3-70b-instruct') |
| `requester` | `string` | The name or email of the user or service principal whose permissions are used for the invocation request |
| `requester_type` | `string` | The type of the requester |
| `ip_address` | `string` | The ip address of the client, if it's not from a databricks service. If the client is a databricks service, the ip_address will be null |
| `url` | `string` | The request URL received by AI Gateway |
| `user_agent` | `string` | The user agent of the client, if it's not from a databricks service |
| `api_type` | `string` | The API type of the request |
| `request_tags` | `map<string,string>` | The tags provided in the request body, for the purpose of usage table aggregation and visualization |
| `input_tokens` | `bigint` | The total count of input tokens. The value will be null if the token count is not available in the response |
| `output_tokens` | `bigint` | The total count of output tokens. The value will be null if the token count is not available in the response |
| `total_tokens` | `bigint` | The sum of input tokens and output tokens. The value will be null if the token count is not available in the response |
| `token_details` | `struct<cache_read_input_tokens:bigint,cache_creation_input_tokens:bigint,output_reasoning_tokens:bigint>` | A more detailed breakdown of the token usages. It includes the details of some major foundation models |
| `response_content_type` | `string` | The content of the Content-Type response header. Indicates whether the response is streaming mode or not |
| `status_code` | `int` | The final HTTP status code returned by the endpoint |
| `routing_information` | `struct<attempts:array<struct<priority:string,action:string,destination:string,destination_id:string,status_code:int,error_code:string,latency_ms:bigint,start_time:timestamp,end_time:timestamp>>>` | Detailed routing information, for primary destination and fallbacks |
| `invocation_id` | `string` | A unique identifier generated by AI Gateway for each individual inference call. Distinguishes multiple calls that share the same request_id (e.g., multi-turn agent calls, guardrail checks) |
| `invocation_metadata` | `struct<source:string,service_tier:string>` | System-generated metadata about this inference call |
| `Catalog` | `system` |  |
| `Database` | `ai_gateway` |  |
| `Table` | `usage` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Tue Feb 17 22:25:50 UTC 2026` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `System table used for usage tracking for AI Gateway endpoints. Each row represents a single request to an AI Gateway endpoint with detailed information about routing, token usage, and applied features.` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.enableDeletionVectors=true, delta.feature.appendOnly=supported, delta.feature.changeDataFeed=supported, delta.feature.deletionVectors=supported, delta.feature.invariants=supported, delta.lastCommitTimestamp=1780413923000, delta.lastUpdateVersion=18881, delta.minReaderVersion=3, delta.minWriterVersion=7, delta.parquet.compression.codec=zstd]` |  |
| `Statistics` | `8913052832 bytes, 150344562 rows` |  |
| `Location` | `uc-deltasharing://system.ai_gateway.usage#system.ai_gateway.usage` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.lakeflow.jobs` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | The ID of the account this job belongs to. |
| `workspace_id` | `string` | The ID of the workspace this job belongs to. |
| `job_id` | `string` | The ID of the job. Only unique within a single workspace. |
| `name` | `string` | The user-supplied name of the job. |
| `creator_id` | `string` | The ID of the principal who created the job. |
| `tags` | `map<string,string>` | The user-supplied custom tags associated with this job. |
| `run_as` | `string` | The ID of the user or service principal whose permissions are used for the job run. |
| `change_time` | `timestamp` | The time when the job was last modified. Timezone recorded as +00:00 (UTC). |
| `delete_time` | `timestamp` | The time when the job was deleted by the user. Timezone recorded as +00:00 (UTC). |
| `description` | `string` | The user-supplied description of the job. **Not populated for rows emitted before late August 2024.** |
| `trigger` | `struct<file_arrival:struct<url:string,min_time_between_triggers_seconds:int,wait_after_last_change_seconds:int>,periodic:struct<interval:int,units:string>,continuous:struct<enabled:boolean>,table_update:struct<table_names:array<string>,min_time_between_triggers_seconds:bigint,wait_after_last_change_seconds:bigint,condition:string>,schedule:struct<quartz_cron_expression:string,timezone_id:string>>` | The trigger configuration for the job. **Not populated for rows emitted before late November 2025** |
| `trigger_type` | `string` | The type of trigger for the job. **Not populated for rows emitted before late November 2025** |
| `run_as_user_name` | `string` | The email/ID of the service principal or group name whose permissions are used for the job run. **Not populated for rows emitted before late November 2025** |
| `creator_user_name` | `string` | The email/ID of the service principal or group name who created the job. **Not populated for rows emitted before late November 2025** |
| `paused` | `boolean` | Indicates whether the job is paused. **Not populated for rows emitted before late November 2025** |
| `timeout_seconds` | `bigint` | The timeout duration for the job in seconds. **Not populated for rows emitted before late November 2025** |
| `health_rules` | `array<struct<metric:string,operator:string,value:bigint>>` | Set of health rules defined for this job. **Not populated for rows emitted before late November 2025** |
| `deployment` | `struct<kind:string,metadata_file_path:string>` | Deployment information for jobs managed by external sources. **Not populated for rows emitted before late November 2025** |
| `create_time` | `timestamp` | The time at which this job was created. Timezone recorded as +00:00 (UTC). **Not populated for rows emitted before late November 2025** |
| `Catalog` | `system` |  |
| `Database` | `lakeflow` |  |
| `Table` | `jobs` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Mon Aug 12 19:54:29 UTC 2024` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The `jobs` table is a slow-changing dimension table (SCD2) that contains the full history of job configurations over time. When a row changes, a new row is emitted, logically replacing the previous one. For documentation, example queries, and monitoring usage, see [Lakeflow System Tables](https://docs.databricks.com/en/admin/system-tables/jobs.html#jobs-table-schema).` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1776865536000, delta.lastUpdateVersion=353746, delta.minReaderVersion=1, delta.minWriterVersion=4]` |  |
| `Statistics` | `12689800431 bytes, 109983850 rows` |  |
| `Location` | `uc-deltasharing://system.lakeflow.jobs#system.lakeflow.jobs` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.lakeflow.job_run_timeline` — OK

| col_name | data_type | comment |
|---|---|---|
| `account_id` | `string` | The ID of the account this job belongs to. |
| `workspace_id` | `string` | The ID of the workspace this job belongs to. |
| `job_id` | `string` | The ID of the job. This key is only unique within a single workspace. |
| `run_id` | `string` | The ID of the job run. |
| `period_start_time` | `timestamp` | The start time for the run or for the time period. Timezone information is recorded at the end of the value with +00:00 representing UTC. |
| `period_end_time` | `timestamp` | The end time for the run or for the time period. Timezone information is recorded at the end of the value with +00:00 representing UTC. |
| `trigger_type` | `string` | The type of trigger that can fire a run. For possible values, see [Trigger type values](https://docs.databricks.com/en/admin/system-tables/jobs.html#trigger) |
| `result_state` | `string` | The outcome of the job run. For possible values, see [Result state values](https://docs.databricks.com/en/admin/system-tables/jobs.html#result). |
| `run_type` | `string` | The type of job run. For possible values, see [Run type values](https://docs.databricks.com/en/admin/system-tables/jobs.html#run-type). |
| `run_name` | `string` | The user-supplied run name associated with this job run. |
| `compute_ids` | `array<string>` | Array containing the compute IDs for the parent job run. Use for identifying cluster used by `WORKFLOW_RUN` run types. For other compute information, refer to the `job_task_run_timeline` table. **Not populated for rows emitted before late August 2024** |
| `termination_code` | `string` | The termination code of the job run. For possible values, see [Termination code values](https://docs.databricks.com/en/admin/system-tables/jobs.html#termination). **Not populated for rows emitted before late August 2024.** |
| `job_parameters` | `map<string,map<string,string>>` | The job-level parameters used in the job run. **Not populated for rows emitted before late August 2024.** |
| `source_task_run_id` | `string` | The id of the source task run. Use for identifying which task run triggered this job run. **Not populated for rows emitted before late November 2025** |
| `root_task_run_id` | `string` | The id of the root task run. Use for identifying which task run triggered this job run. **Not populated for rows emitted before late November 2025** |
| `compute` | `array<struct<type:string,cluster_id:string,warehouse_id:string>>` | Details about the compute resources used in the job run. **Not populated for rows emitted before late November 2025** |
| `termination_type` | `string` | The type of termination for the job run. **Not populated for rows emitted before late November 2025** |
| `setup_duration_seconds` | `bigint` | The duration of the setup phase for the job run in seconds. **Not populated for rows emitted before late November 2025** |
| `queue_duration_seconds` | `bigint` | The duration spent in the queue for the job run in seconds. **Not populated for rows emitted before late November 2025** |
| `run_duration_seconds` | `bigint` | The total duration of the job run in seconds. **Not populated for rows emitted before late November 2025** |
| `cleanup_duration_seconds` | `bigint` | The duration of the cleanup phase for the job run in seconds. **Not populated for rows emitted before late November 2025** |
| `execution_duration_seconds` | `bigint` | The duration of the execution phase for the job run in seconds. **Not populated for rows emitted before late November 2025** |
| `Catalog` | `system` |  |
| `Database` | `lakeflow` |  |
| `Table` | `job_run_timeline` |  |
| `Owner` | `System user` |  |
| `Created Time` | `Mon Aug 12 19:54:29 UTC 2024` |  |
| `Last Access` | `UNKNOWN` |  |
| `Created By` | `Spark` |  |
| `Type` | `MANAGED` |  |
| `Provider` | `deltasharing` |  |
| `Comment` | `The `job_run_timeline` table is a fact table that contains slices of ongoing job runs, allowing one to track the run time and metadata related to each job run. Each slice is represented by `period_start_time` and `period_end_time` timestamps. For documentation, example queries, and monitoring usage, see [Lakeflow System Tables](https://docs.databricks.com/en/admin/system-tables/jobs.html#job-run-timeline-table-schema).` |  |
| `Table Properties` | `[delta.autoOptimize.autoCompact=auto, delta.autoOptimize.optimizeWrite=true, delta.enableChangeDataFeed=true, delta.lastCommitTimestamp=1764909497000, delta.lastUpdateVersion=275365, delta.minReaderVersion=1, delta.minWriterVersion=4]` |  |
| `Statistics` | `19437845201 bytes, 16240322 rows` |  |
| `Location` | `uc-deltasharing://system.lakeflow.job_run_timeline#system.lakeflow.job_run_timeline` |  |
| `Legacy UC Partitioned DELTASHARING Table` | `true` |  |

## `system.information_schema.tables` — OK

| col_name | data_type | comment |
|---|---|---|
| `table_catalog` | `string` | Catalog that contains the relation. |
| `table_schema` | `string` | Schema that contains the relation. |
| `table_name` | `string` | Name of the relation. |
| `table_type` | `string` | One of 'BASE TABLE', 'VIEW'. |
| `is_insertable_into` | `string` | 'YES' if the relation can be inserted into, 'NO' otherwise. |
| `commit_action` | `string` | Always 'PRESERVE'. Reserved for future use. |
| `table_owner` | `string` | User or group (principal) currently owning the relation. |
| `comment` | `string` | An optional comment that describes the relation. |
| `created` | `timestamp` | Timestamp when the relation was created. |
| `created_by` | `string` | Principal which created the relation. |
| `last_altered` | `timestamp` | Timestamp when the relation definition was last altered in any way. |
| `last_altered_by` | `string` | Principal which last altered the relation. |
| `data_source_format` | `string` | Format of the data source such as PARQUET, or CSV. |
| `storage_sub_directory` | `string` | Path to the storage of an external table, NULL otherwise. |
| `storage_path` | `string` |  |
| `Catalog` | `system` |  |
| `Database` | `information_schema` |  |
| `Table` | `tables` |  |
| `Type` | `MANAGED` |  |
| `Table Properties` | `[]` |  |

## Observed `billing_origin_product` values (last 30 days)

- `AGENT_BRICKS`
- `AGENT_EVALUATION`
- `AI_FUNCTIONS`
- `AI_GATEWAY`
- `AI_RUNTIME`
- `ALL_PURPOSE`
- `APPS`
- `BASE_ENVIRONMENTS`
- `DATABASE`
- `DATA_CLASSIFICATION`
- `DATA_QUALITY_MONITORING`
- `DATA_SHARING`
- `DEFAULT_STORAGE`
- `DLT`
- `EXTERNAL_COMPATIBILITY`
- `FINE_GRAINED_ACCESS_CONTROL`
- `INTERACTIVE`
- `JOBS`
- `LAKEBASE`
- `LAKEFLOW_CONNECT`
- `MODEL_SERVING`
- `NETWORKING`
- `PREDICTIVE_OPTIMIZATION`
- `SQL`
- `SUPERVISOR_AGENT`
- `VECTOR_SEARCH`



{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This Converts Parquet or AVRO Data Loaded in the Variant Column in the RAW DB into Flattend Views
                                                This also creates a HASH_KEY for Incremental Tables for the Curated Layer 
                                                Additional CDA Files are Null in the AVRO but not in CDA .
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    tags=["raw_gwpc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:PreviousGroupID::NUMBER AS previousgroupid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Active::BOOLEAN AS active,
                TO_TIMESTAMP_TZ(data_payload:CloseDate::NUMBER/1000) AS closedate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:AssignedByUserID::NUMBER AS assignedbyuserid,
                data_payload:AssignedGroupID::NUMBER AS assignedgroupid,
                data_payload:PolicyID::NUMBER AS policyid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Comments::TEXT AS VARCHAR(255)) AS comments,
                data_payload:AssignedUserID::NUMBER AS assigneduserid,
                data_payload:PreviousQueueID::NUMBER AS previousqueueid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Role::NUMBER AS role,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:AssignmentDate::NUMBER/1000) AS assignmentdate,
                data_payload:PreviousUserID::NUMBER AS previoususerid,
                data_payload:AssignedQueueID::NUMBER AS assignedqueueid,
                data_payload:AssignmentStatus::NUMBER AS assignmentstatus,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS STRING) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' file_type
            FROM {{ source('gwpc', 'pc_policyuserroleassign') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                $1:previousgroupid::NUMBER AS previousgroupid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:active::BOOLEAN AS active,
                $1:closedate::TIMESTAMP_TZ AS closedate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:assignedbyuserid::NUMBER AS assignedbyuserid,
                $1:assignedgroupid::NUMBER AS assignedgroupid,
                $1:policyid::NUMBER AS policyid,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:comments::TEXT AS VARCHAR(255)) AS comments,
                $1:assigneduserid::NUMBER AS assigneduserid,
                $1:previousqueueid::NUMBER AS previousqueueid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:role::NUMBER AS role,
                $1:id::NUMBER AS id,
                $1:assignmentdate::TIMESTAMP_TZ AS assignmentdate,
                $1:previoususerid::NUMBER AS previoususerid,
                $1:assignedqueueid::NUMBER AS assignedqueueid,
                $1:assignmentstatus::NUMBER AS assignmentstatus,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' file_type
            FROM {{ source('gwpc', 'pc_policyuserroleassign') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
    Transformed CTE is To Create the HASH_KEY Based on the Right Combination
-#}   
cte_transformed AS (
    SELECT
        *,
        CASE
             WHEN file_type = 'AVRO' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'createuserid',
                        'previousgroupid',
                        'publicid',
                        'active',
                        'closedate',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'assignedbyuserid',
                        'assignedgroupid',
                        'policyid',
                        'updateuserid',
                        'comments',
                        'assigneduserid',
                        'previousqueueid',
                        'updatetime',
                        'role',
                        'id',
                        'assignmentdate',
                        'previoususerid',
                        'assignedqueueid',
                        'assignmentstatus'
                        ]) }}
            WHEN file_type = 'PARQUET' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'id',
                        'gwcbi_seqval'
                        ]) }}
        END AS hash_key    
    FROM cte_source_data
)
SELECT * FROM cte_transformed
        
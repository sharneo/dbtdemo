{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_userroleassign.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:PreviousGroupID::NUMBER AS previousgroupid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Active::BOOLEAN AS active,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:AssignedByUserID::NUMBER AS assignedbyuserid,
                data_payload:AssignedGroupID::NUMBER AS assignedgroupid,
                data_payload:PreviousQueueID::NUMBER AS previousqueueid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                data_payload:PreviousUserID::NUMBER AS previoususerid,
                data_payload:AssignedQueueID::NUMBER AS assignedqueueid,
                data_payload:ExposureID::NUMBER AS exposureid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:CloseDate::NUMBER/1000) AS closedate,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:AssignedUserID::NUMBER AS assigneduserid,
                CAST(data_payload:Comments::TEXT AS VARCHAR(255)) AS comments,
                data_payload:Role::NUMBER AS role,
                TO_TIMESTAMP_TZ(data_payload:AssignmentDate::NUMBER/1000) AS assignmentdate,
                data_payload:AssignmentStatus::NUMBER AS assignmentstatus,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS VARCHAR(300)) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_userroleassign') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:previousgroupid::NUMBER AS previousgroupid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:active::BOOLEAN AS active,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:assignedbyuserid::NUMBER AS assignedbyuserid,
                $1:assignedgroupid::NUMBER AS assignedgroupid,
                $1:previousqueueid::NUMBER AS previousqueueid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                $1:previoususerid::NUMBER AS previoususerid,
                $1:assignedqueueid::NUMBER AS assignedqueueid,
                $1:exposureid::NUMBER AS exposureid,
                $1:createuserid::NUMBER AS createuserid,
                $1:closedate::TIMESTAMP_TZ AS closedate,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:assigneduserid::NUMBER AS assigneduserid,
                CAST($1:comments::TEXT AS VARCHAR(255)) AS comments,
                $1:role::NUMBER AS role,
                $1:assignmentdate::TIMESTAMP_TZ AS assignmentdate,
                $1:assignmentstatus::NUMBER AS assignmentstatus,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::VARCHAR(300) as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_userroleassign') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS userroleassign_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'previousgroupid',
                        'publicid',
                        'active',
                        'createtime',
                        'assignedbyuserid',
                        'assignedgroupid',
                        'previousqueueid',
                        'updatetime',
                        'claimid',
                        'previoususerid',
                        'assignedqueueid',
                        'exposureid',
                        'createuserid',
                        'closedate',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'updateuserid',
                        'assigneduserid',
                        'comments',
                        'role',
                        'assignmentdate',
                        'assignmentstatus'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_csponacc_ext.
                                                csponacc_ext_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    transient=True,
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["landing", "gwpc", "policy_centre", "business_critical", "pcx_csponacc_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:PreviousGroupID::NUMBER AS previousgroupid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:EmployerSelectedDate::NUMBER/1000) AS employerselecteddate,
                data_payload:AccountId::NUMBER AS accountid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:CSPAvailable::BOOLEAN AS cspavailable,
                TO_TIMESTAMP_TZ(data_payload:CSPEndDate::NUMBER/1000) AS cspenddate,
                data_payload:AssignedByUserID::NUMBER AS assignedbyuserid,
                CAST(data_payload:CSPName::TEXT AS VARCHAR(40)) AS cspname,
                data_payload:AssignedGroupID::NUMBER AS assignedgroupid,
                TO_TIMESTAMP_TZ(data_payload:CSPStartDate::NUMBER/1000) AS cspstartdate,
                data_payload:PreviousQueueID::NUMBER AS previousqueueid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:RoundRobin::BOOLEAN AS roundrobin,
                data_payload:PreviousUserID::NUMBER AS previoususerid,
                data_payload:AssignedQueueID::NUMBER AS assignedqueueid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:CloseDate::NUMBER/1000) AS closedate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:CSPCode::TEXT AS VARCHAR(40)) AS cspcode,
                data_payload:EmployerChosen::BOOLEAN AS employerchosen,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Comments::TEXT AS VARCHAR(100)) AS comments,
                data_payload:AssignedUserID::NUMBER AS assigneduserid,
                data_payload:IsRemoved::BOOLEAN AS isremoved,
                data_payload:CSPType::NUMBER AS csptype,
                TO_TIMESTAMP_TZ(data_payload:AssignmentDate::NUMBER/1000) AS assignmentdate,
                data_payload:BulkTransfer::BOOLEAN AS bulktransfer,
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
            FROM {{ source('gwpc', 'pcx_csponacc_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:previousgroupid::NUMBER AS previousgroupid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:employerselecteddate::TIMESTAMP_TZ AS employerselecteddate,
                $1:accountid::NUMBER AS accountid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:cspavailable::BOOLEAN AS cspavailable,
                $1:cspenddate::TIMESTAMP_TZ AS cspenddate,
                $1:assignedbyuserid::NUMBER AS assignedbyuserid,
                CAST($1:cspname::TEXT AS VARCHAR(40)) AS cspname,
                $1:assignedgroupid::NUMBER AS assignedgroupid,
                $1:cspstartdate::TIMESTAMP_TZ AS cspstartdate,
                $1:previousqueueid::NUMBER AS previousqueueid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:roundrobin::BOOLEAN AS roundrobin,
                $1:previoususerid::NUMBER AS previoususerid,
                $1:assignedqueueid::NUMBER AS assignedqueueid,
                $1:createuserid::NUMBER AS createuserid,
                $1:closedate::TIMESTAMP_TZ AS closedate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                CAST($1:cspcode::TEXT AS VARCHAR(40)) AS cspcode,
                $1:employerchosen::BOOLEAN AS employerchosen,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:comments::TEXT AS VARCHAR(100)) AS comments,
                $1:assigneduserid::NUMBER AS assigneduserid,
                $1:isremoved::BOOLEAN AS isremoved,
                $1:csptype::NUMBER AS csptype,
                $1:assignmentdate::TIMESTAMP_TZ AS assignmentdate,
                $1:bulktransfer::BOOLEAN AS bulktransfer,
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
            FROM {{ source('gwpc', 'pcx_csponacc_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS csponacc_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'previousgroupid',
                        'publicid',
                        'employerselecteddate',
                        'accountid',
                        'createtime',
                        'cspavailable',
                        'cspenddate',
                        'assignedbyuserid',
                        'cspname',
                        'assignedgroupid',
                        'cspstartdate',
                        'previousqueueid',
                        'updatetime',
                        'roundrobin',
                        'previoususerid',
                        'assignedqueueid',
                        'createuserid',
                        'closedate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'cspcode',
                        'employerchosen',
                        'updateuserid',
                        'comments',
                        'assigneduserid',
                        'isremoved',
                        'csptype',
                        'assignmentdate',
                        'bulktransfer',
                        'assignmentstatus'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
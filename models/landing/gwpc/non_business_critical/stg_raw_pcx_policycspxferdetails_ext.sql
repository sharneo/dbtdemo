{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_policycspxferdetails_ext.
                                                policycspxferdetails_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_policycspxferdetails_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CurrentSpecCSPStartDate::NUMBER/1000) AS currentspeccspstartdate,
                CAST(data_payload:EmployerName::TEXT AS VARCHAR(255)) AS employername,
                data_payload:GroupNumber::NUMBER AS groupnumber,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:GroupName::TEXT AS VARCHAR(255)) AS groupname,
                CAST(data_payload:ParentAccountNumber::TEXT AS VARCHAR(255)) AS parentaccountnumber,
                CAST(data_payload:ErrorReason::TEXT AS VARCHAR(255)) AS errorreason,
                CAST(data_payload:NewSpecialistCSP::TEXT AS VARCHAR(100)) AS newspecialistcsp,
                CAST(data_payload:CurrentGeneralistCSPName::TEXT AS VARCHAR(100)) AS currentgeneralistcspname,
                TO_TIMESTAMP_TZ(data_payload:NewGeneralistCSPStartDate::NUMBER/1000) AS newgeneralistcspstartdate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:TransferStatus::NUMBER AS transferstatus,
                CAST(data_payload:CurrentSpecialistCSPName::TEXT AS VARCHAR(100)) AS currentspecialistcspname,
                TO_TIMESTAMP_TZ(data_payload:NewSpecialistCSPStartDate::NUMBER/1000) AS newspecialistcspstartdate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:CurrentGeneralistCSPEndDate::NUMBER/1000) AS currentgeneralistcspenddate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CurrentGeneralistCSPStartDate::NUMBER/1000) AS currentgeneralistcspstartdate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Comments::TEXT AS VARCHAR(250)) AS comments,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(40)) AS accountnumber,
                data_payload:BulkPolicyXferFileID::NUMBER AS bulkpolicyxferfileid,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(40)) AS policynumber,
                TO_TIMESTAMP_TZ(data_payload:CurrentSpecCSPEndDate::NUMBER/1000) AS currentspeccspenddate,
                CAST(data_payload:NewGeneralistCSP::TEXT AS VARCHAR(100)) AS newgeneralistcsp,
                data_payload:BulkPolicyCSPBatchID::NUMBER AS bulkpolicycspbatchid,
                TO_TIMESTAMP_TZ(data_payload:ProcessedDate::NUMBER/1000) AS processeddate,
                data_payload:IsTMFRecord::BOOLEAN AS istmfrecord,
                CAST(data_payload:FailureReasonDetails::TEXT AS VARCHAR(16777216)) AS failurereasondetails,
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
            FROM {{ source('gwpc', 'pcx_policycspxferdetails_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:currentspeccspstartdate::TIMESTAMP_TZ AS currentspeccspstartdate,
                CAST($1:employername::TEXT AS VARCHAR(255)) AS employername,
                $1:groupnumber::NUMBER AS groupnumber,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:groupname::TEXT AS VARCHAR(255)) AS groupname,
                CAST($1:parentaccountnumber::TEXT AS VARCHAR(255)) AS parentaccountnumber,
                CAST($1:errorreason::TEXT AS VARCHAR(255)) AS errorreason,
                CAST($1:newspecialistcsp::TEXT AS VARCHAR(100)) AS newspecialistcsp,
                CAST($1:currentgeneralistcspname::TEXT AS VARCHAR(100)) AS currentgeneralistcspname,
                $1:newgeneralistcspstartdate::TIMESTAMP_TZ AS newgeneralistcspstartdate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:transferstatus::NUMBER AS transferstatus,
                CAST($1:currentspecialistcspname::TEXT AS VARCHAR(100)) AS currentspecialistcspname,
                $1:newspecialistcspstartdate::TIMESTAMP_TZ AS newspecialistcspstartdate,
                $1:createuserid::NUMBER AS createuserid,
                $1:currentgeneralistcspenddate::TIMESTAMP_TZ AS currentgeneralistcspenddate,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:currentgeneralistcspstartdate::TIMESTAMP_TZ AS currentgeneralistcspstartdate,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:comments::TEXT AS VARCHAR(250)) AS comments,
                CAST($1:accountnumber::TEXT AS VARCHAR(40)) AS accountnumber,
                $1:bulkpolicyxferfileid::NUMBER AS bulkpolicyxferfileid,
                CAST($1:policynumber::TEXT AS VARCHAR(40)) AS policynumber,
                $1:currentspeccspenddate::TIMESTAMP_TZ AS currentspeccspenddate,
                CAST($1:newgeneralistcsp::TEXT AS VARCHAR(100)) AS newgeneralistcsp,
                $1:bulkpolicycspbatchid::NUMBER AS bulkpolicycspbatchid,
                $1:processeddate::TIMESTAMP_TZ AS processeddate,
                $1:istmfrecord::BOOLEAN AS istmfrecord,
                CAST($1:failurereasondetails::TEXT AS VARCHAR(16777216)) AS failurereasondetails,
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
            FROM {{ source('gwpc', 'pcx_policycspxferdetails_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS policycspxferdetails_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'currentspeccspstartdate',
                        'employername',
                        'groupnumber',
                        'createtime',
                        'groupname',
                        'parentaccountnumber',
                        'errorreason',
                        'newspecialistcsp',
                        'currentgeneralistcspname',
                        'newgeneralistcspstartdate',
                        'updatetime',
                        'transferstatus',
                        'currentspecialistcspname',
                        'newspecialistcspstartdate',
                        'createuserid',
                        'currentgeneralistcspenddate',
                        'beanversion',
                        'retired',
                        'currentgeneralistcspstartdate',
                        'updateuserid',
                        'comments',
                        'accountnumber',
                        'bulkpolicyxferfileid',
                        'policynumber',
                        'currentspeccspenddate',
                        'newgeneralistcsp',
                        'bulkpolicycspbatchid',
                        'processeddate',
                        'istmfrecord',
                        'failurereasondetails'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
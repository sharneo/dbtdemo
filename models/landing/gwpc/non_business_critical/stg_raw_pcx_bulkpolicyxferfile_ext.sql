{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_bulkpolicyxferfile_ext.
                                                bulkpolicyxferfile_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_policy_centre", "policy_centre", "non_business_critical", "pcx_bulkpolicyxferfile_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:RowsIgnoredFromCSV::NUMBER AS rowsignoredfromcsv,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:TotalRowsInCSV::NUMBER AS totalrowsincsv,
                CAST(data_payload:FileName::TEXT AS VARCHAR(1333)) AS filename,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:FileExecutionDate::NUMBER/1000) AS fileexecutiondate,
                data_payload:FileStatus::NUMBER AS filestatus,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ApprovalDate::NUMBER/1000) AS approvaldate,
                TO_TIMESTAMP_TZ(data_payload:LoadDate::NUMBER/1000) AS loaddate,
                CAST(data_payload:Comments::TEXT AS VARCHAR(16777216)) AS comments,
                data_payload:RecordsLoadedFromCSV::NUMBER AS recordsloadedfromcsv,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Approved::BOOLEAN AS approved,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:FileRejectionDate::NUMBER/1000) AS filerejectiondate,
                data_payload:ApprovedUserID::NUMBER AS approveduserid,
                data_payload:RecordsFailedFromCSV::NUMBER AS recordsfailedfromcsv,
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
            FROM {{ source('gwpc', 'pcx_bulkpolicyxferfile_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:rowsignoredfromcsv::NUMBER AS rowsignoredfromcsv,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:totalrowsincsv::NUMBER AS totalrowsincsv,
                CAST($1:filename::TEXT AS VARCHAR(1333)) AS filename,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:fileexecutiondate::TIMESTAMP_TZ AS fileexecutiondate,
                $1:filestatus::NUMBER AS filestatus,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:approvaldate::TIMESTAMP_TZ AS approvaldate,
                $1:loaddate::TIMESTAMP_TZ AS loaddate,
                CAST($1:comments::TEXT AS VARCHAR(16777216)) AS comments,
                $1:recordsloadedfromcsv::NUMBER AS recordsloadedfromcsv,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:approved::BOOLEAN AS approved,
                $1:id::NUMBER AS id,
                $1:filerejectiondate::TIMESTAMP_TZ AS filerejectiondate,
                $1:approveduserid::NUMBER AS approveduserid,
                $1:recordsfailedfromcsv::NUMBER AS recordsfailedfromcsv,
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
            FROM {{ source('gwpc', 'pcx_bulkpolicyxferfile_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS bulkpolicyxferfile_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'rowsignoredfromcsv',
                        'publicid',
                        'totalrowsincsv',
                        'filename',
                        'beanversion',
                        'createtime',
                        'retired',
                        'fileexecutiondate',
                        'filestatus',
                        'updateuserid',
                        'approvaldate',
                        'loaddate',
                        'comments',
                        'recordsloadedfromcsv',
                        'updatetime',
                        'approved',
                        'filerejectiondate',
                        'approveduserid',
                        'recordsfailedfromcsv'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
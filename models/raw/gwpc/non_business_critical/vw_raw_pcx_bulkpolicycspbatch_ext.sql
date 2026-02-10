
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
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BatchNumber::NUMBER AS batchnumber,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:FailureCount::NUMBER AS failurecount,
                CAST(data_payload:FileName::TEXT AS VARCHAR(255)) AS filename,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:LoadCount::NUMBER AS loadcount,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:CompletionDate::NUMBER/1000) AS completiondate,
                data_payload:SuccessCount::NUMBER AS successcount,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Status::NUMBER AS status,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:BatchRunUserID::NUMBER AS batchrunuserid,
                data_payload:ID::NUMBER AS id,
                data_payload:RecordCount::NUMBER AS recordcount,
                TO_TIMESTAMP_TZ(data_payload:BatchRunDate::NUMBER/1000) AS batchrundate,
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
            FROM {{ source('gwpc', 'pcx_bulkpolicycspbatch_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:batchnumber::NUMBER AS batchnumber,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:failurecount::NUMBER AS failurecount,
                CAST($1:filename::TEXT AS VARCHAR(255)) AS filename,
                $1:beanversion::NUMBER AS beanversion,
                $1:loadcount::NUMBER AS loadcount,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:completiondate::TIMESTAMP_TZ AS completiondate,
                $1:successcount::NUMBER AS successcount,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:status::NUMBER AS status,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:batchrunuserid::NUMBER AS batchrunuserid,
                $1:id::NUMBER AS id,
                $1:recordcount::NUMBER AS recordcount,
                $1:batchrundate::TIMESTAMP_TZ AS batchrundate,
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
            FROM {{ source('gwpc', 'pcx_bulkpolicycspbatch_ext') }}
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
                                'loadcommandid',
                        'createuserid',
                        'batchnumber',
                        'publicid',
                        'failurecount',
                        'filename',
                        'beanversion',
                        'loadcount',
                        'createtime',
                        'completiondate',
                        'successcount',
                        'updateuserid',
                        'status',
                        'updatetime',
                        'batchrunuserid',
                        'id',
                        'recordcount',
                        'batchrundate'
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
        
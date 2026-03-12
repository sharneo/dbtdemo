
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
    tags=["raw_gwcc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ObfuscatedInternal::BOOLEAN AS obfuscatedinternal,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:FailedAttempts::NUMBER AS failedattempts,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Active::BOOLEAN AS active,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:FailedTime::NUMBER/1000) AS failedtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:UserNameDenorm::TEXT AS VARCHAR(255)) AS usernamedenorm,
                CAST(data_payload:UserName::TEXT AS VARCHAR(255)) AS username,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:LockDate::NUMBER/1000) AS lockdate,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Password::TEXT AS VARCHAR(30)) AS password,
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
            FROM {{ source('gwcc', 'cc_credential') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:obfuscatedinternal::BOOLEAN AS obfuscatedinternal,
                $1:createuserid::NUMBER AS createuserid,
                $1:failedattempts::NUMBER AS failedattempts,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:active::BOOLEAN AS active,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:failedtime::TIMESTAMP_TZ AS failedtime,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:usernamedenorm::TEXT AS VARCHAR(255)) AS usernamedenorm,
                CAST($1:username::TEXT AS VARCHAR(255)) AS username,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:lockdate::TIMESTAMP_TZ AS lockdate,
                $1:id::NUMBER AS id,
                CAST($1:password::TEXT AS VARCHAR(30)) AS password,
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
            FROM {{ source('gwcc', 'cc_credential') }}
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
                        'obfuscatedinternal',
                        'createuserid',
                        'failedattempts',
                        'publicid',
                        'active',
                        'beanversion',
                        'retired',
                        'createtime',
                        'failedtime',
                        'updateuserid',
                        'usernamedenorm',
                        'username',
                        'updatetime',
                        'lockdate',
                        'id',
                        'password'
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
        
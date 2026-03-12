
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
                TO_TIMESTAMP_TZ(data_payload:ExpiryDate::NUMBER/1000) AS expirydate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:CommencementDate::NUMBER/1000) AS commencementdate,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:PolicyStatus::NUMBER AS policystatus,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:GroupNumber::NUMBER AS groupnumber,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:GroupName::TEXT AS VARCHAR(255)) AS groupname,
                TO_TIMESTAMP_TZ(data_payload:EffecitveDate::NUMBER/1000) AS effecitvedate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(40)) AS policynumber,
                CAST(data_payload:PolicyName::TEXT AS VARCHAR(255)) AS policyname,
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
            FROM {{ source('gwcc', 'ccx_clmassignmentrules_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:expirydate::TIMESTAMP_TZ AS expirydate,
                $1:createuserid::NUMBER AS createuserid,
                $1:commencementdate::TIMESTAMP_TZ AS commencementdate,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:policystatus::NUMBER AS policystatus,
                $1:beanversion::NUMBER AS beanversion,
                $1:groupnumber::NUMBER AS groupnumber,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:groupname::TEXT AS VARCHAR(255)) AS groupname,
                $1:effecitvedate::TIMESTAMP_TZ AS effecitvedate,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:policynumber::TEXT AS VARCHAR(40)) AS policynumber,
                CAST($1:policyname::TEXT AS VARCHAR(255)) AS policyname,
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
            FROM {{ source('gwcc', 'ccx_clmassignmentrules_icare') }}
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
                        'expirydate',
                        'createuserid',
                        'commencementdate',
                        'publicid',
                        'policystatus',
                        'beanversion',
                        'groupnumber',
                        'retired',
                        'createtime',
                        'groupname',
                        'effecitvedate',
                        'updateuserid',
                        'updatetime',
                        'id',
                        'policynumber',
                        'policyname'
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
        
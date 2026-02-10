
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:meCode::TEXT AS VARCHAR(30)) AS mecode,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:CSPStartDate::NUMBER/1000) AS cspstartdate,
                CAST(data_payload:BatchID::TEXT AS VARCHAR(10)) AS batchid,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(30)) AS accountnumber,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:Executed::BOOLEAN AS executed,
                CAST(data_payload:OldCRMUniqueID::TEXT AS VARCHAR(255)) AS oldcrmuniqueid,
                CAST(data_payload:CRMUniqueID_icare::TEXT AS VARCHAR(255)) AS crmuniqueid_icare,
                CAST(data_payload:GroupAccNumber::TEXT AS VARCHAR(30)) AS groupaccnumber,
                data_payload:CRMVersion_icare::NUMBER AS crmversion_icare,
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
            FROM {{ source('gwpc', 'pcx_groupaccrefdata_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:mecode::TEXT AS VARCHAR(30)) AS mecode,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:cspstartdate::TIMESTAMP_TZ AS cspstartdate,
                CAST($1:batchid::TEXT AS VARCHAR(10)) AS batchid,
                CAST($1:accountnumber::TEXT AS VARCHAR(30)) AS accountnumber,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:executed::BOOLEAN AS executed,
                CAST($1:oldcrmuniqueid::TEXT AS VARCHAR(255)) AS oldcrmuniqueid,
                CAST($1:crmuniqueid_icare::TEXT AS VARCHAR(255)) AS crmuniqueid_icare,
                CAST($1:groupaccnumber::TEXT AS VARCHAR(30)) AS groupaccnumber,
                $1:crmversion_icare::NUMBER AS crmversion_icare,
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
            FROM {{ source('gwpc', 'pcx_groupaccrefdata_ext') }}
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
                        'publicid',
                        'mecode',
                        'beanversion',
                        'retired',
                        'createtime',
                        'updateuserid',
                        'cspstartdate',
                        'batchid',
                        'accountnumber',
                        'updatetime',
                        'id',
                        'executed',
                        'oldcrmuniqueid',
                        'crmuniqueid_icare',
                        'groupaccnumber',
                        'crmversion_icare'
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
        
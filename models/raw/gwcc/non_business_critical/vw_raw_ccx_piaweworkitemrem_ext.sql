
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
                data_payload:ProcessHistoryID::NUMBER AS processhistoryid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Priority::NUMBER AS priority,
                data_payload:Attempts::NUMBER AS attempts,
                TO_TIMESTAMP_TZ(data_payload:LastUpdateTime::NUMBER/1000) AS lastupdatetime,
                TO_TIMESTAMP_TZ(data_payload:CreationTime::NUMBER/1000) AS creationtime,
                CAST(data_payload:Exception::TEXT AS VARCHAR(16777216)) AS exception,
                data_payload:Target::NUMBER AS target,
                data_payload:AvailableSince::NUMBER AS availablesince,
                data_payload:Status::NUMBER AS status,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CheckedOutBy::TEXT AS VARCHAR(50)) AS checkedoutby,
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
            FROM {{ source('gwcc', 'ccx_piaweworkitemrem_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:processhistoryid::NUMBER AS processhistoryid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:priority::NUMBER AS priority,
                $1:attempts::NUMBER AS attempts,
                $1:lastupdatetime::TIMESTAMP_TZ AS lastupdatetime,
                $1:creationtime::TIMESTAMP_TZ AS creationtime,
                CAST($1:exception::TEXT AS VARCHAR(16777216)) AS exception,
                $1:target::NUMBER AS target,
                $1:availablesince::NUMBER AS availablesince,
                $1:status::NUMBER AS status,
                $1:id::NUMBER AS id,
                CAST($1:checkedoutby::TEXT AS VARCHAR(50)) AS checkedoutby,
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
            FROM {{ source('gwcc', 'ccx_piaweworkitemrem_ext') }}
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
                        'processhistoryid',
                        'publicid',
                        'priority',
                        'attempts',
                        'lastupdatetime',
                        'creationtime',
                        'exception',
                        'target',
                        'availablesince',
                        'status',
                        'id',
                        'checkedoutby'
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
        
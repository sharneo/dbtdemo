
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
                data_payload:AverageNumMisses::NUMBER AS averagenummisses,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:AverageNumMissesWhenCacheFull::NUMBER AS averagenummisseswhencachefull,
                TO_TIMESTAMP_TZ(data_payload:SliceTimestamp::NUMBER/1000) AS slicetimestamp,
                data_payload:AverageNumHits::NUMBER AS averagenumhits,
                data_payload:ID::NUMBER AS id,
                data_payload:AverageSpaceRetained::NUMBER AS averagespaceretained,
                CAST(data_payload:ServerId::TEXT AS VARCHAR(255)) AS serverid,
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
            FROM {{ source('gwcc', 'cc_cachestatsrollupsnap') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:averagenummisses::NUMBER AS averagenummisses,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:averagenummisseswhencachefull::NUMBER AS averagenummisseswhencachefull,
                $1:slicetimestamp::TIMESTAMP_TZ AS slicetimestamp,
                $1:averagenumhits::NUMBER AS averagenumhits,
                $1:id::NUMBER AS id,
                $1:averagespaceretained::NUMBER AS averagespaceretained,
                CAST($1:serverid::TEXT AS VARCHAR(255)) AS serverid,
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
            FROM {{ source('gwcc', 'cc_cachestatsrollupsnap') }}
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
                                'averagenummisses',
                        'publicid',
                        'averagenummisseswhencachefull',
                        'slicetimestamp',
                        'averagenumhits',
                        'id',
                        'averagespaceretained',
                        'serverid'
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
        

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
                data_payload:AverageLength::NUMBER AS averagelength,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:MinimumLength::NUMBER AS minimumlength,
                data_payload:MaximumLength::NUMBER AS maximumlength,
                data_payload:TableDataDistID::NUMBER AS tabledatadistid,
                data_payload:ID::NUMBER AS id,
                data_payload:NumNonNull::NUMBER AS numnonnull,
                CAST(data_payload:ClobColName::TEXT AS VARCHAR(30)) AS clobcolname,
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
            FROM {{ source('gwcc', 'cc_clobcoldatadist') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:averagelength::NUMBER AS averagelength,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:minimumlength::NUMBER AS minimumlength,
                $1:maximumlength::NUMBER AS maximumlength,
                $1:tabledatadistid::NUMBER AS tabledatadistid,
                $1:id::NUMBER AS id,
                $1:numnonnull::NUMBER AS numnonnull,
                CAST($1:clobcolname::TEXT AS VARCHAR(30)) AS clobcolname,
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
            FROM {{ source('gwcc', 'cc_clobcoldatadist') }}
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
                                'averagelength',
                        'publicid',
                        'minimumlength',
                        'maximumlength',
                        'tabledatadistid',
                        'id',
                        'numnonnull',
                        'clobcolname'
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
        
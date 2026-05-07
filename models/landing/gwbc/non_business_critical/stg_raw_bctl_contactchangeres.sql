{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bctl_contactchangeres.
                                                contactchangeres_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bctl_contactchangeres"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:S_en_US_edg::NUMBER AS s_en_us_edg,
                CAST(data_payload:L_en_US::TEXT AS VARCHAR(256)) AS l_en_us,
                data_payload:PRIORITY::NUMBER AS priority,
                CAST(data_payload:TYPECODE::TEXT AS VARCHAR(50)) AS typecode,
                data_payload:S_en_US::NUMBER AS s_en_us,
                data_payload:RETIRED::BOOLEAN AS retired,
                CAST(data_payload:L_en_US_edg::TEXT AS VARCHAR(256)) AS l_en_us_edg,
                CAST(data_payload:NAME::TEXT AS VARCHAR(256)) AS name,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:DESCRIPTION::TEXT AS VARCHAR(512)) AS description,
                CAST(data_payload:L_en_AU::TEXT AS VARCHAR(256)) AS l_en_au,
                data_payload:S_en_AU::NUMBER AS s_en_au,
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
            FROM {{ source('gwbc', 'bctl_contactchangeres') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:s_en_us_edg::NUMBER AS s_en_us_edg,
                CAST($1:l_en_us::TEXT AS VARCHAR(256)) AS l_en_us,
                $1:priority::NUMBER AS priority,
                CAST($1:typecode::TEXT AS VARCHAR(50)) AS typecode,
                $1:s_en_us::NUMBER AS s_en_us,
                $1:retired::BOOLEAN AS retired,
                CAST($1:l_en_us_edg::TEXT AS VARCHAR(256)) AS l_en_us_edg,
                CAST($1:name::TEXT AS VARCHAR(256)) AS name,
                $1:id::NUMBER AS id,
                CAST($1:description::TEXT AS VARCHAR(512)) AS description,
                CAST($1:l_en_au::TEXT AS VARCHAR(256)) AS l_en_au,
                $1:s_en_au::NUMBER AS s_en_au,
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
            FROM {{ source('gwbc', 'bctl_contactchangeres') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS contactchangeres_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        's_en_us_edg',
                        'l_en_us',
                        'priority',
                        'typecode',
                        's_en_us',
                        'retired',
                        'l_en_us_edg',
                        'name',
                        'description',
                        'l_en_au',
                        's_en_au'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
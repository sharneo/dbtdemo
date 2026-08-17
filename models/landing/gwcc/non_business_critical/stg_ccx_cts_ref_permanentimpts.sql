{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_cts_ref_permanentimpts.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:Act::TEXT AS VARCHAR(255)) AS act,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:Benefit::TEXT AS VARCHAR(255)) AS benefit,
                TO_TIMESTAMP_TZ(data_payload:FromDate::NUMBER/1000) AS fromdate,
                TO_TIMESTAMP_TZ(data_payload:ToDate::NUMBER/1000) AS todate,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:Percentage AS NUMBER(4,1)) AS percentage,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Category::TEXT AS VARCHAR(255)) AS category,
                CAST(data_payload:SubCategory::TEXT AS VARCHAR(255)) AS subcategory,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS VARCHAR(300)) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_cts_ref_permanentimpts') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:act::TEXT AS VARCHAR(255)) AS act,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:benefit::TEXT AS VARCHAR(255)) AS benefit,
                $1:fromdate::TIMESTAMP_TZ AS fromdate,
                $1:todate::TIMESTAMP_TZ AS todate,
                $1:subtype::NUMBER AS subtype,
                CAST($1:percentage AS NUMBER(4,1)) AS percentage,
                $1:id::NUMBER AS id,
                CAST($1:category::TEXT AS VARCHAR(255)) AS category,
                CAST($1:subcategory::TEXT AS VARCHAR(255)) AS subcategory,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::VARCHAR(300) as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_cts_ref_permanentimpts') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS cts_ref_permanentimpts_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'act',
                        'publicid',
                        'benefit',
                        'fromdate',
                        'todate',
                        'subtype',
                        'percentage',
                        'category',
                        'subcategory'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

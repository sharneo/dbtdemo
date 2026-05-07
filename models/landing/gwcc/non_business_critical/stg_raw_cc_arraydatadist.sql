{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_arraydatadist.
                                                arraydatadist_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_arraydatadist"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:MinSize::NUMBER AS minsize,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ArrayName::TEXT AS VARCHAR(255)) AS arrayname,
                data_payload:MaxSize::NUMBER AS maxsize,
                CAST(data_payload:MeanSize AS NUMBER(12,2)) AS meansize,
                CAST(data_payload:ArrayEntityName::TEXT AS VARCHAR(255)) AS arrayentityname,
                data_payload:TableDataDistID::NUMBER AS tabledatadistid,
                data_payload:ID::NUMBER AS id,
                data_payload:MedianSize::NUMBER AS mediansize,
                CAST(data_payload:ArrayEntityTableName::TEXT AS VARCHAR(255)) AS arrayentitytablename,
                data_payload:NumNonNull::NUMBER AS numnonnull,
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
            FROM {{ source('gwcc', 'cc_arraydatadist') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:minsize::NUMBER AS minsize,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:arrayname::TEXT AS VARCHAR(255)) AS arrayname,
                $1:maxsize::NUMBER AS maxsize,
                CAST($1:meansize AS NUMBER(12,2)) AS meansize,
                CAST($1:arrayentityname::TEXT AS VARCHAR(255)) AS arrayentityname,
                $1:tabledatadistid::NUMBER AS tabledatadistid,
                $1:id::NUMBER AS id,
                $1:mediansize::NUMBER AS mediansize,
                CAST($1:arrayentitytablename::TEXT AS VARCHAR(255)) AS arrayentitytablename,
                $1:numnonnull::NUMBER AS numnonnull,
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
            FROM {{ source('gwcc', 'cc_arraydatadist') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS arraydatadist_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'minsize',
                        'publicid',
                        'arrayname',
                        'maxsize',
                        'meansize',
                        'arrayentityname',
                        'tabledatadistid',
                        'mediansize',
                        'arrayentitytablename',
                        'numnonnull'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
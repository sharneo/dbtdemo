{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-06-01      0.0                             Incremental staging model for ccx_psychrelevtwrap_ext.
                                                psychrelevtwrap_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "claim_centre", "business_critical", "ccx_psychrelevtwrap_ext"]
) }}

WITH cte_source_data AS
(
            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:ClaimWorkCompID::NUMBER AS claimworkcompid,
                data_payload:RelevantEvent::NUMBER AS relevantevent,
                data_payload:Retired::NUMBER AS retired,
                CAST(NULL AS TIMESTAMP_LTZ) AS gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) AS gwcbi_lsn,
                CAST(NULL AS NUMBER) AS gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) AS gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) AS gwcbi_seqval,
                CAST(NULL AS STRING) AS gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) AS gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' AS file_type,
                'GWCC' AS source_system
            FROM {{ source('gwcc', 'ccx_psychrelevtwrap_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'

            UNION ALL

            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:claimworkcompid::NUMBER AS claimworkcompid,
                $1:relevantevent::NUMBER AS relevantevent,
                $1:retired::NUMBER AS retired,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) AS gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER AS gwcbi_lsn,
                $1:gwcbi___operation::NUMBER AS gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) AS gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER AS gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING AS gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER AS gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' AS file_type,
                'GWCC' AS source_system
            FROM {{ source('gwcc', 'ccx_psychrelevtwrap_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key(['id']) }} AS VARCHAR(150)) AS psychrelevtwrap_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
            'loadcommandid',
            'createuserid',
            'publicid',
            'createtime',
            'updateuserid',
            'updatetime',
            'beanversion',
            'archivepartition',
            'claimworkcompid',
            'relevantevent',
            'retired'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY record_insertion_date DESC 
    ) = 1
)

SELECT * FROM cte_transformed
{%- if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{%- endif %}

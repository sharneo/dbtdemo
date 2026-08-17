{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_inboundfile_psc.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwpc", "policy_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:Checksum::TEXT AS VARCHAR(255)) AS checksum,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:PurgedDate::NUMBER/1000) AS purgeddate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Status::NUMBER AS status,
                TO_TIMESTAMP_TZ(data_payload:LoadedDate::NUMBER/1000) AS loadeddate,
                CAST(data_payload:HandlerName::TEXT AS VARCHAR(255)) AS handlername,
                CAST(data_payload:ArchiveLocation::TEXT AS VARCHAR(1333)) AS archivelocation,
                data_payload:Type::NUMBER AS type,
                data_payload:ID::NUMBER AS id,
                data_payload:ImportedFromUI_icare::BOOLEAN AS importedfromui_icare,
                CAST(data_payload:InputLocation::TEXT AS VARCHAR(1333)) AS inputlocation,
                CAST(data_payload:ErrorMessage::TEXT AS VARCHAR(1333)) AS errormessage,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_inboundfile_psc') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:checksum::TEXT AS VARCHAR(255)) AS checksum,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:purgeddate::TIMESTAMP_TZ AS purgeddate,
                $1:beanversion::NUMBER AS beanversion,
                $1:status::NUMBER AS status,
                $1:loadeddate::TIMESTAMP_TZ AS loadeddate,
                CAST($1:handlername::TEXT AS VARCHAR(255)) AS handlername,
                CAST($1:archivelocation::TEXT AS VARCHAR(1333)) AS archivelocation,
                $1:type::NUMBER AS type,
                $1:id::NUMBER AS id,
                $1:importedfromui_icare::BOOLEAN AS importedfromui_icare,
                CAST($1:inputlocation::TEXT AS VARCHAR(1333)) AS inputlocation,
                CAST($1:errormessage::TEXT AS VARCHAR(1333)) AS errormessage,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_inboundfile_psc') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS inboundfile_psc_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'checksum',
                        'publicid',
                        'purgeddate',
                        'beanversion',
                        'status',
                        'loadeddate',
                        'handlername',
                        'archivelocation',
                        'type',
                        'importedfromui_icare',
                        'inputlocation',
                        'errormessage'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

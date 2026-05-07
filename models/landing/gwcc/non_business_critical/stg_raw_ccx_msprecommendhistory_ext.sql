{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_msprecommendhistory_ext.
                                                msprecommendhistory_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_msprecommendhistory_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:MSPRecommendation::NUMBER AS msprecommendation,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:SummaryAndRecommendation::TEXT AS VARCHAR(16777216)) AS summaryandrecommendation,
                CAST(data_payload:CaseSummaryForIME::TEXT AS VARCHAR(16777216)) AS casesummaryforime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:IMESpecialityRequired::TEXT AS VARCHAR(255)) AS imespecialityrequired,
                CAST(data_payload:QuestionsAddressedInIME::TEXT AS VARCHAR(16777216)) AS questionsaddressedinime,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:MSPRecommendationID::NUMBER AS msprecommendationid,
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
            FROM {{ source('gwcc', 'ccx_msprecommendhistory_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:msprecommendation::NUMBER AS msprecommendation,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:summaryandrecommendation::TEXT AS VARCHAR(16777216)) AS summaryandrecommendation,
                CAST($1:casesummaryforime::TEXT AS VARCHAR(16777216)) AS casesummaryforime,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:imespecialityrequired::TEXT AS VARCHAR(255)) AS imespecialityrequired,
                CAST($1:questionsaddressedinime::TEXT AS VARCHAR(16777216)) AS questionsaddressedinime,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:msprecommendationid::NUMBER AS msprecommendationid,
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
            FROM {{ source('gwcc', 'ccx_msprecommendhistory_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS msprecommendhistory_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'msprecommendation',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'createtime',
                        'summaryandrecommendation',
                        'casesummaryforime',
                        'updateuserid',
                        'imespecialityrequired',
                        'questionsaddressedinime',
                        'updatetime',
                        'msprecommendationid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
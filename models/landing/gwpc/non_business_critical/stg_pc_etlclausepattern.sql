{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_etlclausepattern.
                                                etlclausepattern_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pc_etlclausepattern"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CoverageCategory::TEXT AS VARCHAR(255)) AS coveragecategory,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:ClauseType::TEXT AS VARCHAR(255)) AS clausetype,
                CAST(data_payload:CoveredPartyType::TEXT AS VARCHAR(255)) AS coveredpartytype,
                CAST(data_payload:Name::TEXT AS VARCHAR(255)) AS name,
                CAST(data_payload:PatternID::TEXT AS VARCHAR(255)) AS patternid,
                CAST(data_payload:CoverageSubtype::TEXT AS VARCHAR(255)) AS coveragesubtype,
                CAST(data_payload:CodeIdentifier::TEXT AS VARCHAR(255)) AS codeidentifier,
                CAST(data_payload:OwningEntityType::TEXT AS VARCHAR(255)) AS owningentitytype,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS STRING) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pc_etlclausepattern') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:coveragecategory::TEXT AS VARCHAR(255)) AS coveragecategory,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:clausetype::TEXT AS VARCHAR(255)) AS clausetype,
                CAST($1:coveredpartytype::TEXT AS VARCHAR(255)) AS coveredpartytype,
                CAST($1:name::TEXT AS VARCHAR(255)) AS name,
                CAST($1:patternid::TEXT AS VARCHAR(255)) AS patternid,
                CAST($1:coveragesubtype::TEXT AS VARCHAR(255)) AS coveragesubtype,
                CAST($1:codeidentifier::TEXT AS VARCHAR(255)) AS codeidentifier,
                CAST($1:owningentitytype::TEXT AS VARCHAR(255)) AS owningentitytype,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pc_etlclausepattern') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS etlclausepattern_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'publicid',
                        'coveragecategory',
                        'beanversion',
                        'clausetype',
                        'coveredpartytype',
                        'name',
                        'patternid',
                        'coveragesubtype',
                        'codeidentifier',
                        'owningentitytype',
                        'subtype'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
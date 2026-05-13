{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_inboundrecord_psc.
                                                inboundrecord_psc_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_inboundrecord_psc"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:InboundPropertyGroup::NUMBER AS inboundpropertygroup,
                data_payload:Status::NUMBER AS status,
                data_payload:DistributeProcessing::BOOLEAN AS distributeprocessing,
                CAST(data_payload:HandlerName::TEXT AS VARCHAR(255)) AS handlername,
                data_payload:InboundFile::NUMBER AS inboundfile,
                data_payload:FileType::NUMBER AS filetype,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Content::TEXT AS VARCHAR(16777216)) AS content,
                data_payload:ID::NUMBER AS id,
                data_payload:LineNumber::NUMBER AS linenumber,
                CAST(data_payload:ErrorMessage::TEXT AS VARCHAR(1333)) AS errormessage,
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
            FROM {{ source('gwpc', 'pcx_inboundrecord_psc') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:inboundpropertygroup::NUMBER AS inboundpropertygroup,
                $1:status::NUMBER AS status,
                $1:distributeprocessing::BOOLEAN AS distributeprocessing,
                CAST($1:handlername::TEXT AS VARCHAR(255)) AS handlername,
                $1:inboundfile::NUMBER AS inboundfile,
                $1:filetype::NUMBER AS filetype,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:content::TEXT AS VARCHAR(16777216)) AS content,
                $1:id::NUMBER AS id,
                $1:linenumber::NUMBER AS linenumber,
                CAST($1:errormessage::TEXT AS VARCHAR(1333)) AS errormessage,
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
            FROM {{ source('gwpc', 'pcx_inboundrecord_psc') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS inboundrecord_psc_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'createtime',
                        'updateuserid',
                        'inboundpropertygroup',
                        'status',
                        'distributeprocessing',
                        'handlername',
                        'inboundfile',
                        'filetype',
                        'updatetime',
                        'content',
                        'linenumber',
                        'errormessage'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
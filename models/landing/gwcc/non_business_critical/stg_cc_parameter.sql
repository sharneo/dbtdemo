{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_parameter.
                                                parameter_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_parameter"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:StringValue::TEXT AS VARCHAR(255)) AS stringvalue,
                data_payload:IntValue::NUMBER AS intvalue,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ParameterType::NUMBER AS parametertype,
                TO_TIMESTAMP_TZ(data_payload:DateValue::NUMBER/1000) AS datevalue,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:LongTextValue::TEXT AS VARCHAR(16777216)) AS longtextvalue,
                data_payload:BooleanValue::BOOLEAN AS booleanvalue,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:ParameterName::TEXT AS VARCHAR(255)) AS parametername,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ComponentType::NUMBER AS componenttype,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_parameter') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:stringvalue::TEXT AS VARCHAR(255)) AS stringvalue,
                $1:intvalue::NUMBER AS intvalue,
                $1:createuserid::NUMBER AS createuserid,
                $1:parametertype::NUMBER AS parametertype,
                $1:datevalue::TIMESTAMP_TZ AS datevalue,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:longtextvalue::TEXT AS VARCHAR(16777216)) AS longtextvalue,
                $1:booleanvalue::BOOLEAN AS booleanvalue,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:parametername::TEXT AS VARCHAR(255)) AS parametername,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:componenttype::NUMBER AS componenttype,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_parameter') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS parameter_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'stringvalue',
                        'intvalue',
                        'createuserid',
                        'parametertype',
                        'datevalue',
                        'publicid',
                        'beanversion',
                        'longtextvalue',
                        'booleanvalue',
                        'retired',
                        'createtime',
                        'updateuserid',
                        'parametername',
                        'updatetime',
                        'componenttype'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
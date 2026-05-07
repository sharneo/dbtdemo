{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_adminaudit_ext.
                                                adminaudit_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_adminaudit_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:OriginalValue::TEXT AS VARCHAR(16777216)) AS originalvalue,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ModifiedEntityID::NUMBER AS modifiedentityid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:NewValue::TEXT AS VARCHAR(16777216)) AS newvalue,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:ModifiedDate::NUMBER/1000) AS modifieddate,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ModifiedByUserID::TEXT AS VARCHAR(255)) AS modifiedbyuserid,
                CAST(data_payload:TriggerEventName::TEXT AS VARCHAR(255)) AS triggereventname,
                CAST(data_payload:ModifiedUserFullName::TEXT AS VARCHAR(510)) AS modifieduserfullname,
                CAST(data_payload:ModifiedEntityPublicID::TEXT AS VARCHAR(255)) AS modifiedentitypublicid,
                CAST(data_payload:ModifiedObjectName::TEXT AS VARCHAR(255)) AS modifiedobjectname,
                CAST(data_payload:ModifiedByUserName::TEXT AS VARCHAR(510)) AS modifiedbyusername,
                CAST(data_payload:ModifiedEntityName::TEXT AS VARCHAR(255)) AS modifiedentityname,
                CAST(data_payload:ModifiedFieldName::TEXT AS VARCHAR(255)) AS modifiedfieldname,
                CAST(data_payload:ModifiedUsername::TEXT AS VARCHAR(255)) AS modifiedusername,
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
            FROM {{ source('gwpc', 'pcx_adminaudit_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:originalvalue::TEXT AS VARCHAR(16777216)) AS originalvalue,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:modifiedentityid::NUMBER AS modifiedentityid,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:newvalue::TEXT AS VARCHAR(16777216)) AS newvalue,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:modifieddate::TIMESTAMP_TZ AS modifieddate,
                $1:id::NUMBER AS id,
                CAST($1:modifiedbyuserid::TEXT AS VARCHAR(255)) AS modifiedbyuserid,
                CAST($1:triggereventname::TEXT AS VARCHAR(255)) AS triggereventname,
                CAST($1:modifieduserfullname::TEXT AS VARCHAR(510)) AS modifieduserfullname,
                CAST($1:modifiedentitypublicid::TEXT AS VARCHAR(255)) AS modifiedentitypublicid,
                CAST($1:modifiedobjectname::TEXT AS VARCHAR(255)) AS modifiedobjectname,
                CAST($1:modifiedbyusername::TEXT AS VARCHAR(510)) AS modifiedbyusername,
                CAST($1:modifiedentityname::TEXT AS VARCHAR(255)) AS modifiedentityname,
                CAST($1:modifiedfieldname::TEXT AS VARCHAR(255)) AS modifiedfieldname,
                CAST($1:modifiedusername::TEXT AS VARCHAR(255)) AS modifiedusername,
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
            FROM {{ source('gwpc', 'pcx_adminaudit_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS adminaudit_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'originalvalue',
                        'createuserid',
                        'publicid',
                        'modifiedentityid',
                        'beanversion',
                        'createtime',
                        'retired',
                        'updateuserid',
                        'newvalue',
                        'updatetime',
                        'modifieddate',
                        'modifiedbyuserid',
                        'triggereventname',
                        'modifieduserfullname',
                        'modifiedentitypublicid',
                        'modifiedobjectname',
                        'modifiedbyusername',
                        'modifiedentityname',
                        'modifiedfieldname',
                        'modifiedusername'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
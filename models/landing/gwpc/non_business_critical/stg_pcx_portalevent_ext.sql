{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_portalevent_ext.
                                                portalevent_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_portalevent_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:AckId::TEXT AS VARCHAR(60)) AS ackid,
                CAST(data_payload:ProjectName::TEXT AS VARCHAR(500)) AS projectname,
                CAST(data_payload:JobNumber::TEXT AS VARCHAR(60)) AS jobnumber,
                CAST(data_payload:Operation::TEXT AS VARCHAR(60)) AS operation,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:AgencyCRMId::TEXT AS VARCHAR(255)) AS agencycrmid,
                CAST(data_payload:AppErrorCode::TEXT AS VARCHAR(60)) AS apperrorcode,
                CAST(data_payload:ErrorCode::TEXT AS VARCHAR(60)) AS errorcode,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Consumer::TEXT AS VARCHAR(60)) AS consumer,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(60)) AS policynumber,
                CAST(data_payload:ErrorMessage::TEXT AS VARCHAR(500)) AS errormessage,
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
            FROM {{ source('gwpc', 'pcx_portalevent_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:ackid::TEXT AS VARCHAR(60)) AS ackid,
                CAST($1:projectname::TEXT AS VARCHAR(500)) AS projectname,
                CAST($1:jobnumber::TEXT AS VARCHAR(60)) AS jobnumber,
                CAST($1:operation::TEXT AS VARCHAR(60)) AS operation,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:status::NUMBER AS status,
                CAST($1:agencycrmid::TEXT AS VARCHAR(255)) AS agencycrmid,
                CAST($1:apperrorcode::TEXT AS VARCHAR(60)) AS apperrorcode,
                CAST($1:errorcode::TEXT AS VARCHAR(60)) AS errorcode,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:consumer::TEXT AS VARCHAR(60)) AS consumer,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                CAST($1:policynumber::TEXT AS VARCHAR(60)) AS policynumber,
                CAST($1:errormessage::TEXT AS VARCHAR(500)) AS errormessage,
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
            FROM {{ source('gwpc', 'pcx_portalevent_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS portalevent_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'ackid',
                        'projectname',
                        'jobnumber',
                        'operation',
                        'beanversion',
                        'createtime',
                        'retired',
                        'updateuserid',
                        'status',
                        'agencycrmid',
                        'apperrorcode',
                        'errorcode',
                        'updatetime',
                        'consumer',
                        'subtype',
                        'policynumber',
                        'errormessage'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
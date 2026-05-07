{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_medicalaction.
                                                medicalaction_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "cc_medicalaction"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:MedicalActionType::NUMBER AS medicalactiontype,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ExposureID::NUMBER AS exposureid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:ActionDate::NUMBER/1000) AS actiondate,
                data_payload:FutureActionReq::NUMBER AS futureactionreq,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:NextAppointment::NUMBER/1000) AS nextappointment,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Note::TEXT AS VARCHAR(255)) AS note,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ProviderName::TEXT AS VARCHAR(60)) AS providername,
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
            FROM {{ source('gwcc', 'cc_medicalaction') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:medicalactiontype::NUMBER AS medicalactiontype,
                $1:createuserid::NUMBER AS createuserid,
                $1:exposureid::NUMBER AS exposureid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:actiondate::TIMESTAMP_TZ AS actiondate,
                $1:futureactionreq::NUMBER AS futureactionreq,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:nextappointment::TIMESTAMP_TZ AS nextappointment,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:note::TEXT AS VARCHAR(255)) AS note,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:providername::TEXT AS VARCHAR(60)) AS providername,
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
            FROM {{ source('gwcc', 'cc_medicalaction') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS medicalaction_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'medicalactiontype',
                        'createuserid',
                        'exposureid',
                        'publicid',
                        'actiondate',
                        'futureactionreq',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'createtime',
                        'nextappointment',
                        'updateuserid',
                        'note',
                        'updatetime',
                        'providername'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
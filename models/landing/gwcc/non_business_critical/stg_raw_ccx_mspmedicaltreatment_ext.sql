{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_mspmedicaltreatment_ext.
                                                mspmedicaltreatment_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_mspmedicaltreatment_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:MSPReferralID::NUMBER AS mspreferralid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:MedTreatmentID::TEXT AS VARCHAR(15)) AS medtreatmentid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:TreatmentDecisionDueDate::NUMBER/1000) AS treatmentdecisionduedate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:TreatmentType::TEXT AS VARCHAR(255)) AS treatmenttype,
                data_payload:ApprovalStatus::NUMBER AS approvalstatus,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:ContactID::NUMBER AS contactid,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                TO_TIMESTAMP_TZ(data_payload:RequestDate::NUMBER/1000) AS requestdate,
                data_payload:Category::NUMBER AS category,
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
            FROM {{ source('gwcc', 'ccx_mspmedicaltreatment_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:mspreferralid::NUMBER AS mspreferralid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:medtreatmentid::TEXT AS VARCHAR(15)) AS medtreatmentid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:treatmentdecisionduedate::TIMESTAMP_TZ AS treatmentdecisionduedate,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:treatmenttype::TEXT AS VARCHAR(255)) AS treatmenttype,
                $1:approvalstatus::NUMBER AS approvalstatus,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:contactid::NUMBER AS contactid,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:requestdate::TIMESTAMP_TZ AS requestdate,
                $1:category::NUMBER AS category,
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
            FROM {{ source('gwcc', 'ccx_mspmedicaltreatment_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS mspmedicaltreatment_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'mspreferralid',
                        'publicid',
                        'medtreatmentid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'treatmentdecisionduedate',
                        'updateuserid',
                        'treatmenttype',
                        'approvalstatus',
                        'updatetime',
                        'contactid',
                        'description',
                        'requestdate',
                        'category'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
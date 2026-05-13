{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_medicaltreatment.
                                                medicaltreatment_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_medicaltreatment"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ApprovedTreatment::NUMBER AS approvedtreatment,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:TreatmentQuantity::NUMBER AS treatmentquantity,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:ActionDate::NUMBER/1000) AS actiondate,
                CAST(data_payload:ICD1::TEXT AS VARCHAR(8)) AS icd1,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:ICD2::TEXT AS VARCHAR(8)) AS icd2,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:ICD3::TEXT AS VARCHAR(8)) AS icd3,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:ICD4::TEXT AS VARCHAR(8)) AS icd4,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                data_payload:ClaimContactID::NUMBER AS claimcontactid,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime_Ext::NUMBER/1000) AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_medicaltreatment') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:approvedtreatment::NUMBER AS approvedtreatment,
                $1:createuserid::NUMBER AS createuserid,
                $1:treatmentquantity::NUMBER AS treatmentquantity,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:actiondate::TIMESTAMP_TZ AS actiondate,
                CAST($1:icd1::TEXT AS VARCHAR(8)) AS icd1,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:icd2::TEXT AS VARCHAR(8)) AS icd2,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:icd3::TEXT AS VARCHAR(8)) AS icd3,
                $1:retired::NUMBER AS retired,
                CAST($1:icd4::TEXT AS VARCHAR(8)) AS icd4,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                $1:claimcontactid::NUMBER AS claimcontactid,
                $1:legacycreatetime_ext::TIMESTAMP_TZ AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_medicaltreatment') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS medicaltreatment_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'approvedtreatment',
                        'createuserid',
                        'treatmentquantity',
                        'publicid',
                        'actiondate',
                        'icd1',
                        'beanversion',
                        'icd2',
                        'archivepartition',
                        'createtime',
                        'icd3',
                        'retired',
                        'icd4',
                        'updateuserid',
                        'updatetime',
                        'claimid',
                        'claimcontactid',
                        'legacycreatetime_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
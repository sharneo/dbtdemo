{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_paycode_icare.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:PaymentType::TEXT AS VARCHAR(255)) AS paymenttype,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                TO_TIMESTAMP_TZ(data_payload:ExpiryDate::NUMBER/1000) AS expirydate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:PaymentCategory::TEXT AS VARCHAR(255)) AS paymentcategory,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                CAST(data_payload:ReplacedAMACode::TEXT AS VARCHAR(10)) AS replacedamacode,
                CAST(data_payload:PaymentSubType::TEXT AS VARCHAR(3000)) AS paymentsubtype,
                CAST(data_payload:Paycode::TEXT AS VARCHAR(10)) AS paycode,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:Source::NUMBER AS source,
                CAST(data_payload:OldAMACode::TEXT AS VARCHAR(10)) AS oldamacode,
                data_payload:ExcludeMedicalValidation::BOOLEAN AS excludemedicalvalidation,
                TO_TIMESTAMP_TZ(data_payload:ArchivedDate::NUMBER/1000) AS archiveddate,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_paycode_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:paymenttype::TEXT AS VARCHAR(255)) AS paymenttype,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:expirydate::TIMESTAMP_TZ AS expirydate,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:paymentcategory::TEXT AS VARCHAR(255)) AS paymentcategory,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                CAST($1:replacedamacode::TEXT AS VARCHAR(10)) AS replacedamacode,
                CAST($1:paymentsubtype::TEXT AS VARCHAR(3000)) AS paymentsubtype,
                CAST($1:paycode::TEXT AS VARCHAR(10)) AS paycode,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:source::NUMBER AS source,
                CAST($1:oldamacode::TEXT AS VARCHAR(10)) AS oldamacode,
                $1:excludemedicalvalidation::BOOLEAN AS excludemedicalvalidation,
                $1:archiveddate::TIMESTAMP_TZ AS archiveddate,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_paycode_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS paycode_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'paymenttype',
                        'loadcommandid',
                        'expirydate',
                        'createuserid',
                        'publicid',
                        'paymentcategory',
                        'beanversion',
                        'retired',
                        'createtime',
                        'updateuserid',
                        'startdate',
                        'replacedamacode',
                        'paymentsubtype',
                        'paycode',
                        'updatetime',
                        'source',
                        'oldamacode',
                        'excludemedicalvalidation',
                        'archiveddate'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

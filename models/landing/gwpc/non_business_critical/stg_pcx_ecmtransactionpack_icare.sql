{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_ecmtransactionpack_icare.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwpc", "policy_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:TransactionID::TEXT AS VARCHAR(15)) AS transactionid,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Payload::TEXT AS VARCHAR(16777216)) AS payload,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:BrokerCopyStatus::NUMBER AS brokercopystatus,
                data_payload:GroupNumber::NUMBER AS groupnumber,
                data_payload:PolicyType::NUMBER AS policytype,
                CAST(data_payload:BCPayload::TEXT AS VARCHAR(16777216)) AS bcpayload,
                data_payload:AddresseeType::NUMBER AS addresseetype,
                data_payload:ProcessMethod::NUMBER AS processmethod,
                CAST(data_payload:PolPeriodPublicID::TEXT AS VARCHAR(64)) AS polperiodpublicid,
                data_payload:PackType::NUMBER AS packtype,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(255)) AS accountnumber,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(40)) AS policynumber,
                CAST(data_payload:ParentTransactionID::TEXT AS VARCHAR(255)) AS parenttransactionid,
                data_payload:WaitDocResendFail_Ext::BOOLEAN AS waitdocresendfail_ext,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_ecmtransactionpack_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:transactionid::TEXT AS VARCHAR(15)) AS transactionid,
                $1:id::NUMBER AS id,
                CAST($1:payload::TEXT AS VARCHAR(16777216)) AS payload,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:brokercopystatus::NUMBER AS brokercopystatus,
                $1:groupnumber::NUMBER AS groupnumber,
                $1:policytype::NUMBER AS policytype,
                CAST($1:bcpayload::TEXT AS VARCHAR(16777216)) AS bcpayload,
                $1:addresseetype::NUMBER AS addresseetype,
                $1:processmethod::NUMBER AS processmethod,
                CAST($1:polperiodpublicid::TEXT AS VARCHAR(64)) AS polperiodpublicid,
                $1:packtype::NUMBER AS packtype,
                CAST($1:accountnumber::TEXT AS VARCHAR(255)) AS accountnumber,
                CAST($1:policynumber::TEXT AS VARCHAR(40)) AS policynumber,
                CAST($1:parenttransactionid::TEXT AS VARCHAR(255)) AS parenttransactionid,
                $1:waitdocresendfail_ext::BOOLEAN AS waitdocresendfail_ext,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_ecmtransactionpack_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS ecmtransactionpack_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'createuserid',
                        'publicid',
                        'updatetime',
                        'beanversion',
                        'createtime',
                        'retired',
                        'transactionid',
                        'payload',
                        'updateuserid',
                        'brokercopystatus',
                        'groupnumber',
                        'policytype',
                        'bcpayload',
                        'addresseetype',
                        'processmethod',
                        'polperiodpublicid',
                        'packtype',
                        'accountnumber',
                        'policynumber',
                        'parenttransactionid',
                        'waitdocresendfail_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

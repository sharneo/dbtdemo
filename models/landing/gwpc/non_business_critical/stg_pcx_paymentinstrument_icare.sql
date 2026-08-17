{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_paymentinstrument_icare.
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
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:PaymentMethod::NUMBER AS paymentmethod,
                CAST(data_payload:BankName::TEXT AS VARCHAR(255)) AS bankname,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:Account::NUMBER AS account,
                CAST(data_payload:BranchName::TEXT AS VARCHAR(255)) AS branchname,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:IsActive::BOOLEAN AS isactive,
                CAST(data_payload:BSB::TEXT AS VARCHAR(255)) AS bsb,
                data_payload:BSBDetails::NUMBER AS bsbdetails,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(255)) AS accountnumber,
                CAST(data_payload:AccountName::TEXT AS VARCHAR(255)) AS accountname,
                CAST(data_payload:Token::TEXT AS VARCHAR(255)) AS token,
                data_payload:ID::NUMBER AS id,
                data_payload:CreditCard::NUMBER AS creditcard,
                TO_TIMESTAMP_TZ(data_payload:TimeStamp::NUMBER/1000) AS timestamp,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
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
            FROM {{ source('gwpc', 'pcx_paymentinstrument_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:paymentmethod::NUMBER AS paymentmethod,
                CAST($1:bankname::TEXT AS VARCHAR(255)) AS bankname,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:account::NUMBER AS account,
                CAST($1:branchname::TEXT AS VARCHAR(255)) AS branchname,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:isactive::BOOLEAN AS isactive,
                CAST($1:bsb::TEXT AS VARCHAR(255)) AS bsb,
                $1:bsbdetails::NUMBER AS bsbdetails,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:accountnumber::TEXT AS VARCHAR(255)) AS accountnumber,
                CAST($1:accountname::TEXT AS VARCHAR(255)) AS accountname,
                CAST($1:token::TEXT AS VARCHAR(255)) AS token,
                $1:id::NUMBER AS id,
                $1:creditcard::NUMBER AS creditcard,
                $1:timestamp::TIMESTAMP_TZ AS timestamp,
                $1:archivepartition::NUMBER AS archivepartition,
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
            FROM {{ source('gwpc', 'pcx_paymentinstrument_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS paymentinstrument_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'paymentmethod',
                        'bankname',
                        'beanversion',
                        'createtime',
                        'retired',
                        'account',
                        'branchname',
                        'updateuserid',
                        'isactive',
                        'bsb',
                        'bsbdetails',
                        'updatetime',
                        'accountnumber',
                        'accountname',
                        'token',
                        'creditcard',
                        'timestamp',
                        'archivepartition'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

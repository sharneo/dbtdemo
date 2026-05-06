{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bcx_claimrecoverydetails.
                                                claimrecoverydetails_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bcx_claimrecoverydetails"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:ClaimPublicID::TEXT AS VARCHAR(64)) AS claimpublicid,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(40)) AS claimnumber,
                CAST(data_payload:Message::TEXT AS VARCHAR(100)) AS message,
                CAST(data_payload:InvoiceType::TEXT AS VARCHAR(150)) AS invoicetype,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:RecoveryType::NUMBER AS recoverytype,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(255)) AS accountnumber,
                data_payload:InvoiceIssueFlag::BOOLEAN AS invoiceissueflag,
                CAST(data_payload:RecoveryPublicID::TEXT AS VARCHAR(64)) AS recoverypublicid,
                CAST(data_payload:NewRecoveryPublicID::TEXT AS VARCHAR(64)) AS newrecoverypublicid,
                data_payload:WaiveFlag::BOOLEAN AS waiveflag,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PAYGAmount AS NUMBER(18,2)) AS paygamount,
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
            FROM {{ source('gwbc', 'bcx_claimrecoverydetails') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:claimpublicid::TEXT AS VARCHAR(64)) AS claimpublicid,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:claimnumber::TEXT AS VARCHAR(40)) AS claimnumber,
                CAST($1:message::TEXT AS VARCHAR(100)) AS message,
                CAST($1:invoicetype::TEXT AS VARCHAR(150)) AS invoicetype,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:recoverytype::NUMBER AS recoverytype,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:accountnumber::TEXT AS VARCHAR(255)) AS accountnumber,
                $1:invoiceissueflag::BOOLEAN AS invoiceissueflag,
                CAST($1:recoverypublicid::TEXT AS VARCHAR(64)) AS recoverypublicid,
                CAST($1:newrecoverypublicid::TEXT AS VARCHAR(64)) AS newrecoverypublicid,
                $1:waiveflag::BOOLEAN AS waiveflag,
                $1:id::NUMBER AS id,
                CAST($1:paygamount AS NUMBER(18,2)) AS paygamount,
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
            FROM {{ source('gwbc', 'bcx_claimrecoverydetails') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS claimrecoverydetails_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'claimpublicid',
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'claimnumber',
                        'message',
                        'invoicetype',
                        'beanversion',
                        'createtime',
                        'retired',
                        'updateuserid',
                        'recoverytype',
                        'updatetime',
                        'accountnumber',
                        'invoiceissueflag',
                        'recoverypublicid',
                        'newrecoverypublicid',
                        'waiveflag',
                        'paygamount'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
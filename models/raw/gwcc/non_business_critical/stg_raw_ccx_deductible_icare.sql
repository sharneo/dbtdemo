{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_deductible_icare.
                                                deductible_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_deductible_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:CoverageID::NUMBER AS coverageid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Paid::BOOLEAN AS paid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Currency::NUMBER AS currency,
                data_payload:Overridden::BOOLEAN AS overridden,
                data_payload:Waived::BOOLEAN AS waived,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:EditReason::TEXT AS VARCHAR(255)) AS editreason,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime::NUMBER/1000) AS legacycreatetime,
                CAST(data_payload:InvoiceAmountSentToBC AS NUMBER(18,2)) AS invoiceamountsenttobc,
                TO_TIMESTAMP_TZ(data_payload:InvoiceDate::NUMBER/1000) AS invoicedate,
                TO_TIMESTAMP_TZ(data_payload:PaymentDueDate::NUMBER/1000) AS paymentduedate,
                CAST(data_payload:CRNNumber::TEXT AS VARCHAR(255)) AS crnnumber,
                CAST(data_payload:InvoiceNumber::TEXT AS VARCHAR(50)) AS invoicenumber,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(50)) AS accountnumber,
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
            FROM {{ source('gwcc', 'ccx_deductible_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:coverageid::NUMBER AS coverageid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:paid::BOOLEAN AS paid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:currency::NUMBER AS currency,
                $1:overridden::BOOLEAN AS overridden,
                $1:waived::BOOLEAN AS waived,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:id::NUMBER AS id,
                CAST($1:editreason::TEXT AS VARCHAR(255)) AS editreason,
                $1:legacycreatetime::TIMESTAMP_TZ AS legacycreatetime,
                CAST($1:invoiceamountsenttobc AS NUMBER(18,2)) AS invoiceamountsenttobc,
                $1:invoicedate::TIMESTAMP_TZ AS invoicedate,
                $1:paymentduedate::TIMESTAMP_TZ AS paymentduedate,
                CAST($1:crnnumber::TEXT AS VARCHAR(255)) AS crnnumber,
                CAST($1:invoicenumber::TEXT AS VARCHAR(50)) AS invoicenumber,
                CAST($1:accountnumber::TEXT AS VARCHAR(50)) AS accountnumber,
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
            FROM {{ source('gwcc', 'ccx_deductible_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS deductible_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'coverageid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'paid',
                        'updateuserid',
                        'currency',
                        'overridden',
                        'waived',
                        'updatetime',
                        'claimid',
                        'amount',
                        'editreason',
                        'legacycreatetime',
                        'invoiceamountsenttobc',
                        'invoicedate',
                        'paymentduedate',
                        'crnnumber',
                        'invoicenumber',
                        'accountnumber'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
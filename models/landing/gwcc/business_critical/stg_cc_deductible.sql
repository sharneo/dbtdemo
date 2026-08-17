{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_deductible.
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
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:CoverageID::NUMBER AS coverageid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:Paid::BOOLEAN AS paid,
                CAST(data_payload:EditReason::TEXT AS VARCHAR(255)) AS editreason,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Currency::NUMBER AS currency,
                CAST(data_payload:InvoiceNumber_icare::TEXT AS VARCHAR(50)) AS invoicenumber_icare,
                CAST(data_payload:AccountNumber_icare::TEXT AS VARCHAR(50)) AS accountnumber_icare,
                data_payload:Overridden::BOOLEAN AS overridden,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Waived::BOOLEAN AS waived,
                data_payload:ClaimID::NUMBER AS claimid,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                CAST(data_payload:InvoiceAmountSentToBC AS NUMBER(18,2)) AS invoiceamountsenttobc,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CRNNumber_Ext::TEXT AS VARCHAR(255)) AS crnnumber_ext,
                TO_TIMESTAMP_TZ(data_payload:InvoiceDate_Ext::NUMBER/1000) AS invoicedate_ext,
                TO_TIMESTAMP_TZ(data_payload:PaymentDueDate_Ext::NUMBER/1000) AS paymentduedate_ext,
                CAST(data_payload:ExcessDatesOverrideReason_Ext::TEXT AS VARCHAR(255)) AS excessdatesoverridereason_ext,
                TO_TIMESTAMP_TZ(data_payload:EndDate_Ext::NUMBER/1000) AS enddate_ext,
                TO_TIMESTAMP_TZ(data_payload:StartDate_Ext::NUMBER/1000) AS startdate_ext,
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
            FROM {{ source('gwcc', 'cc_deductible') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                $1:coverageid::NUMBER AS coverageid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:paid::BOOLEAN AS paid,
                CAST($1:editreason::TEXT AS VARCHAR(255)) AS editreason,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:currency::NUMBER AS currency,
                CAST($1:invoicenumber_icare::TEXT AS VARCHAR(50)) AS invoicenumber_icare,
                CAST($1:accountnumber_icare::TEXT AS VARCHAR(50)) AS accountnumber_icare,
                $1:overridden::BOOLEAN AS overridden,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:waived::BOOLEAN AS waived,
                $1:claimid::NUMBER AS claimid,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                CAST($1:invoiceamountsenttobc AS NUMBER(18,2)) AS invoiceamountsenttobc,
                $1:id::NUMBER AS id,
                CAST($1:crnnumber_ext::TEXT AS VARCHAR(255)) AS crnnumber_ext,
                $1:invoicedate_ext::TIMESTAMP_TZ AS invoicedate_ext,
                $1:paymentduedate_ext::TIMESTAMP_TZ AS paymentduedate_ext,
                CAST($1:excessdatesoverridereason_ext::TEXT AS VARCHAR(255)) AS excessdatesoverridereason_ext,
                $1:enddate_ext::TIMESTAMP_TZ AS enddate_ext,
                $1:startdate_ext::TIMESTAMP_TZ AS startdate_ext,
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
            FROM {{ source('gwcc', 'cc_deductible') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS deductible_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'createuserid',
                        'coverageid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'paid',
                        'editreason',
                        'updateuserid',
                        'currency',
                        'invoicenumber_icare',
                        'accountnumber_icare',
                        'overridden',
                        'updatetime',
                        'waived',
                        'claimid',
                        'amount',
                        'invoiceamountsenttobc',
                        'crnnumber_ext',
                        'invoicedate_ext',
                        'paymentduedate_ext',
                        'excessdatesoverridereason_ext',
                        'enddate_ext',
                        'startdate_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

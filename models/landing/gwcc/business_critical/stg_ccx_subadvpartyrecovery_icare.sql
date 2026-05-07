{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_subadvpartyrecovery_icare.
                                                subadvpartyrecovery_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "ccx_subadvpartyrecovery_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:InvoiceAmount AS NUMBER(18,2)) AS invoiceamount,
                TO_TIMESTAMP_TZ(data_payload:RecoveryPeriodEnd::NUMBER/1000) AS recoveryperiodend,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:RecoveryType_icare::NUMBER AS recoverytype_icare,
                TO_TIMESTAMP_TZ(data_payload:RecoveryPeriodStart::NUMBER/1000) AS recoveryperiodstart,
                CAST(data_payload:WrittenOffAmount AS NUMBER(18,2)) AS writtenoffamount,
                data_payload:Waived::BOOLEAN AS waived,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:InvoiceCreated::BOOLEAN AS invoicecreated,
                TO_TIMESTAMP_TZ(data_payload:InvoiceDate::NUMBER/1000) AS invoicedate,
                TO_TIMESTAMP_TZ(data_payload:DueDate::NUMBER/1000) AS duedate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:InvoiceType::NUMBER AS invoicetype,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:PayeeInstructions_icare::TEXT AS VARCHAR(250)) AS payeeinstructions_icare,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:WriteOff::BOOLEAN AS writeoff,
                data_payload:SubroAdverseParty::NUMBER AS subroadverseparty,
                data_payload:Strategy::NUMBER AS strategy,
                CAST(data_payload:InvoiceNumber::TEXT AS VARCHAR(25)) AS invoicenumber,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(50)) AS accountnumber,
                CAST(data_payload:AmountReceived AS NUMBER(18,2)) AS amountreceived,
                TO_TIMESTAMP_TZ(data_payload:PaymentDueDate::NUMBER/1000) AS paymentduedate,
                CAST(data_payload:CRNNumber::TEXT AS VARCHAR(255)) AS crnnumber,
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
            FROM {{ source('gwcc', 'ccx_subadvpartyrecovery_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:invoiceamount AS NUMBER(18,2)) AS invoiceamount,
                $1:recoveryperiodend::TIMESTAMP_TZ AS recoveryperiodend,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:recoverytype_icare::NUMBER AS recoverytype_icare,
                $1:recoveryperiodstart::TIMESTAMP_TZ AS recoveryperiodstart,
                CAST($1:writtenoffamount AS NUMBER(18,2)) AS writtenoffamount,
                $1:waived::BOOLEAN AS waived,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:invoicecreated::BOOLEAN AS invoicecreated,
                $1:invoicedate::TIMESTAMP_TZ AS invoicedate,
                $1:duedate::TIMESTAMP_TZ AS duedate,
                $1:createuserid::NUMBER AS createuserid,
                $1:invoicetype::NUMBER AS invoicetype,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:payeeinstructions_icare::TEXT AS VARCHAR(250)) AS payeeinstructions_icare,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:writeoff::BOOLEAN AS writeoff,
                $1:subroadverseparty::NUMBER AS subroadverseparty,
                $1:strategy::NUMBER AS strategy,
                CAST($1:invoicenumber::TEXT AS VARCHAR(25)) AS invoicenumber,
                CAST($1:accountnumber::TEXT AS VARCHAR(50)) AS accountnumber,
                CAST($1:amountreceived AS NUMBER(18,2)) AS amountreceived,
                $1:paymentduedate::TIMESTAMP_TZ AS paymentduedate,
                CAST($1:crnnumber::TEXT AS VARCHAR(255)) AS crnnumber,
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
            FROM {{ source('gwcc', 'ccx_subadvpartyrecovery_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS subadvpartyrecovery_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'invoiceamount',
                        'recoveryperiodend',
                        'createtime',
                        'recoverytype_icare',
                        'recoveryperiodstart',
                        'writtenoffamount',
                        'waived',
                        'updatetime',
                        'invoicecreated',
                        'invoicedate',
                        'duedate',
                        'createuserid',
                        'invoicetype',
                        'beanversion',
                        'payeeinstructions_icare',
                        'archivepartition',
                        'retired',
                        'updateuserid',
                        'writeoff',
                        'subroadverseparty',
                        'strategy',
                        'invoicenumber',
                        'accountnumber',
                        'amountreceived',
                        'paymentduedate',
                        'crnnumber'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
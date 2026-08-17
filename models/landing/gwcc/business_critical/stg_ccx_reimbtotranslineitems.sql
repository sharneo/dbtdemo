{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_reimbtotranslineitems.
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
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:OverpaymentReimbursement_icare::NUMBER AS overpaymentreimbursement_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:TransactionLineItem::NUMBER AS transactionlineitem,
                CAST(data_payload:InvoiceAmount AS NUMBER(18,2)) AS invoiceamount,
                CAST(data_payload:WriteOffAmountAllocate AS NUMBER(18,2)) AS writeoffamountallocate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:ReimbursementAmount AS NUMBER(18,2)) AS reimbursementamount,
                CAST(data_payload:CreditAmount AS NUMBER(18,2)) AS creditamount,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Waived::BOOLEAN AS waived,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PaymentAmountAllocate AS NUMBER(18,2)) AS paymentamountallocate,
                CAST(data_payload:RecoveryWBRate AS NUMBER(18,2)) AS recoverywbrate,
                CAST(data_payload:RecoveryDeductions AS NUMBER(18,2)) AS recoverydeductions,
                CAST(data_payload:RecoveryHoursWorked AS NUMBER(6,2)) AS recoveryhoursworked,
                CAST(data_payload:RecoveryPAYG AS NUMBER(18,2)) AS recoverypayg,
                CAST(data_payload:RecoveryPIAWE AS NUMBER(18,2)) AS recoverypiawe,
                CAST(data_payload:RecoveryEarnings AS NUMBER(18,2)) AS recoveryearnings,
                TO_TIMESTAMP_TZ(data_payload:DateTo_Ext::NUMBER/1000) AS dateto_ext,
                data_payload:CountOfWeeks::NUMBER AS countofweeks,
                CAST(data_payload:RecoveryAWE_Ext AS NUMBER(18,2)) AS recoveryawe_ext,
                CAST(data_payload:RecoveryPercentageOfWeek_Ext AS NUMBER(5,2)) AS recoverypercentageofweek_ext,
                CAST(data_payload:RecoveryWeeklyActualRate_Ext AS NUMBER(18,2)) AS recoveryweeklyactualrate_ext,
                TO_TIMESTAMP_TZ(data_payload:DateFrom_Ext::NUMBER/1000) AS datefrom_ext,
                data_payload:AdjustmentFlag_Ext::BOOLEAN AS adjustmentflag_ext,
                CAST(data_payload:PreviousPaidAmount_Ext AS NUMBER(18,2)) AS previouspaidamount_ext,
                CAST(data_payload:GrossRecoveryAmountDue AS NUMBER(18,2)) AS grossrecoveryamountdue,
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
            FROM {{ source('gwcc', 'ccx_reimbtotranslineitems') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:overpaymentreimbursement_icare::NUMBER AS overpaymentreimbursement_icare,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:transactionlineitem::NUMBER AS transactionlineitem,
                CAST($1:invoiceamount AS NUMBER(18,2)) AS invoiceamount,
                CAST($1:writeoffamountallocate AS NUMBER(18,2)) AS writeoffamountallocate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:reimbursementamount AS NUMBER(18,2)) AS reimbursementamount,
                CAST($1:creditamount AS NUMBER(18,2)) AS creditamount,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:waived::BOOLEAN AS waived,
                $1:id::NUMBER AS id,
                CAST($1:paymentamountallocate AS NUMBER(18,2)) AS paymentamountallocate,
                CAST($1:recoverywbrate AS NUMBER(18,2)) AS recoverywbrate,
                CAST($1:recoverydeductions AS NUMBER(18,2)) AS recoverydeductions,
                CAST($1:recoveryhoursworked AS NUMBER(6,2)) AS recoveryhoursworked,
                CAST($1:recoverypayg AS NUMBER(18,2)) AS recoverypayg,
                CAST($1:recoverypiawe AS NUMBER(18,2)) AS recoverypiawe,
                CAST($1:recoveryearnings AS NUMBER(18,2)) AS recoveryearnings,
                $1:dateto_ext::TIMESTAMP_TZ AS dateto_ext,
                $1:countofweeks::NUMBER AS countofweeks,
                CAST($1:recoveryawe_ext AS NUMBER(18,2)) AS recoveryawe_ext,
                CAST($1:recoverypercentageofweek_ext AS NUMBER(5,2)) AS recoverypercentageofweek_ext,
                CAST($1:recoveryweeklyactualrate_ext AS NUMBER(18,2)) AS recoveryweeklyactualrate_ext,
                $1:datefrom_ext::TIMESTAMP_TZ AS datefrom_ext,
                $1:adjustmentflag_ext::BOOLEAN AS adjustmentflag_ext,
                CAST($1:previouspaidamount_ext AS NUMBER(18,2)) AS previouspaidamount_ext,
                CAST($1:grossrecoveryamountdue AS NUMBER(18,2)) AS grossrecoveryamountdue,
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
            FROM {{ source('gwcc', 'ccx_reimbtotranslineitems') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS reimbtotranslineitems_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'overpaymentreimbursement_icare',
                        'createuserid',
                        'publicid',
                        'transactionlineitem',
                        'invoiceamount',
                        'writeoffamountallocate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'createtime',
                        'updateuserid',
                        'reimbursementamount',
                        'creditamount',
                        'updatetime',
                        'waived',
                        'paymentamountallocate',
                        'recoverywbrate',
                        'recoverydeductions',
                        'recoveryhoursworked',
                        'recoverypayg',
                        'recoverypiawe',
                        'recoveryearnings',
                        'dateto_ext',
                        'countofweeks',
                        'recoveryawe_ext',
                        'recoverypercentageofweek_ext',
                        'recoveryweeklyactualrate_ext',
                        'datefrom_ext',
                        'adjustmentflag_ext',
                        'previouspaidamount_ext',
                        'grossrecoveryamountdue'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_writeoff.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwbc", "billing_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:Reversed::BOOLEAN AS reversed,
                data_payload:TAccountContainerID::NUMBER AS taccountcontainerid,
                data_payload:Reason::NUMBER AS reason,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:ExecutionDate::NUMBER/1000) AS executiondate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                data_payload:ItemCommissionID::NUMBER AS itemcommissionid,
                data_payload:RequestingUserID::NUMBER AS requestinguserid,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                CAST(data_payload:ReversedAmount AS NUMBER(18,2)) AS reversedamount,
                data_payload:InvoiceItemID::NUMBER AS invoiceitemid,
                data_payload:ReversedAmount_cur::NUMBER AS reversedamount_cur,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ProducerID::NUMBER AS producerid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ApprovalDate::NUMBER/1000) AS approvaldate,
                data_payload:ApprovalStatus::NUMBER AS approvalstatus,
                data_payload:ChargePatternID::NUMBER AS chargepatternid,
                data_payload:ChargeCommissionID::NUMBER AS chargecommissionid,
                data_payload:GrossAgencyPmntItemID::NUMBER AS grossagencypmntitemid,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:CmsnAgencyPmntItemID::NUMBER AS cmsnagencypmntitemid,
                data_payload:RecoveredAmount_icare_cur::NUMBER AS recoveredamount_icare_cur,
                CAST(data_payload:ReversalRefID_icare::TEXT AS VARCHAR(255)) AS reversalrefid_icare,
                CAST(data_payload:RecoveredAmount_icare_amt AS NUMBER(18,2)) AS recoveredamount_icare_amt,
                data_payload:SequenceNum_icare::NUMBER AS sequencenum_icare,
                data_payload:IsWaiverApplicable_icare::BOOLEAN AS iswaiverapplicable_icare,
                CAST(data_payload:OriginalRefId_icare::TEXT AS VARCHAR(255)) AS originalrefid_icare,
                data_payload:WaiverPercentage_icare::NUMBER AS waiverpercentage_icare,
                CAST(data_payload:RCInvoiceNumber_icare::TEXT AS VARCHAR(255)) AS rcinvoicenumber_icare,
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
                'GWBC' as source_system
            FROM {{ source('gwbc', 'bc_writeoff') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:reversed::BOOLEAN AS reversed,
                $1:taccountcontainerid::NUMBER AS taccountcontainerid,
                $1:reason::NUMBER AS reason,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:executiondate::TIMESTAMP_TZ AS executiondate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:currency::NUMBER AS currency,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:itemcommissionid::NUMBER AS itemcommissionid,
                $1:requestinguserid::NUMBER AS requestinguserid,
                $1:amount_cur::NUMBER AS amount_cur,
                CAST($1:reversedamount AS NUMBER(18,2)) AS reversedamount,
                $1:invoiceitemid::NUMBER AS invoiceitemid,
                $1:reversedamount_cur::NUMBER AS reversedamount_cur,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:producerid::NUMBER AS producerid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:approvaldate::TIMESTAMP_TZ AS approvaldate,
                $1:approvalstatus::NUMBER AS approvalstatus,
                $1:chargepatternid::NUMBER AS chargepatternid,
                $1:chargecommissionid::NUMBER AS chargecommissionid,
                $1:grossagencypmntitemid::NUMBER AS grossagencypmntitemid,
                $1:subtype::NUMBER AS subtype,
                $1:cmsnagencypmntitemid::NUMBER AS cmsnagencypmntitemid,
                $1:recoveredamount_icare_cur::NUMBER AS recoveredamount_icare_cur,
                CAST($1:reversalrefid_icare::TEXT AS VARCHAR(255)) AS reversalrefid_icare,
                CAST($1:recoveredamount_icare_amt AS NUMBER(18,2)) AS recoveredamount_icare_amt,
                $1:sequencenum_icare::NUMBER AS sequencenum_icare,
                $1:iswaiverapplicable_icare::BOOLEAN AS iswaiverapplicable_icare,
                CAST($1:originalrefid_icare::TEXT AS VARCHAR(255)) AS originalrefid_icare,
                $1:waiverpercentage_icare::NUMBER AS waiverpercentage_icare,
                CAST($1:rcinvoicenumber_icare::TEXT AS VARCHAR(255)) AS rcinvoicenumber_icare,
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
                'GWBC' as source_system
            FROM {{ source('gwbc', 'bc_writeoff') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS writeoff_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'reversed',
                        'taccountcontainerid',
                        'reason',
                        'publicid',
                        'executiondate',
                        'createtime',
                        'currency',
                        'updatetime',
                        'amount',
                        'itemcommissionid',
                        'requestinguserid',
                        'amount_cur',
                        'reversedamount',
                        'invoiceitemid',
                        'reversedamount_cur',
                        'createuserid',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'producerid',
                        'updateuserid',
                        'approvaldate',
                        'approvalstatus',
                        'chargepatternid',
                        'chargecommissionid',
                        'grossagencypmntitemid',
                        'subtype',
                        'cmsnagencypmntitemid',
                        'recoveredamount_icare_cur',
                        'reversalrefid_icare',
                        'recoveredamount_icare_amt',
                        'sequencenum_icare',
                        'iswaiverapplicable_icare',
                        'originalrefid_icare',
                        'waiverpercentage_icare',
                        'rcinvoicenumber_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_transactionset.
                                                transactionset_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_transactionset"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:VoidTransactionReason_icare::NUMBER AS voidtransactionreason_icare,
                data_payload:Retired::NUMBER AS retired,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                CAST(data_payload:TransferFromClaimNumber_icare::TEXT AS VARCHAR(40)) AS transferfromclaimnumber_icare,
                data_payload:RecurrenceID::NUMBER AS recurrenceid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ApprovalDate::NUMBER/1000) AS approvaldate,
                data_payload:AdjustmentPayment_icare::BOOLEAN AS adjustmentpayment_icare,
                data_payload:ApprovalStatus::NUMBER AS approvalstatus,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ManualTransactionReason_icare::NUMBER AS manualtransactionreason_icare,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:RequestingUserID::NUMBER AS requestinguserid,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:TransferToClaimNumber_icare::TEXT AS VARCHAR(40)) AS transfertoclaimnumber_icare,
                data_payload:ID::NUMBER AS id,
                data_payload:PaymentType_icare::NUMBER AS paymenttype_icare,
                CAST(data_payload:ManagingEntityName_icare::TEXT AS VARCHAR(255)) AS managingentityname_icare,
                data_payload:InvoiceType_Ext::NUMBER AS invoicetype_ext,
                data_payload:ExcessRefundPayment_Ext::BOOLEAN AS excessrefundpayment_ext,
                data_payload:isSavedCheck_Ext::BOOLEAN AS issavedcheck_ext,
                CAST(data_payload:WeeklyBenefitRationale_Ext::TEXT AS VARCHAR(255)) AS weeklybenefitrationale_ext,
                data_payload:SportingTransactionReason_Ext::NUMBER AS sportingtransactionreason_ext,
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
            FROM {{ source('gwcc', 'cc_transactionset') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:voidtransactionreason_icare::NUMBER AS voidtransactionreason_icare,
                $1:retired::NUMBER AS retired,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                CAST($1:transferfromclaimnumber_icare::TEXT AS VARCHAR(40)) AS transferfromclaimnumber_icare,
                $1:recurrenceid::NUMBER AS recurrenceid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:approvaldate::TIMESTAMP_TZ AS approvaldate,
                $1:adjustmentpayment_icare::BOOLEAN AS adjustmentpayment_icare,
                $1:approvalstatus::NUMBER AS approvalstatus,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:manualtransactionreason_icare::NUMBER AS manualtransactionreason_icare,
                $1:claimid::NUMBER AS claimid,
                $1:requestinguserid::NUMBER AS requestinguserid,
                $1:subtype::NUMBER AS subtype,
                CAST($1:transfertoclaimnumber_icare::TEXT AS VARCHAR(40)) AS transfertoclaimnumber_icare,
                $1:id::NUMBER AS id,
                $1:paymenttype_icare::NUMBER AS paymenttype_icare,
                CAST($1:managingentityname_icare::TEXT AS VARCHAR(255)) AS managingentityname_icare,
                $1:invoicetype_ext::NUMBER AS invoicetype_ext,
                $1:excessrefundpayment_ext::BOOLEAN AS excessrefundpayment_ext,
                $1:issavedcheck_ext::BOOLEAN AS issavedcheck_ext,
                CAST($1:weeklybenefitrationale_ext::TEXT AS VARCHAR(255)) AS weeklybenefitrationale_ext,
                $1:sportingtransactionreason_ext::NUMBER AS sportingtransactionreason_ext,
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
            FROM {{ source('gwcc', 'cc_transactionset') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS transactionset_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'voidtransactionreason_icare',
                        'retired',
                        'documentlinkableid',
                        'transferfromclaimnumber_icare',
                        'recurrenceid',
                        'updateuserid',
                        'approvaldate',
                        'adjustmentpayment_icare',
                        'approvalstatus',
                        'updatetime',
                        'manualtransactionreason_icare',
                        'claimid',
                        'requestinguserid',
                        'subtype',
                        'transfertoclaimnumber_icare',
                        'paymenttype_icare',
                        'managingentityname_icare',
                        'invoicetype_ext',
                        'excessrefundpayment_ext',
                        'issavedcheck_ext',
                        'weeklybenefitrationale_ext',
                        'sportingtransactionreason_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_paygcheck_icare.
                                                paygcheck_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_paygcheck_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:PaymentType::NUMBER AS paymenttype,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:TotalLumpSumEAmount AS NUMBER(18,2)) AS totallumpsumeamount,
                data_payload:PayeeType::NUMBER AS payeetype,
                data_payload:CheckType::NUMBER AS checktype,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(128)) AS claimnumber,
                TO_TIMESTAMP_TZ(data_payload:IssueDate::NUMBER/1000) AS issuedate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:TotalLumpSumENetAmount AS NUMBER(18,2)) AS totallumpsumenetamount,
                CAST(data_payload:VoidTransactionReason::TEXT AS VARCHAR(255)) AS voidtransactionreason,
                CAST(data_payload:CheckNumber::TEXT AS VARCHAR(255)) AS checknumber,
                CAST(data_payload:PayeeName::TEXT AS VARCHAR(128)) AS payeename,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:IsManualCheck::BOOLEAN AS ismanualcheck,
                data_payload:PAYGSnapShot_icareID::NUMBER AS paygsnapshot_icareid,
                TO_TIMESTAMP_TZ(data_payload:LastPostingDate::NUMBER/1000) AS lastpostingdate,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ManualTransactionReason::TEXT AS VARCHAR(255)) AS manualtransactionreason,
                CAST(data_payload:PAYGAmountWitheld AS NUMBER(18,2)) AS paygamountwitheld,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:PreviousCheckType::NUMBER AS previouschecktype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:TransactionAmount AS NUMBER(18,2)) AS transactionamount,
                data_payload:CheckStatus::NUMBER AS checkstatus,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:TFN::TEXT AS VARCHAR(30)) AS tfn,
                TO_TIMESTAMP_TZ(data_payload:CAMTVoidDate::NUMBER/1000) AS camtvoiddate,
                data_payload:IsOverpayment::BOOLEAN AS isoverpayment,
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
            FROM {{ source('gwcc', 'ccx_paygcheck_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:paymenttype::NUMBER AS paymenttype,
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:totallumpsumeamount AS NUMBER(18,2)) AS totallumpsumeamount,
                $1:payeetype::NUMBER AS payeetype,
                $1:checktype::NUMBER AS checktype,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:claimnumber::TEXT AS VARCHAR(128)) AS claimnumber,
                $1:issuedate::TIMESTAMP_TZ AS issuedate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:totallumpsumenetamount AS NUMBER(18,2)) AS totallumpsumenetamount,
                CAST($1:voidtransactionreason::TEXT AS VARCHAR(255)) AS voidtransactionreason,
                CAST($1:checknumber::TEXT AS VARCHAR(255)) AS checknumber,
                CAST($1:payeename::TEXT AS VARCHAR(128)) AS payeename,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:ismanualcheck::BOOLEAN AS ismanualcheck,
                $1:paygsnapshot_icareid::NUMBER AS paygsnapshot_icareid,
                $1:lastpostingdate::TIMESTAMP_TZ AS lastpostingdate,
                $1:id::NUMBER AS id,
                CAST($1:manualtransactionreason::TEXT AS VARCHAR(255)) AS manualtransactionreason,
                CAST($1:paygamountwitheld AS NUMBER(18,2)) AS paygamountwitheld,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:previouschecktype::NUMBER AS previouschecktype,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:transactionamount AS NUMBER(18,2)) AS transactionamount,
                $1:checkstatus::NUMBER AS checkstatus,
                $1:subtype::NUMBER AS subtype,
                CAST($1:tfn::TEXT AS VARCHAR(30)) AS tfn,
                $1:camtvoiddate::TIMESTAMP_TZ AS camtvoiddate,
                $1:isoverpayment::BOOLEAN AS isoverpayment,
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
            FROM {{ source('gwcc', 'ccx_paygcheck_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS paygcheck_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'paymenttype',
                        'loadcommandid',
                        'totallumpsumeamount',
                        'payeetype',
                        'checktype',
                        'publicid',
                        'claimnumber',
                        'issuedate',
                        'createtime',
                        'totallumpsumenetamount',
                        'voidtransactionreason',
                        'checknumber',
                        'payeename',
                        'updatetime',
                        'ismanualcheck',
                        'paygsnapshot_icareid',
                        'lastpostingdate',
                        'manualtransactionreason',
                        'paygamountwitheld',
                        'createuserid',
                        'beanversion',
                        'retired',
                        'previouschecktype',
                        'updateuserid',
                        'transactionamount',
                        'checkstatus',
                        'subtype',
                        'tfn',
                        'camtvoiddate',
                        'isoverpayment'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
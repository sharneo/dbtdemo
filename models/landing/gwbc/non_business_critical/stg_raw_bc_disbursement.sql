{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_disbursement.
                                                disbursement_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwbc", "billing_centre", "non_business_critical", "bc_disbursement"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:AgencyCyclePaymentID::NUMBER AS agencycyclepaymentid,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:InternalComment::TEXT AS VARCHAR(255)) AS internalcomment,
                TO_TIMESTAMP_TZ(data_payload:ChequePresentedDate_icare::NUMBER/1000) AS chequepresenteddate_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Reason::NUMBER AS reason,
                TO_TIMESTAMP_TZ(data_payload:BankBookingDate_icare::NUMBER/1000) AS bankbookingdate_icare,
                data_payload:AccountID::NUMBER AS accountid,
                data_payload:PaymentInstrumentID::NUMBER AS paymentinstrumentid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Currency::NUMBER AS currency,
                data_payload:CollateralID::NUMBER AS collateralid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                data_payload:VoidReason::NUMBER AS voidreason,
                CAST(data_payload:RefNumberDenorm::TEXT AS VARCHAR(255)) AS refnumberdenorm,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                data_payload:RequestingUserID::NUMBER AS requestinguserid,
                data_payload:ID::NUMBER AS id,
                data_payload:ReturnToSender_icare::BOOLEAN AS returntosender_icare,
                CAST(data_payload:RefNumber::TEXT AS VARCHAR(255)) AS refnumber,
                CAST(data_payload:PayToDenorm::TEXT AS VARCHAR(255)) AS paytodenorm,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:DueDate::NUMBER/1000) AS duedate,
                CAST(data_payload:PayTo::TEXT AS VARCHAR(255)) AS payto,
                CAST(data_payload:BankRefDetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                data_payload:UnappliedFundID::NUMBER AS unappliedfundid,
                CAST(data_payload:ChequeNumber_icare::TEXT AS VARCHAR(255)) AS chequenumber_icare,
                TO_TIMESTAMP_TZ(data_payload:CloseDate::NUMBER/1000) AS closedate,
                data_payload:SuspensePaymentID::NUMBER AS suspensepaymentid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:DisbursementNumber::TEXT AS VARCHAR(255)) AS disbursementnumber,
                data_payload:ReportingGroupID::NUMBER AS reportinggroupid,
                data_payload:ProducerID::NUMBER AS producerid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ApprovalDate::NUMBER/1000) AS approvaldate,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:MailTo::TEXT AS VARCHAR(255)) AS mailto,
                CAST(data_payload:Address::TEXT AS VARCHAR(1333)) AS address,
                data_payload:ApprovalStatus::NUMBER AS approvalstatus,
                CAST(data_payload:Memo::TEXT AS VARCHAR(255)) AS memo,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:mailTo_icare::NUMBER AS mailto_icare,
                data_payload:IsCOR_icare::BOOLEAN AS iscor_icare,
                CAST(data_payload:UserInputRef_icare::TEXT AS VARCHAR(255)) AS userinputref_icare,
                data_payload:AwaitingEFTDetails_Ext::NUMBER AS awaitingeftdetails_ext,
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
            FROM {{ source('gwbc', 'bc_disbursement') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:agencycyclepaymentid::NUMBER AS agencycyclepaymentid,
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:internalcomment::TEXT AS VARCHAR(255)) AS internalcomment,
                $1:chequepresenteddate_icare::TIMESTAMP_TZ AS chequepresenteddate_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:reason::NUMBER AS reason,
                $1:bankbookingdate_icare::TIMESTAMP_TZ AS bankbookingdate_icare,
                $1:accountid::NUMBER AS accountid,
                $1:paymentinstrumentid::NUMBER AS paymentinstrumentid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:currency::NUMBER AS currency,
                $1:collateralid::NUMBER AS collateralid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:voidreason::NUMBER AS voidreason,
                CAST($1:refnumberdenorm::TEXT AS VARCHAR(255)) AS refnumberdenorm,
                $1:amount_cur::NUMBER AS amount_cur,
                $1:requestinguserid::NUMBER AS requestinguserid,
                $1:id::NUMBER AS id,
                $1:returntosender_icare::BOOLEAN AS returntosender_icare,
                CAST($1:refnumber::TEXT AS VARCHAR(255)) AS refnumber,
                CAST($1:paytodenorm::TEXT AS VARCHAR(255)) AS paytodenorm,
                $1:createuserid::NUMBER AS createuserid,
                $1:duedate::TIMESTAMP_TZ AS duedate,
                CAST($1:payto::TEXT AS VARCHAR(255)) AS payto,
                CAST($1:bankrefdetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                $1:unappliedfundid::NUMBER AS unappliedfundid,
                CAST($1:chequenumber_icare::TEXT AS VARCHAR(255)) AS chequenumber_icare,
                $1:closedate::TIMESTAMP_TZ AS closedate,
                $1:suspensepaymentid::NUMBER AS suspensepaymentid,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                CAST($1:disbursementnumber::TEXT AS VARCHAR(255)) AS disbursementnumber,
                $1:reportinggroupid::NUMBER AS reportinggroupid,
                $1:producerid::NUMBER AS producerid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:approvaldate::TIMESTAMP_TZ AS approvaldate,
                $1:status::NUMBER AS status,
                CAST($1:mailto::TEXT AS VARCHAR(255)) AS mailto,
                CAST($1:address::TEXT AS VARCHAR(1333)) AS address,
                $1:approvalstatus::NUMBER AS approvalstatus,
                CAST($1:memo::TEXT AS VARCHAR(255)) AS memo,
                $1:subtype::NUMBER AS subtype,
                $1:mailto_icare::NUMBER AS mailto_icare,
                $1:iscor_icare::BOOLEAN AS iscor_icare,
                CAST($1:userinputref_icare::TEXT AS VARCHAR(255)) AS userinputref_icare,
                $1:awaitingeftdetails_ext::NUMBER AS awaitingeftdetails_ext,
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
            FROM {{ source('gwbc', 'bc_disbursement') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS disbursement_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'agencycyclepaymentid',
                        'loadcommandid',
                        'internalcomment',
                        'chequepresenteddate_icare',
                        'publicid',
                        'reason',
                        'bankbookingdate_icare',
                        'accountid',
                        'paymentinstrumentid',
                        'createtime',
                        'currency',
                        'collateralid',
                        'updatetime',
                        'amount',
                        'voidreason',
                        'refnumberdenorm',
                        'amount_cur',
                        'requestinguserid',
                        'returntosender_icare',
                        'refnumber',
                        'paytodenorm',
                        'createuserid',
                        'duedate',
                        'payto',
                        'bankrefdetail_icare',
                        'unappliedfundid',
                        'chequenumber_icare',
                        'closedate',
                        'suspensepaymentid',
                        'beanversion',
                        'retired',
                        'disbursementnumber',
                        'reportinggroupid',
                        'producerid',
                        'updateuserid',
                        'approvaldate',
                        'status',
                        'mailto',
                        'address',
                        'approvalstatus',
                        'memo',
                        'subtype',
                        'mailto_icare',
                        'iscor_icare',
                        'userinputref_icare',
                        'awaitingeftdetails_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
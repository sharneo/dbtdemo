{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_suspensepayment.
                                                suspensepayment_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bc_suspensepayment"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:PolicyPeriodAppliedToID::NUMBER AS policyperiodappliedtoid,
                data_payload:ProducerAppliedToID::NUMBER AS producerappliedtoid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:AccountAppliedToID::NUMBER AS accountappliedtoid,
                data_payload:PaymentInstrumentID::NUMBER AS paymentinstrumentid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:OfferOption::TEXT AS VARCHAR(255)) AS offeroption,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:RefNumber_icare::TEXT AS VARCHAR(255)) AS refnumber_icare,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                CAST(data_payload:RefNumberDenorm::TEXT AS VARCHAR(255)) AS refnumberdenorm,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                data_payload:HiddenTAccountContainerID::NUMBER AS hiddentaccountcontainerid,
                CAST(data_payload:ProducerNameDenorm::TEXT AS VARCHAR(255)) AS producernamedenorm,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:RefNumber::TEXT AS VARCHAR(255)) AS refnumber,
                CAST(data_payload:ProducerName::TEXT AS VARCHAR(255)) AS producername,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:BankRefDetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                CAST(data_payload:OfferNumber::TEXT AS VARCHAR(255)) AS offernumber,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ReportingGroupID::NUMBER AS reportinggroupid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:OriginalBankRefDetail_icare::TEXT AS VARCHAR(255)) AS originalbankrefdetail_icare,
                CAST(data_payload:AccountNumberDenorm::TEXT AS VARCHAR(255)) AS accountnumberdenorm,
                TO_TIMESTAMP_TZ(data_payload:PaymentDate::NUMBER/1000) AS paymentdate,
                CAST(data_payload:InvoiceNumber::TEXT AS VARCHAR(255)) AS invoicenumber,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(255)) AS accountnumber,
                data_payload:PaymentMoneyReceivedID::NUMBER AS paymentmoneyreceivedid,
                CAST(data_payload:PolicyNumberDenorm::TEXT AS VARCHAR(255)) AS policynumberdenorm,
                data_payload:AppliedByUserID::NUMBER AS appliedbyuserid,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(255)) AS policynumber,
                CAST(data_payload:Description::TEXT AS VARCHAR(1333)) AS description,
                data_payload:ReversedByUserID::NUMBER AS reversedbyuserid,
                data_payload:OfferingType_icare::NUMBER AS offeringtype_icare,
                CAST(data_payload:CreatedFromDBMR_icare::TEXT AS VARCHAR(255)) AS createdfromdbmr_icare,
                data_payload:LOBAccountTypeCode::NUMBER AS lobaccounttypecode,
                CAST(data_payload:UserInputRef_icare::TEXT AS VARCHAR(255)) AS userinputref_icare,
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
            FROM {{ source('gwbc', 'bc_suspensepayment') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:policyperiodappliedtoid::NUMBER AS policyperiodappliedtoid,
                $1:producerappliedtoid::NUMBER AS producerappliedtoid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:accountappliedtoid::NUMBER AS accountappliedtoid,
                $1:paymentinstrumentid::NUMBER AS paymentinstrumentid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:offeroption::TEXT AS VARCHAR(255)) AS offeroption,
                $1:currency::NUMBER AS currency,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:refnumber_icare::TEXT AS VARCHAR(255)) AS refnumber_icare,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                CAST($1:refnumberdenorm::TEXT AS VARCHAR(255)) AS refnumberdenorm,
                $1:amount_cur::NUMBER AS amount_cur,
                $1:hiddentaccountcontainerid::NUMBER AS hiddentaccountcontainerid,
                CAST($1:producernamedenorm::TEXT AS VARCHAR(255)) AS producernamedenorm,
                $1:id::NUMBER AS id,
                CAST($1:refnumber::TEXT AS VARCHAR(255)) AS refnumber,
                CAST($1:producername::TEXT AS VARCHAR(255)) AS producername,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:bankrefdetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                CAST($1:offernumber::TEXT AS VARCHAR(255)) AS offernumber,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:reportinggroupid::NUMBER AS reportinggroupid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:status::NUMBER AS status,
                CAST($1:originalbankrefdetail_icare::TEXT AS VARCHAR(255)) AS originalbankrefdetail_icare,
                CAST($1:accountnumberdenorm::TEXT AS VARCHAR(255)) AS accountnumberdenorm,
                $1:paymentdate::TIMESTAMP_TZ AS paymentdate,
                CAST($1:invoicenumber::TEXT AS VARCHAR(255)) AS invoicenumber,
                CAST($1:accountnumber::TEXT AS VARCHAR(255)) AS accountnumber,
                $1:paymentmoneyreceivedid::NUMBER AS paymentmoneyreceivedid,
                CAST($1:policynumberdenorm::TEXT AS VARCHAR(255)) AS policynumberdenorm,
                $1:appliedbyuserid::NUMBER AS appliedbyuserid,
                CAST($1:policynumber::TEXT AS VARCHAR(255)) AS policynumber,
                CAST($1:description::TEXT AS VARCHAR(1333)) AS description,
                $1:reversedbyuserid::NUMBER AS reversedbyuserid,
                $1:offeringtype_icare::NUMBER AS offeringtype_icare,
                CAST($1:createdfromdbmr_icare::TEXT AS VARCHAR(255)) AS createdfromdbmr_icare,
                $1:lobaccounttypecode::NUMBER AS lobaccounttypecode,
                CAST($1:userinputref_icare::TEXT AS VARCHAR(255)) AS userinputref_icare,
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
            FROM {{ source('gwbc', 'bc_suspensepayment') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS suspensepayment_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'policyperiodappliedtoid',
                        'producerappliedtoid',
                        'publicid',
                        'accountappliedtoid',
                        'paymentinstrumentid',
                        'createtime',
                        'offeroption',
                        'currency',
                        'updatetime',
                        'refnumber_icare',
                        'amount',
                        'refnumberdenorm',
                        'amount_cur',
                        'hiddentaccountcontainerid',
                        'producernamedenorm',
                        'refnumber',
                        'producername',
                        'createuserid',
                        'bankrefdetail_icare',
                        'offernumber',
                        'beanversion',
                        'retired',
                        'reportinggroupid',
                        'updateuserid',
                        'status',
                        'originalbankrefdetail_icare',
                        'accountnumberdenorm',
                        'paymentdate',
                        'invoicenumber',
                        'accountnumber',
                        'paymentmoneyreceivedid',
                        'policynumberdenorm',
                        'appliedbyuserid',
                        'policynumber',
                        'description',
                        'reversedbyuserid',
                        'offeringtype_icare',
                        'createdfromdbmr_icare',
                        'lobaccounttypecode',
                        'userinputref_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_basemoneyreceived.
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:PromisingProducerID::NUMBER AS promisingproducerid,
                CAST(data_payload:CreatedFromSuspense_icare::TEXT AS VARCHAR(255)) AS createdfromsuspense_icare,
                data_payload:InvoiceID::NUMBER AS invoiceid,
                data_payload:AccountID::NUMBER AS accountid,
                data_payload:PaymentInstrumentID::NUMBER AS paymentinstrumentid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:Name::TEXT AS VARCHAR(255)) AS name,
                data_payload:PolicyPeriodID::NUMBER AS policyperiodid,
                data_payload:CreatedFromPR_icare::NUMBER AS createdfrompr_icare,
                TO_TIMESTAMP_TZ(data_payload:ReceivedDate::NUMBER/1000) AS receiveddate,
                data_payload:Currency::NUMBER AS currency,
                data_payload:BaseDistID::NUMBER AS basedistid,
                data_payload:ReversalReason::NUMBER AS reversalreason,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                CAST(data_payload:RefNumberDenorm::TEXT AS VARCHAR(255)) AS refnumberdenorm,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:RefNumber::TEXT AS VARCHAR(255)) AS refnumber,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:BankRefDetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                data_payload:UnappliedFundID::NUMBER AS unappliedfundid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:AppliedDate::NUMBER/1000) AS applieddate,
                data_payload:ReportingGroupID::NUMBER AS reportinggroupid,
                data_payload:ProducerID::NUMBER AS producerid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:ReconcilationRef_icare::TEXT AS VARCHAR(255)) AS reconcilationref_icare,
                CAST(data_payload:InvoiceNumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                data_payload:Subtype::NUMBER AS subtype,
                TO_TIMESTAMP_TZ(data_payload:ReversalDate::NUMBER/1000) AS reversaldate,
                CAST(data_payload:Description::TEXT AS VARCHAR(1333)) AS description,
                CAST(data_payload:RevBankRefDetail_icare::TEXT AS VARCHAR(255)) AS revbankrefdetail_icare,
                TO_TIMESTAMP_TZ(data_payload:ReversalDate_icare::NUMBER/1000) AS reversaldate_icare,
                data_payload:RCPaymentArrangement_icare::NUMBER AS rcpaymentarrangement_icare,
                CAST(data_payload:UserInputRef_icare::TEXT AS VARCHAR(255)) AS userinputref_icare,
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
            FROM {{ source('gwbc', 'bc_basemoneyreceived') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:promisingproducerid::NUMBER AS promisingproducerid,
                CAST($1:createdfromsuspense_icare::TEXT AS VARCHAR(255)) AS createdfromsuspense_icare,
                $1:invoiceid::NUMBER AS invoiceid,
                $1:accountid::NUMBER AS accountid,
                $1:paymentinstrumentid::NUMBER AS paymentinstrumentid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:name::TEXT AS VARCHAR(255)) AS name,
                $1:policyperiodid::NUMBER AS policyperiodid,
                $1:createdfrompr_icare::NUMBER AS createdfrompr_icare,
                $1:receiveddate::TIMESTAMP_TZ AS receiveddate,
                $1:currency::NUMBER AS currency,
                $1:basedistid::NUMBER AS basedistid,
                $1:reversalreason::NUMBER AS reversalreason,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:amount_cur::NUMBER AS amount_cur,
                CAST($1:refnumberdenorm::TEXT AS VARCHAR(255)) AS refnumberdenorm,
                $1:id::NUMBER AS id,
                CAST($1:refnumber::TEXT AS VARCHAR(255)) AS refnumber,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:bankrefdetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                $1:unappliedfundid::NUMBER AS unappliedfundid,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:applieddate::TIMESTAMP_TZ AS applieddate,
                $1:reportinggroupid::NUMBER AS reportinggroupid,
                $1:producerid::NUMBER AS producerid,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:reconcilationref_icare::TEXT AS VARCHAR(255)) AS reconcilationref_icare,
                CAST($1:invoicenumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                $1:subtype::NUMBER AS subtype,
                $1:reversaldate::TIMESTAMP_TZ AS reversaldate,
                CAST($1:description::TEXT AS VARCHAR(1333)) AS description,
                CAST($1:revbankrefdetail_icare::TEXT AS VARCHAR(255)) AS revbankrefdetail_icare,
                $1:reversaldate_icare::TIMESTAMP_TZ AS reversaldate_icare,
                $1:rcpaymentarrangement_icare::NUMBER AS rcpaymentarrangement_icare,
                CAST($1:userinputref_icare::TEXT AS VARCHAR(255)) AS userinputref_icare,
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
            FROM {{ source('gwbc', 'bc_basemoneyreceived') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS basemoneyreceived_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'promisingproducerid',
                        'createdfromsuspense_icare',
                        'invoiceid',
                        'accountid',
                        'paymentinstrumentid',
                        'createtime',
                        'name',
                        'policyperiodid',
                        'createdfrompr_icare',
                        'receiveddate',
                        'currency',
                        'basedistid',
                        'reversalreason',
                        'updatetime',
                        'amount',
                        'amount_cur',
                        'refnumberdenorm',
                        'refnumber',
                        'createuserid',
                        'bankrefdetail_icare',
                        'unappliedfundid',
                        'beanversion',
                        'retired',
                        'applieddate',
                        'reportinggroupid',
                        'producerid',
                        'updateuserid',
                        'reconcilationref_icare',
                        'invoicenumber_icare',
                        'subtype',
                        'reversaldate',
                        'description',
                        'revbankrefdetail_icare',
                        'reversaldate_icare',
                        'rcpaymentarrangement_icare',
                        'userinputref_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

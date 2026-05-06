{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_paymentinstrument.
                                                paymentinstrument_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_billing_centre", "billing_centre", "non_business_critical", "bc_paymentinstrument"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Immutable::BOOLEAN AS immutable,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ChequeNumber_icare::TEXT AS VARCHAR(255)) AS chequenumber_icare,
                data_payload:PaymentMethod::NUMBER AS paymentmethod,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:AccountID::NUMBER AS accountid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:IsActive_icare::BOOLEAN AS isactive_icare,
                data_payload:ProducerID::NUMBER AS producerid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Detail::TEXT AS VARCHAR(255)) AS detail,
                CAST(data_payload:BankAccountNumber_icare::TEXT AS VARCHAR(255)) AS bankaccountnumber_icare,
                CAST(data_payload:BSBNumber_icare::TEXT AS VARCHAR(255)) AS bsbnumber_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Token::TEXT AS VARCHAR(255)) AS token,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                CAST(data_payload:AccountHolderName_icare::TEXT AS VARCHAR(255)) AS accountholdername_icare,
                CAST(data_payload:CardHolderName_icare::TEXT AS VARCHAR(255)) AS cardholdername_icare,
                data_payload:CreditCardIssuer::NUMBER AS creditcardissuer,
                CAST(data_payload:CardNumber_icare::TEXT AS VARCHAR(255)) AS cardnumber_icare,
                data_payload:BankAccountType_icare::NUMBER AS bankaccounttype_icare,
                CAST(data_payload:ExpiryMonth_icare::TEXT AS VARCHAR(255)) AS expirymonth_icare,
                CAST(data_payload:ExpiryYear_icare::TEXT AS VARCHAR(255)) AS expiryyear_icare,
                CAST(data_payload:TokenNumber_icare::TEXT AS VARCHAR(255)) AS tokennumber_icare,
                CAST(data_payload:CAMTBankAccNumber_icare::TEXT AS VARCHAR(255)) AS camtbankaccnumber_icare,
                data_payload:RNSWPayment_Ext::BOOLEAN AS rnswpayment_ext,
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
            FROM {{ source('gwbc', 'bc_paymentinstrument') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:immutable::BOOLEAN AS immutable,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:chequenumber_icare::TEXT AS VARCHAR(255)) AS chequenumber_icare,
                $1:paymentmethod::NUMBER AS paymentmethod,
                $1:beanversion::NUMBER AS beanversion,
                $1:accountid::NUMBER AS accountid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:isactive_icare::BOOLEAN AS isactive_icare,
                $1:producerid::NUMBER AS producerid,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:detail::TEXT AS VARCHAR(255)) AS detail,
                CAST($1:bankaccountnumber_icare::TEXT AS VARCHAR(255)) AS bankaccountnumber_icare,
                CAST($1:bsbnumber_icare::TEXT AS VARCHAR(255)) AS bsbnumber_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:token::TEXT AS VARCHAR(255)) AS token,
                $1:id::NUMBER AS id,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                CAST($1:accountholdername_icare::TEXT AS VARCHAR(255)) AS accountholdername_icare,
                CAST($1:cardholdername_icare::TEXT AS VARCHAR(255)) AS cardholdername_icare,
                $1:creditcardissuer::NUMBER AS creditcardissuer,
                CAST($1:cardnumber_icare::TEXT AS VARCHAR(255)) AS cardnumber_icare,
                $1:bankaccounttype_icare::NUMBER AS bankaccounttype_icare,
                CAST($1:expirymonth_icare::TEXT AS VARCHAR(255)) AS expirymonth_icare,
                CAST($1:expiryyear_icare::TEXT AS VARCHAR(255)) AS expiryyear_icare,
                CAST($1:tokennumber_icare::TEXT AS VARCHAR(255)) AS tokennumber_icare,
                CAST($1:camtbankaccnumber_icare::TEXT AS VARCHAR(255)) AS camtbankaccnumber_icare,
                $1:rnswpayment_ext::BOOLEAN AS rnswpayment_ext,
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
            FROM {{ source('gwbc', 'bc_paymentinstrument') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS paymentinstrument_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'immutable',
                        'publicid',
                        'chequenumber_icare',
                        'paymentmethod',
                        'beanversion',
                        'accountid',
                        'createtime',
                        'retired',
                        'isactive_icare',
                        'producerid',
                        'updateuserid',
                        'detail',
                        'bankaccountnumber_icare',
                        'bsbnumber_icare',
                        'updatetime',
                        'token',
                        'description',
                        'accountholdername_icare',
                        'cardholdername_icare',
                        'creditcardissuer',
                        'cardnumber_icare',
                        'bankaccounttype_icare',
                        'expirymonth_icare',
                        'expiryyear_icare',
                        'tokennumber_icare',
                        'camtbankaccnumber_icare',
                        'rnswpayment_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_transaction.
                                                transaction_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "cc_transaction"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:PaymentType::NUMBER AS paymenttype,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:Matter::NUMBER AS matter,
                data_payload:CloseClaim::BOOLEAN AS closeclaim,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:SubmitDate::NUMBER/1000) AS submitdate,
                data_payload:CostType::NUMBER AS costtype,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:RecoveryType_icare::NUMBER AS recoverytype_icare,
                data_payload:ReservingCurrency::NUMBER AS reservingcurrency,
                data_payload:CostCategory::NUMBER AS costcategory,
                data_payload:ReserveLineID::NUMBER AS reservelineid,
                data_payload:Currency::NUMBER AS currency,
                data_payload:RecoveryCategory::NUMBER AS recoverycategory,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ClaimToReportingExchangeRate::NUMBER AS claimtoreportingexchangerate,
                data_payload:RecoveryCodingID::NUMBER AS recoverycodingid,
                data_payload:ID::NUMBER AS id,
                data_payload:TransactionSetID::NUMBER AS transactionsetid,
                data_payload:DoesNotErodeReserves::BOOLEAN AS doesnoterodereserves,
                data_payload:ExposureID::NUMBER AS exposureid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:CheckID::NUMBER AS checkid,
                data_payload:LifeCycleState::NUMBER AS lifecyclestate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:PayerDenormID::NUMBER AS payerdenormid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:InvoiceNumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:Comments::TEXT AS VARCHAR(255)) AS comments,
                data_payload:TransToReservingExchangeRate::NUMBER AS transtoreservingexchangerate,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:OBOClaimContactID::NUMBER AS oboclaimcontactid,
                data_payload:TransToClaimExchangeRate::NUMBER AS transtoclaimexchangerate,
                data_payload:PayeeType_icare::NUMBER AS payeetype_icare,
                data_payload:ClaimContactID::NUMBER AS claimcontactid,
                data_payload:CloseExposure::BOOLEAN AS closeexposure,
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
            FROM {{ source('gwcc', 'cc_transaction') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:paymenttype::NUMBER AS paymenttype,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:matter::NUMBER AS matter,
                $1:closeclaim::BOOLEAN AS closeclaim,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:submitdate::TIMESTAMP_TZ AS submitdate,
                $1:costtype::NUMBER AS costtype,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:recoverytype_icare::NUMBER AS recoverytype_icare,
                $1:reservingcurrency::NUMBER AS reservingcurrency,
                $1:costcategory::NUMBER AS costcategory,
                $1:reservelineid::NUMBER AS reservelineid,
                $1:currency::NUMBER AS currency,
                $1:recoverycategory::NUMBER AS recoverycategory,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:claimtoreportingexchangerate::NUMBER AS claimtoreportingexchangerate,
                $1:recoverycodingid::NUMBER AS recoverycodingid,
                $1:id::NUMBER AS id,
                $1:transactionsetid::NUMBER AS transactionsetid,
                $1:doesnoterodereserves::BOOLEAN AS doesnoterodereserves,
                $1:exposureid::NUMBER AS exposureid,
                $1:createuserid::NUMBER AS createuserid,
                $1:checkid::NUMBER AS checkid,
                $1:lifecyclestate::NUMBER AS lifecyclestate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:payerdenormid::NUMBER AS payerdenormid,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:invoicenumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                $1:status::NUMBER AS status,
                CAST($1:comments::TEXT AS VARCHAR(255)) AS comments,
                $1:transtoreservingexchangerate::NUMBER AS transtoreservingexchangerate,
                $1:subtype::NUMBER AS subtype,
                $1:oboclaimcontactid::NUMBER AS oboclaimcontactid,
                $1:transtoclaimexchangerate::NUMBER AS transtoclaimexchangerate,
                $1:payeetype_icare::NUMBER AS payeetype_icare,
                $1:claimcontactid::NUMBER AS claimcontactid,
                $1:closeexposure::BOOLEAN AS closeexposure,
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
            FROM {{ source('gwcc', 'cc_transaction') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS transaction_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'paymenttype',
                        'loadcommandid',
                        'matter',
                        'closeclaim',
                        'publicid',
                        'submitdate',
                        'costtype',
                        'createtime',
                        'recoverytype_icare',
                        'reservingcurrency',
                        'costcategory',
                        'reservelineid',
                        'currency',
                        'recoverycategory',
                        'updatetime',
                        'claimid',
                        'claimtoreportingexchangerate',
                        'recoverycodingid',
                        'transactionsetid',
                        'doesnoterodereserves',
                        'exposureid',
                        'createuserid',
                        'checkid',
                        'lifecyclestate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'payerdenormid',
                        'updateuserid',
                        'invoicenumber_icare',
                        'status',
                        'comments',
                        'transtoreservingexchangerate',
                        'subtype',
                        'oboclaimcontactid',
                        'transtoclaimexchangerate',
                        'payeetype_icare',
                        'claimcontactid',
                        'closeexposure'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
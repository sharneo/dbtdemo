
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This Converts Parquet or AVRO Data Loaded in the Variant Column in the RAW DB into Flattend Views
                                                This also creates a HASH_KEY for Incremental Tables for the Curated Layer 
                                                Additional CDA Files are Null in the AVRO but not in CDA .
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    tags=["raw_gwcc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PaymentInformationID::TEXT AS VARCHAR(255)) AS paymentinformationid,
                CAST(data_payload:ReversalIndicator::TEXT AS VARCHAR(255)) AS reversalindicator,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(255)) AS claimnumber,
                TO_TIMESTAMP_TZ(data_payload:BookingDate::NUMBER/1000) AS bookingdate,
                data_payload:ReasonToMismatch::NUMBER AS reasontomismatch,
                data_payload:TransactionProprietaryCode::NUMBER AS transactionproprietarycode,
                CAST(data_payload:TransactionDomainCode::TEXT AS VARCHAR(255)) AS transactiondomaincode,
                CAST(data_payload:DebtorAccountID::TEXT AS VARCHAR(255)) AS debtoraccountid,
                data_payload:ReconciliationStatus::NUMBER AS reconciliationstatus,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:TransactionProprietaryIssuer::TEXT AS VARCHAR(255)) AS transactionproprietaryissuer,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ValueDate::NUMBER/1000) AS valuedate,
                CAST(data_payload:PaymentSource::TEXT AS VARCHAR(5)) AS paymentsource,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:TriggerNotification::BOOLEAN AS triggernotification,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:InstructionID::TEXT AS VARCHAR(255)) AS instructionid,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:ArchivedCheckPublicID::TEXT AS VARCHAR(64)) AS archivedcheckpublicid,
                CAST(data_payload:ProviderUniqueRefID::TEXT AS VARCHAR(255)) AS provideruniquerefid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:ControlRecordMessageId::TEXT AS VARCHAR(255)) AS controlrecordmessageid,
                CAST(data_payload:TransactionAmount AS NUMBER(19,2)) AS transactionamount,
                CAST(data_payload:Memo::TEXT AS VARCHAR(255)) AS memo,
                CAST(data_payload:DebtorName::TEXT AS VARCHAR(255)) AS debtorname,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ProcessingStatus::TEXT AS VARCHAR(255)) AS processingstatus,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:StatementID::TEXT AS VARCHAR(255)) AS statementid,
                CAST(data_payload:WestpacUIDNonTransformed::TEXT AS VARCHAR(255)) AS westpacuidnontransformed,
                CAST(data_payload:TransactionStatus::TEXT AS VARCHAR(255)) AS transactionstatus,
                CAST(data_payload:CheckNumber::TEXT AS VARCHAR(255)) AS checknumber,
                CAST(data_payload:GWCCPaymentID::TEXT AS VARCHAR(255)) AS gwccpaymentid,
                CAST(data_payload:PaymentMethodCode::TEXT AS VARCHAR(255)) AS paymentmethodcode,
                CAST(data_payload:AdditionalInfo::TEXT AS VARCHAR(255)) AS additionalinfo,
                data_payload:CreditDebitIndicator::NUMBER AS creditdebitindicator,
                CAST(data_payload:EndToEndID::TEXT AS VARCHAR(255)) AS endtoendid,
                CAST(data_payload:MerchantID::TEXT AS VARCHAR(255)) AS merchantid,
                TO_TIMESTAMP_TZ(data_payload:StatementDateTime::NUMBER/1000) AS statementdatetime,
                CAST(data_payload:CreditorAccountID::TEXT AS VARCHAR(255)) AS creditoraccountid,
                CAST(data_payload:RecallID::TEXT AS VARCHAR(255)) AS recallid,
                CAST(data_payload:WestpacUID::TEXT AS VARCHAR(255)) AS westpacuid,
                data_payload:CAMT053_ID::NUMBER AS camt053_id,
                CAST(data_payload:TransactionDomainFamilyCode::TEXT AS VARCHAR(255)) AS transactiondomainfamilycode,
                CAST(data_payload:TransactionSubFamilyCode::TEXT AS VARCHAR(255)) AS transactionsubfamilycode,
                data_payload:MatchedPaymentID::NUMBER AS matchedpaymentid,
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
            FROM {{ source('gwcc', 'ccx_camt053mispresentchk_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:paymentinformationid::TEXT AS VARCHAR(255)) AS paymentinformationid,
                CAST($1:reversalindicator::TEXT AS VARCHAR(255)) AS reversalindicator,
                CAST($1:claimnumber::TEXT AS VARCHAR(255)) AS claimnumber,
                $1:bookingdate::TIMESTAMP_TZ AS bookingdate,
                $1:reasontomismatch::NUMBER AS reasontomismatch,
                $1:transactionproprietarycode::NUMBER AS transactionproprietarycode,
                CAST($1:transactiondomaincode::TEXT AS VARCHAR(255)) AS transactiondomaincode,
                CAST($1:debtoraccountid::TEXT AS VARCHAR(255)) AS debtoraccountid,
                $1:reconciliationstatus::NUMBER AS reconciliationstatus,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:transactionproprietaryissuer::TEXT AS VARCHAR(255)) AS transactionproprietaryissuer,
                $1:id::NUMBER AS id,
                $1:valuedate::TIMESTAMP_TZ AS valuedate,
                CAST($1:paymentsource::TEXT AS VARCHAR(5)) AS paymentsource,
                $1:createuserid::NUMBER AS createuserid,
                $1:triggernotification::BOOLEAN AS triggernotification,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:instructionid::TEXT AS VARCHAR(255)) AS instructionid,
                $1:retired::NUMBER AS retired,
                CAST($1:archivedcheckpublicid::TEXT AS VARCHAR(64)) AS archivedcheckpublicid,
                CAST($1:provideruniquerefid::TEXT AS VARCHAR(255)) AS provideruniquerefid,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:controlrecordmessageid::TEXT AS VARCHAR(255)) AS controlrecordmessageid,
                CAST($1:transactionamount AS NUMBER(19,2)) AS transactionamount,
                CAST($1:memo::TEXT AS VARCHAR(255)) AS memo,
                CAST($1:debtorname::TEXT AS VARCHAR(255)) AS debtorname,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:processingstatus::TEXT AS VARCHAR(255)) AS processingstatus,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:statementid::TEXT AS VARCHAR(255)) AS statementid,
                CAST($1:westpacuidnontransformed::TEXT AS VARCHAR(255)) AS westpacuidnontransformed,
                CAST($1:transactionstatus::TEXT AS VARCHAR(255)) AS transactionstatus,
                CAST($1:checknumber::TEXT AS VARCHAR(255)) AS checknumber,
                CAST($1:gwccpaymentid::TEXT AS VARCHAR(255)) AS gwccpaymentid,
                CAST($1:paymentmethodcode::TEXT AS VARCHAR(255)) AS paymentmethodcode,
                CAST($1:additionalinfo::TEXT AS VARCHAR(255)) AS additionalinfo,
                $1:creditdebitindicator::NUMBER AS creditdebitindicator,
                CAST($1:endtoendid::TEXT AS VARCHAR(255)) AS endtoendid,
                CAST($1:merchantid::TEXT AS VARCHAR(255)) AS merchantid,
                $1:statementdatetime::TIMESTAMP_TZ AS statementdatetime,
                CAST($1:creditoraccountid::TEXT AS VARCHAR(255)) AS creditoraccountid,
                CAST($1:recallid::TEXT AS VARCHAR(255)) AS recallid,
                CAST($1:westpacuid::TEXT AS VARCHAR(255)) AS westpacuid,
                $1:camt053_id::NUMBER AS camt053_id,
                CAST($1:transactiondomainfamilycode::TEXT AS VARCHAR(255)) AS transactiondomainfamilycode,
                CAST($1:transactionsubfamilycode::TEXT AS VARCHAR(255)) AS transactionsubfamilycode,
                $1:matchedpaymentid::NUMBER AS matchedpaymentid,
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
            FROM {{ source('gwcc', 'ccx_camt053mispresentchk_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
    Transformed CTE is To Create the HASH_KEY Based on the Right Combination
-#}   
cte_transformed AS (
    SELECT
        *,
        CASE
             WHEN file_type = 'AVRO' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'loadcommandid',
                        'paymentinformationid',
                        'reversalindicator',
                        'claimnumber',
                        'bookingdate',
                        'reasontomismatch',
                        'transactionproprietarycode',
                        'transactiondomaincode',
                        'debtoraccountid',
                        'reconciliationstatus',
                        'updatetime',
                        'transactionproprietaryissuer',
                        'id',
                        'valuedate',
                        'paymentsource',
                        'createuserid',
                        'triggernotification',
                        'beanversion',
                        'instructionid',
                        'retired',
                        'archivedcheckpublicid',
                        'provideruniquerefid',
                        'updateuserid',
                        'controlrecordmessageid',
                        'transactionamount',
                        'memo',
                        'debtorname',
                        'publicid',
                        'processingstatus',
                        'createtime',
                        'statementid',
                        'westpacuidnontransformed',
                        'transactionstatus',
                        'checknumber',
                        'gwccpaymentid',
                        'paymentmethodcode',
                        'additionalinfo',
                        'creditdebitindicator',
                        'endtoendid',
                        'merchantid',
                        'statementdatetime',
                        'creditoraccountid',
                        'recallid',
                        'westpacuid',
                        'camt053_id',
                        'transactiondomainfamilycode',
                        'transactionsubfamilycode',
                        'matchedpaymentid'
                        ]) }}
            WHEN file_type = 'PARQUET' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'id',
                        'gwcbi_seqval'
                        ]) }}
        END AS hash_key    
    FROM cte_source_data
)
SELECT * FROM cte_transformed
        
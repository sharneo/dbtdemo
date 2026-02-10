
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
                CAST(data_payload:ReversalIndicator::TEXT AS VARCHAR(255)) AS reversalindicator,
                CAST(data_payload:PaymentInformationID::TEXT AS VARCHAR(255)) AS paymentinformationid,
                CAST(data_payload:DebtorName::TEXT AS VARCHAR(255)) AS debtorname,
                data_payload:AssociatedCheck::NUMBER AS associatedcheck,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ProcessingStatus::TEXT AS VARCHAR(255)) AS processingstatus,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(255)) AS claimnumber,
                CAST(data_payload:AssociatedCheckPublicID::TEXT AS VARCHAR(255)) AS associatedcheckpublicid,
                data_payload:Attempts::NUMBER AS attempts,
                TO_TIMESTAMP_TZ(data_payload:BookingDate::NUMBER/1000) AS bookingdate,
                CAST(data_payload:CheckPublicID::TEXT AS VARCHAR(255)) AS checkpublicid,
                data_payload:TransactionProprietaryCode::NUMBER AS transactionproprietarycode,
                CAST(data_payload:TransactionDomainCode::TEXT AS VARCHAR(255)) AS transactiondomaincode,
                CAST(data_payload:Exception::TEXT AS VARCHAR(16777216)) AS exception,
                data_payload:AvailableSince::NUMBER AS availablesince,
                CAST(data_payload:CheckNumber::TEXT AS VARCHAR(255)) AS checknumber,
                CAST(data_payload:DebtorAccountID::TEXT AS VARCHAR(255)) AS debtoraccountid,
                TO_TIMESTAMP_TZ(data_payload:LastUpdatedDate::NUMBER/1000) AS lastupdateddate,
                CAST(data_payload:PaymentMethodCode::TEXT AS VARCHAR(255)) AS paymentmethodcode,
                CAST(data_payload:AdditionalInfo::TEXT AS VARCHAR(255)) AS additionalinfo,
                CAST(data_payload:TransactionProprietaryIssuer::TEXT AS VARCHAR(255)) AS transactionproprietaryissuer,
                data_payload:ID::NUMBER AS id,
                data_payload:CreditDebitIndicator::NUMBER AS creditdebitindicator,
                CAST(data_payload:CheckedOutBy::TEXT AS VARCHAR(50)) AS checkedoutby,
                TO_TIMESTAMP_TZ(data_payload:ValueDate::NUMBER/1000) AS valuedate,
                CAST(data_payload:EndToEndID::TEXT AS VARCHAR(255)) AS endtoendid,
                data_payload:ProcessHistoryID::NUMBER AS processhistoryid,
                CAST(data_payload:MerchantID::TEXT AS VARCHAR(255)) AS merchantid,
                data_payload:Priority::NUMBER AS priority,
                CAST(data_payload:InstructionID::TEXT AS VARCHAR(255)) AS instructionid,
                TO_TIMESTAMP_TZ(data_payload:StatementDateTime::NUMBER/1000) AS statementdatetime,
                data_payload:SkipFlag::BOOLEAN AS skipflag,
                CAST(data_payload:RecallID::TEXT AS VARCHAR(255)) AS recallid,
                CAST(data_payload:CreditorAccountID::TEXT AS VARCHAR(255)) AS creditoraccountid,
                TO_TIMESTAMP_TZ(data_payload:LastUpdateTime::NUMBER/1000) AS lastupdatetime,
                TO_TIMESTAMP_TZ(data_payload:CreationTime::NUMBER/1000) AS creationtime,
                CAST(data_payload:WestpacUID::TEXT AS VARCHAR(255)) AS westpacuid,
                data_payload:EntryLevelCreditDebitIndicator::NUMBER AS entrylevelcreditdebitindicator,
                data_payload:IsPresented::BOOLEAN AS ispresented,
                data_payload:CAMT053_ID::NUMBER AS camt053_id,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:TransactionSubFamilyCode::TEXT AS VARCHAR(255)) AS transactionsubfamilycode,
                CAST(data_payload:TransactionDomainFamilyCode::TEXT AS VARCHAR(255)) AS transactiondomainfamilycode,
                data_payload:ControlRecordMessageID::NUMBER AS controlrecordmessageid,
                CAST(data_payload:TransactionAmount AS NUMBER(19,2)) AS transactionamount,
                CAST(data_payload:OriginalWestpacUID::TEXT AS VARCHAR(255)) AS originalwestpacuid,
                CAST(data_payload:PaymentSource::TEXT AS VARCHAR(5)) AS paymentsource,
                CAST(data_payload:ProviderUniqueRefID::TEXT AS VARCHAR(255)) AS provideruniquerefid,
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
            FROM {{ source('gwcc', 'ccx_camt053odsworkitem_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:reversalindicator::TEXT AS VARCHAR(255)) AS reversalindicator,
                CAST($1:paymentinformationid::TEXT AS VARCHAR(255)) AS paymentinformationid,
                CAST($1:debtorname::TEXT AS VARCHAR(255)) AS debtorname,
                $1:associatedcheck::NUMBER AS associatedcheck,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:processingstatus::TEXT AS VARCHAR(255)) AS processingstatus,
                CAST($1:claimnumber::TEXT AS VARCHAR(255)) AS claimnumber,
                CAST($1:associatedcheckpublicid::TEXT AS VARCHAR(255)) AS associatedcheckpublicid,
                $1:attempts::NUMBER AS attempts,
                $1:bookingdate::TIMESTAMP_TZ AS bookingdate,
                CAST($1:checkpublicid::TEXT AS VARCHAR(255)) AS checkpublicid,
                $1:transactionproprietarycode::NUMBER AS transactionproprietarycode,
                CAST($1:transactiondomaincode::TEXT AS VARCHAR(255)) AS transactiondomaincode,
                CAST($1:exception::TEXT AS VARCHAR(16777216)) AS exception,
                $1:availablesince::NUMBER AS availablesince,
                CAST($1:checknumber::TEXT AS VARCHAR(255)) AS checknumber,
                CAST($1:debtoraccountid::TEXT AS VARCHAR(255)) AS debtoraccountid,
                $1:lastupdateddate::TIMESTAMP_TZ AS lastupdateddate,
                CAST($1:paymentmethodcode::TEXT AS VARCHAR(255)) AS paymentmethodcode,
                CAST($1:additionalinfo::TEXT AS VARCHAR(255)) AS additionalinfo,
                CAST($1:transactionproprietaryissuer::TEXT AS VARCHAR(255)) AS transactionproprietaryissuer,
                $1:id::NUMBER AS id,
                $1:creditdebitindicator::NUMBER AS creditdebitindicator,
                CAST($1:checkedoutby::TEXT AS VARCHAR(50)) AS checkedoutby,
                $1:valuedate::TIMESTAMP_TZ AS valuedate,
                CAST($1:endtoendid::TEXT AS VARCHAR(255)) AS endtoendid,
                $1:processhistoryid::NUMBER AS processhistoryid,
                CAST($1:merchantid::TEXT AS VARCHAR(255)) AS merchantid,
                $1:priority::NUMBER AS priority,
                CAST($1:instructionid::TEXT AS VARCHAR(255)) AS instructionid,
                $1:statementdatetime::TIMESTAMP_TZ AS statementdatetime,
                $1:skipflag::BOOLEAN AS skipflag,
                CAST($1:recallid::TEXT AS VARCHAR(255)) AS recallid,
                CAST($1:creditoraccountid::TEXT AS VARCHAR(255)) AS creditoraccountid,
                $1:lastupdatetime::TIMESTAMP_TZ AS lastupdatetime,
                $1:creationtime::TIMESTAMP_TZ AS creationtime,
                CAST($1:westpacuid::TEXT AS VARCHAR(255)) AS westpacuid,
                $1:entrylevelcreditdebitindicator::NUMBER AS entrylevelcreditdebitindicator,
                $1:ispresented::BOOLEAN AS ispresented,
                $1:camt053_id::NUMBER AS camt053_id,
                $1:status::NUMBER AS status,
                CAST($1:transactionsubfamilycode::TEXT AS VARCHAR(255)) AS transactionsubfamilycode,
                CAST($1:transactiondomainfamilycode::TEXT AS VARCHAR(255)) AS transactiondomainfamilycode,
                $1:controlrecordmessageid::NUMBER AS controlrecordmessageid,
                CAST($1:transactionamount AS NUMBER(19,2)) AS transactionamount,
                CAST($1:originalwestpacuid::TEXT AS VARCHAR(255)) AS originalwestpacuid,
                CAST($1:paymentsource::TEXT AS VARCHAR(5)) AS paymentsource,
                CAST($1:provideruniquerefid::TEXT AS VARCHAR(255)) AS provideruniquerefid,
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
            FROM {{ source('gwcc', 'ccx_camt053odsworkitem_icare') }}
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
                        'reversalindicator',
                        'paymentinformationid',
                        'debtorname',
                        'associatedcheck',
                        'publicid',
                        'processingstatus',
                        'claimnumber',
                        'associatedcheckpublicid',
                        'attempts',
                        'bookingdate',
                        'checkpublicid',
                        'transactionproprietarycode',
                        'transactiondomaincode',
                        'exception',
                        'availablesince',
                        'checknumber',
                        'debtoraccountid',
                        'lastupdateddate',
                        'paymentmethodcode',
                        'additionalinfo',
                        'transactionproprietaryissuer',
                        'id',
                        'creditdebitindicator',
                        'checkedoutby',
                        'valuedate',
                        'endtoendid',
                        'processhistoryid',
                        'merchantid',
                        'priority',
                        'instructionid',
                        'statementdatetime',
                        'skipflag',
                        'recallid',
                        'creditoraccountid',
                        'lastupdatetime',
                        'creationtime',
                        'westpacuid',
                        'entrylevelcreditdebitindicator',
                        'ispresented',
                        'camt053_id',
                        'status',
                        'transactionsubfamilycode',
                        'transactiondomainfamilycode',
                        'controlrecordmessageid',
                        'transactionamount',
                        'originalwestpacuid',
                        'paymentsource',
                        'provideruniquerefid'
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
        
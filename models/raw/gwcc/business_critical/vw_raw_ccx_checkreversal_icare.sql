
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
                data_payload:PaymentType::NUMBER AS paymenttype,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:WaivedFlag::BOOLEAN AS waivedflag,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:WriteOffAmountUnallocated AS NUMBER(18,2)) AS writeoffamountunallocated,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:TotalReimbursementAmount AS NUMBER(18,2)) AS totalreimbursementamount,
                CAST(data_payload:WriteOffAmountReceivedToDate AS NUMBER(18,2)) AS writeoffamountreceivedtodate,
                data_payload:Payer::NUMBER AS payer,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:InternalInvoiceNumber::TEXT AS VARCHAR(255)) AS internalinvoicenumber,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:InvoiceDate::NUMBER/1000) AS invoicedate,
                TO_TIMESTAMP_TZ(data_payload:DueDate::NUMBER/1000) AS duedate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PaymentAmountReceivedToDate AS NUMBER(18,2)) AS paymentamountreceivedtodate,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PaymentAmountUnallocated AS NUMBER(18,2)) AS paymentamountunallocated,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:PayeeInstructionsDetails::TEXT AS VARCHAR(250)) AS payeeinstructionsdetails,
                CAST(data_payload:InvoiceNumber::TEXT AS VARCHAR(255)) AS invoicenumber,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(255)) AS accountnumber,
                data_payload:Claim::NUMBER AS claim,
                data_payload:RecoveryType::NUMBER AS recoverytype,
                CAST(data_payload:TotalRecoveryPAYG AS NUMBER(18,2)) AS totalrecoverypayg,
                TO_TIMESTAMP_TZ(data_payload:PaymentDueDate::NUMBER/1000) AS paymentduedate,
                CAST(data_payload:CRNNumber::TEXT AS VARCHAR(255)) AS crnnumber,
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
            FROM {{ source('gwcc', 'ccx_checkreversal_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:paymenttype::NUMBER AS paymenttype,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:waivedflag::BOOLEAN AS waivedflag,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:writeoffamountunallocated AS NUMBER(18,2)) AS writeoffamountunallocated,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:totalreimbursementamount AS NUMBER(18,2)) AS totalreimbursementamount,
                CAST($1:writeoffamountreceivedtodate AS NUMBER(18,2)) AS writeoffamountreceivedtodate,
                $1:payer::NUMBER AS payer,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:internalinvoicenumber::TEXT AS VARCHAR(255)) AS internalinvoicenumber,
                $1:id::NUMBER AS id,
                $1:invoicedate::TIMESTAMP_TZ AS invoicedate,
                $1:duedate::TIMESTAMP_TZ AS duedate,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:paymentamountreceivedtodate AS NUMBER(18,2)) AS paymentamountreceivedtodate,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:paymentamountunallocated AS NUMBER(18,2)) AS paymentamountunallocated,
                $1:status::NUMBER AS status,
                CAST($1:payeeinstructionsdetails::TEXT AS VARCHAR(250)) AS payeeinstructionsdetails,
                CAST($1:invoicenumber::TEXT AS VARCHAR(255)) AS invoicenumber,
                CAST($1:accountnumber::TEXT AS VARCHAR(255)) AS accountnumber,
                $1:claim::NUMBER AS claim,
                $1:recoverytype::NUMBER AS recoverytype,
                CAST($1:totalrecoverypayg AS NUMBER(18,2)) AS totalrecoverypayg,
                $1:paymentduedate::TIMESTAMP_TZ AS paymentduedate,
                CAST($1:crnnumber::TEXT AS VARCHAR(255)) AS crnnumber,
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
            FROM {{ source('gwcc', 'ccx_checkreversal_icare') }}
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
                                'paymenttype',
                        'loadcommandid',
                        'waivedflag',
                        'publicid',
                        'writeoffamountunallocated',
                        'createtime',
                        'totalreimbursementamount',
                        'writeoffamountreceivedtodate',
                        'payer',
                        'updatetime',
                        'internalinvoicenumber',
                        'id',
                        'invoicedate',
                        'duedate',
                        'createuserid',
                        'paymentamountreceivedtodate',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'updateuserid',
                        'paymentamountunallocated',
                        'status',
                        'payeeinstructionsdetails',
                        'invoicenumber',
                        'accountnumber',
                        'claim',
                        'recoverytype',
                        'totalrecoverypayg',
                        'paymentduedate',
                        'crnnumber'
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
        
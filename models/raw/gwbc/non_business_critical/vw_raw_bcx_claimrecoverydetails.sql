
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
    tags=["raw_gwbc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:ClaimPublicID::TEXT AS VARCHAR(64)) AS claimpublicid,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(40)) AS claimnumber,
                CAST(data_payload:Message::TEXT AS VARCHAR(100)) AS message,
                CAST(data_payload:InvoiceType::TEXT AS VARCHAR(150)) AS invoicetype,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:RecoveryType::NUMBER AS recoverytype,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(255)) AS accountnumber,
                data_payload:InvoiceIssueFlag::BOOLEAN AS invoiceissueflag,
                CAST(data_payload:RecoveryPublicID::TEXT AS VARCHAR(64)) AS recoverypublicid,
                CAST(data_payload:NewRecoveryPublicID::TEXT AS VARCHAR(64)) AS newrecoverypublicid,
                data_payload:WaiveFlag::BOOLEAN AS waiveflag,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PAYGAmount AS NUMBER(18,2)) AS paygamount,
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
            FROM {{ source('gwbc', 'bcx_claimrecoverydetails') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:claimpublicid::TEXT AS VARCHAR(64)) AS claimpublicid,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:claimnumber::TEXT AS VARCHAR(40)) AS claimnumber,
                CAST($1:message::TEXT AS VARCHAR(100)) AS message,
                CAST($1:invoicetype::TEXT AS VARCHAR(150)) AS invoicetype,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:recoverytype::NUMBER AS recoverytype,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:accountnumber::TEXT AS VARCHAR(255)) AS accountnumber,
                $1:invoiceissueflag::BOOLEAN AS invoiceissueflag,
                CAST($1:recoverypublicid::TEXT AS VARCHAR(64)) AS recoverypublicid,
                CAST($1:newrecoverypublicid::TEXT AS VARCHAR(64)) AS newrecoverypublicid,
                $1:waiveflag::BOOLEAN AS waiveflag,
                $1:id::NUMBER AS id,
                CAST($1:paygamount AS NUMBER(18,2)) AS paygamount,
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
            FROM {{ source('gwbc', 'bcx_claimrecoverydetails') }}
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
                                'claimpublicid',
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'claimnumber',
                        'message',
                        'invoicetype',
                        'beanversion',
                        'createtime',
                        'retired',
                        'updateuserid',
                        'recoverytype',
                        'updatetime',
                        'accountnumber',
                        'invoiceissueflag',
                        'recoverypublicid',
                        'newrecoverypublicid',
                        'waiveflag',
                        'id',
                        'paygamount'
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
        
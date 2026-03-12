
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
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:Reversed::BOOLEAN AS reversed,
                CAST(data_payload:CommissionAmount AS NUMBER(18,2)) AS commissionamount,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:CommissionAmount_cur::NUMBER AS commissionamount_cur,
                data_payload:Reason::NUMBER AS reason,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:TransactionDate::NUMBER/1000) AS transactiondate,
                CAST(data_payload:CommissionAmountChanged AS NUMBER(18,2)) AS commissionamountchanged,
                data_payload:Currency::NUMBER AS currency,
                data_payload:CommissionAmountChanged_cur::NUMBER AS commissionamountchanged_cur,
                data_payload:ReversalReason::NUMBER AS reversalreason,
                data_payload:WriteoffChannel::NUMBER AS writeoffchannel,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Amount AS NUMBER(18,2)) AS amount,
                data_payload:Amount_cur::NUMBER AS amount_cur,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:TransactionNumberDenorm::TEXT AS VARCHAR(255)) AS transactionnumberdenorm,
                CAST(data_payload:TransactionNumber::TEXT AS VARCHAR(255)) AS transactionnumber,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:Basis AS NUMBER(18,2)) AS basis,
                data_payload:Basis_cur::NUMBER AS basis_cur,
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
            FROM {{ source('gwbc', 'bc_transaction') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:reversed::BOOLEAN AS reversed,
                CAST($1:commissionamount AS NUMBER(18,2)) AS commissionamount,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:commissionamount_cur::NUMBER AS commissionamount_cur,
                $1:reason::NUMBER AS reason,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:transactiondate::TIMESTAMP_TZ AS transactiondate,
                CAST($1:commissionamountchanged AS NUMBER(18,2)) AS commissionamountchanged,
                $1:currency::NUMBER AS currency,
                $1:commissionamountchanged_cur::NUMBER AS commissionamountchanged_cur,
                $1:reversalreason::NUMBER AS reversalreason,
                $1:writeoffchannel::NUMBER AS writeoffchannel,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:amount AS NUMBER(18,2)) AS amount,
                $1:amount_cur::NUMBER AS amount_cur,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:transactionnumberdenorm::TEXT AS VARCHAR(255)) AS transactionnumberdenorm,
                CAST($1:transactionnumber::TEXT AS VARCHAR(255)) AS transactionnumber,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:subtype::NUMBER AS subtype,
                CAST($1:basis AS NUMBER(18,2)) AS basis,
                $1:basis_cur::NUMBER AS basis_cur,
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
            FROM {{ source('gwbc', 'bc_transaction') }}
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
                        'reversed',
                        'commissionamount',
                        'publicid',
                        'commissionamount_cur',
                        'reason',
                        'createtime',
                        'transactiondate',
                        'commissionamountchanged',
                        'currency',
                        'commissionamountchanged_cur',
                        'reversalreason',
                        'writeoffchannel',
                        'updatetime',
                        'amount',
                        'amount_cur',
                        'id',
                        'createuserid',
                        'transactionnumberdenorm',
                        'transactionnumber',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'updateuserid',
                        'subtype',
                        'basis',
                        'basis_cur'
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
        
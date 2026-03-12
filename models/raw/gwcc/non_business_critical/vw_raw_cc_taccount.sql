
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
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:CreditBalance AS NUMBER(18,2)) AS creditbalance,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ReserveLineID::NUMBER AS reservelineid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:DebitReservingBalance AS NUMBER(18,2)) AS debitreservingbalance,
                CAST(data_payload:DebitBalance AS NUMBER(18,2)) AS debitbalance,
                CAST(data_payload:DebitRptBalance AS NUMBER(18,2)) AS debitrptbalance,
                data_payload:TAccountType::NUMBER AS taccounttype,
                data_payload:NormalBalance::NUMBER AS normalbalance,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:NumContributingTxns::NUMBER AS numcontributingtxns,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CreditReservingBalance AS NUMBER(18,2)) AS creditreservingbalance,
                CAST(data_payload:CreditRptBalance AS NUMBER(18,2)) AS creditrptbalance,
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
            FROM {{ source('gwcc', 'cc_taccount') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:creditbalance AS NUMBER(18,2)) AS creditbalance,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:reservelineid::NUMBER AS reservelineid,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:debitreservingbalance AS NUMBER(18,2)) AS debitreservingbalance,
                CAST($1:debitbalance AS NUMBER(18,2)) AS debitbalance,
                CAST($1:debitrptbalance AS NUMBER(18,2)) AS debitrptbalance,
                $1:taccounttype::NUMBER AS taccounttype,
                $1:normalbalance::NUMBER AS normalbalance,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:numcontributingtxns::NUMBER AS numcontributingtxns,
                $1:id::NUMBER AS id,
                CAST($1:creditreservingbalance AS NUMBER(18,2)) AS creditreservingbalance,
                CAST($1:creditrptbalance AS NUMBER(18,2)) AS creditrptbalance,
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
            FROM {{ source('gwcc', 'cc_taccount') }}
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
                        'createuserid',
                        'creditbalance',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'reservelineid',
                        'updateuserid',
                        'debitreservingbalance',
                        'debitbalance',
                        'debitrptbalance',
                        'taccounttype',
                        'normalbalance',
                        'updatetime',
                        'numcontributingtxns',
                        'id',
                        'creditreservingbalance',
                        'creditrptbalance'
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
        
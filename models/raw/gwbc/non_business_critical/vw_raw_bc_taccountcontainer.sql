
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:UnearnedDenorm AS NUMBER(18,2)) AS unearneddenorm,
                data_payload:UnearnedDenorm_cur::NUMBER AS unearneddenorm_cur,
                data_payload:Currency::NUMBER AS currency,
                CAST(data_payload:ExpenseDenorm AS NUMBER(18,2)) AS expensedenorm,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ExpenseDenorm_cur::NUMBER AS expensedenorm_cur,
                CAST(data_payload:RevenueDenorm AS NUMBER(18,2)) AS revenuedenorm,
                data_payload:RevenueDenorm_cur::NUMBER AS revenuedenorm_cur,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:NegativeWriteoffDenorm AS NUMBER(18,2)) AS negativewriteoffdenorm,
                CAST(data_payload:BilledDenorm AS NUMBER(18,2)) AS billeddenorm,
                data_payload:NegativeWriteoffDenorm_cur::NUMBER AS negativewriteoffdenorm_cur,
                data_payload:BilledDenorm_cur::NUMBER AS billeddenorm_cur,
                CAST(data_payload:WriteoffExpenseDenorm AS NUMBER(18,2)) AS writeoffexpensedenorm,
                data_payload:WriteoffExpenseDenorm_cur::NUMBER AS writeoffexpensedenorm_cur,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:DueDenorm AS NUMBER(18,2)) AS duedenorm,
                data_payload:DueDenorm_cur::NUMBER AS duedenorm_cur,
                CAST(data_payload:ReserveDenorm AS NUMBER(18,2)) AS reservedenorm,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ReserveDenorm_cur::NUMBER AS reservedenorm_cur,
                CAST(data_payload:UnbilledDenorm AS NUMBER(18,2)) AS unbilleddenorm,
                data_payload:UnbilledDenorm_cur::NUMBER AS unbilleddenorm_cur,
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
            FROM {{ source('gwbc', 'bc_taccountcontainer') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:unearneddenorm AS NUMBER(18,2)) AS unearneddenorm,
                $1:unearneddenorm_cur::NUMBER AS unearneddenorm_cur,
                $1:currency::NUMBER AS currency,
                CAST($1:expensedenorm AS NUMBER(18,2)) AS expensedenorm,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:expensedenorm_cur::NUMBER AS expensedenorm_cur,
                CAST($1:revenuedenorm AS NUMBER(18,2)) AS revenuedenorm,
                $1:revenuedenorm_cur::NUMBER AS revenuedenorm_cur,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:negativewriteoffdenorm AS NUMBER(18,2)) AS negativewriteoffdenorm,
                CAST($1:billeddenorm AS NUMBER(18,2)) AS billeddenorm,
                $1:negativewriteoffdenorm_cur::NUMBER AS negativewriteoffdenorm_cur,
                $1:billeddenorm_cur::NUMBER AS billeddenorm_cur,
                CAST($1:writeoffexpensedenorm AS NUMBER(18,2)) AS writeoffexpensedenorm,
                $1:writeoffexpensedenorm_cur::NUMBER AS writeoffexpensedenorm_cur,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:duedenorm AS NUMBER(18,2)) AS duedenorm,
                $1:duedenorm_cur::NUMBER AS duedenorm_cur,
                CAST($1:reservedenorm AS NUMBER(18,2)) AS reservedenorm,
                $1:subtype::NUMBER AS subtype,
                $1:reservedenorm_cur::NUMBER AS reservedenorm_cur,
                CAST($1:unbilleddenorm AS NUMBER(18,2)) AS unbilleddenorm,
                $1:unbilleddenorm_cur::NUMBER AS unbilleddenorm_cur,
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
            FROM {{ source('gwbc', 'bc_taccountcontainer') }}
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
                        'publicid',
                        'createtime',
                        'unearneddenorm',
                        'unearneddenorm_cur',
                        'currency',
                        'expensedenorm',
                        'updatetime',
                        'expensedenorm_cur',
                        'revenuedenorm',
                        'revenuedenorm_cur',
                        'id',
                        'createuserid',
                        'negativewriteoffdenorm',
                        'billeddenorm',
                        'negativewriteoffdenorm_cur',
                        'billeddenorm_cur',
                        'writeoffexpensedenorm',
                        'writeoffexpensedenorm_cur',
                        'archivepartition',
                        'beanversion',
                        'updateuserid',
                        'duedenorm',
                        'duedenorm_cur',
                        'reservedenorm',
                        'subtype',
                        'reservedenorm_cur',
                        'unbilleddenorm',
                        'unbilleddenorm_cur'
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
        
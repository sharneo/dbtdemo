
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
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:BankRefDetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:WriteOffAmount AS NUMBER(18,2)) AS writeoffamount,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:FrozenByArchiving::BOOLEAN AS frozenbyarchiving,
                data_payload:WriteOffAmount_cur::NUMBER AS writeoffamount_cur,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:AppliedDate::NUMBER/1000) AS applieddate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:InvoiceNumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                TO_TIMESTAMP_TZ(data_payload:DistributedDate::NUMBER/1000) AS distributeddate,
                data_payload:Currency::NUMBER AS currency,
                data_payload:NetDistToInvoiceItems_cur::NUMBER AS netdisttoinvoiceitems_cur,
                CAST(data_payload:NetDistributedToInvoiceItems AS NUMBER(18,2)) AS netdistributedtoinvoiceitems,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ReversalDate::NUMBER/1000) AS reversaldate,
                CAST(data_payload:NetInSuspense AS NUMBER(18,2)) AS netinsuspense,
                data_payload:NetInSuspense_cur::NUMBER AS netinsuspense_cur,
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
            FROM {{ source('gwbc', 'bc_basedist') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:bankrefdetail_icare::TEXT AS VARCHAR(255)) AS bankrefdetail_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:writeoffamount AS NUMBER(18,2)) AS writeoffamount,
                $1:beanversion::NUMBER AS beanversion,
                $1:frozenbyarchiving::BOOLEAN AS frozenbyarchiving,
                $1:writeoffamount_cur::NUMBER AS writeoffamount_cur,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:applieddate::TIMESTAMP_TZ AS applieddate,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:invoicenumber_icare::TEXT AS VARCHAR(255)) AS invoicenumber_icare,
                $1:distributeddate::TIMESTAMP_TZ AS distributeddate,
                $1:currency::NUMBER AS currency,
                $1:netdisttoinvoiceitems_cur::NUMBER AS netdisttoinvoiceitems_cur,
                CAST($1:netdistributedtoinvoiceitems AS NUMBER(18,2)) AS netdistributedtoinvoiceitems,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:reversaldate::TIMESTAMP_TZ AS reversaldate,
                CAST($1:netinsuspense AS NUMBER(18,2)) AS netinsuspense,
                $1:netinsuspense_cur::NUMBER AS netinsuspense_cur,
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
            FROM {{ source('gwbc', 'bc_basedist') }}
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
                        'bankrefdetail_icare',
                        'publicid',
                        'writeoffamount',
                        'beanversion',
                        'frozenbyarchiving',
                        'writeoffamount_cur',
                        'retired',
                        'createtime',
                        'applieddate',
                        'updateuserid',
                        'invoicenumber_icare',
                        'distributeddate',
                        'currency',
                        'netdisttoinvoiceitems_cur',
                        'netdistributedtoinvoiceitems',
                        'updatetime',
                        'subtype',
                        'id',
                        'reversaldate',
                        'netinsuspense',
                        'netinsuspense_cur'
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
        
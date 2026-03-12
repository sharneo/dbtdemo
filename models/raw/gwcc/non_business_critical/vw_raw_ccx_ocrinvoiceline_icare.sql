
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
                CAST(data_payload:GSTAmount AS NUMBER(18,2)) AS gstamount,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:ServiceProviderID::TEXT AS VARCHAR(255)) AS serviceproviderid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                TO_TIMESTAMP_TZ(data_payload:Servicedate::NUMBER/1000) AS servicedate,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:PaymentLineAmountNet AS NUMBER(18,2)) AS paymentlineamountnet,
                CAST(data_payload:ABN::TEXT AS VARCHAR(255)) AS abn,
                CAST(data_payload:HICProviderID::TEXT AS VARCHAR(255)) AS hicproviderid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:OCRInvoice::NUMBER AS ocrinvoice,
                CAST(data_payload:ServiceProviderName::TEXT AS VARCHAR(255)) AS serviceprovidername,
                CAST(data_payload:PaymentLineAmountGross AS NUMBER(18,2)) AS paymentlineamountgross,
                data_payload:PaymentClassificationNumber::NUMBER AS paymentclassificationnumber,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:SequenceNumber::NUMBER AS sequencenumber,
                data_payload:Quantity::NUMBER AS quantity,
                CAST(data_payload:ServiceProviderCRMId::TEXT AS VARCHAR(255)) AS serviceprovidercrmid,
                CAST(data_payload:PaycodeDesc::TEXT AS VARCHAR(255)) AS paycodedesc,
                CAST(data_payload:MBSCode::TEXT AS VARCHAR(40)) AS mbscode,
                data_payload:AltCodeType::NUMBER AS altcodetype,
                CAST(data_payload:AltCode::TEXT AS VARCHAR(10)) AS altcode,
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
            FROM {{ source('gwcc', 'ccx_ocrinvoiceline_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:gstamount AS NUMBER(18,2)) AS gstamount,
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:serviceproviderid::TEXT AS VARCHAR(255)) AS serviceproviderid,
                $1:createuserid::NUMBER AS createuserid,
                $1:servicedate::TIMESTAMP_TZ AS servicedate,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:paymentlineamountnet AS NUMBER(18,2)) AS paymentlineamountnet,
                CAST($1:abn::TEXT AS VARCHAR(255)) AS abn,
                CAST($1:hicproviderid::TEXT AS VARCHAR(255)) AS hicproviderid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:ocrinvoice::NUMBER AS ocrinvoice,
                CAST($1:serviceprovidername::TEXT AS VARCHAR(255)) AS serviceprovidername,
                CAST($1:paymentlineamountgross AS NUMBER(18,2)) AS paymentlineamountgross,
                $1:paymentclassificationnumber::NUMBER AS paymentclassificationnumber,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:sequencenumber::NUMBER AS sequencenumber,
                $1:quantity::NUMBER AS quantity,
                CAST($1:serviceprovidercrmid::TEXT AS VARCHAR(255)) AS serviceprovidercrmid,
                CAST($1:paycodedesc::TEXT AS VARCHAR(255)) AS paycodedesc,
                CAST($1:mbscode::TEXT AS VARCHAR(40)) AS mbscode,
                $1:altcodetype::NUMBER AS altcodetype,
                CAST($1:altcode::TEXT AS VARCHAR(10)) AS altcode,
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
            FROM {{ source('gwcc', 'ccx_ocrinvoiceline_icare') }}
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
                                'gstamount',
                        'loadcommandid',
                        'serviceproviderid',
                        'createuserid',
                        'servicedate',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'paymentlineamountnet',
                        'abn',
                        'hicproviderid',
                        'updateuserid',
                        'ocrinvoice',
                        'serviceprovidername',
                        'paymentlineamountgross',
                        'paymentclassificationnumber',
                        'updatetime',
                        'id',
                        'sequencenumber',
                        'quantity',
                        'serviceprovidercrmid',
                        'paycodedesc',
                        'mbscode',
                        'altcodetype',
                        'altcode'
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
        
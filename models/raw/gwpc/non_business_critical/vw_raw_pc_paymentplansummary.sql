
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
    tags=["raw_gwpc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:Notes::TEXT AS VARCHAR(255)) AS notes,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:Total AS NUMBER(18,2)) AS total,
                data_payload:Total_cur::NUMBER AS total_cur,
                CAST(data_payload:Fee AS NUMBER(18,2)) AS fee,
                data_payload:Fee_cur::NUMBER AS fee_cur,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:Name::TEXT AS VARCHAR(255)) AS name,
                CAST(data_payload:ReportingPatternCode::TEXT AS VARCHAR(64)) AS reportingpatterncode,
                data_payload:InvoiceFrequency::NUMBER AS invoicefrequency,
                CAST(data_payload:BillingId::TEXT AS VARCHAR(255)) AS billingid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:DownPayment AS NUMBER(18,2)) AS downpayment,
                data_payload:DownPayment_cur::NUMBER AS downpayment_cur,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:PaymentPlanType::NUMBER AS paymentplantype,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:TotalFees AS NUMBER(18,2)) AS totalfees,
                data_payload:TotalFees_cur::NUMBER AS totalfees_cur,
                data_payload:PolicyPeriod::NUMBER AS policyperiod,
                CAST(data_payload:Installment AS NUMBER(18,2)) AS installment,
                data_payload:Installment_cur::NUMBER AS installment_cur,
                CAST(data_payload:Tax AS NUMBER(18,2)) AS tax,
                data_payload:Tax_cur::NUMBER AS tax_cur,
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
            FROM {{ source('gwpc', 'pc_paymentplansummary') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:notes::TEXT AS VARCHAR(255)) AS notes,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:total AS NUMBER(18,2)) AS total,
                $1:total_cur::NUMBER AS total_cur,
                CAST($1:fee AS NUMBER(18,2)) AS fee,
                $1:fee_cur::NUMBER AS fee_cur,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:name::TEXT AS VARCHAR(255)) AS name,
                CAST($1:reportingpatterncode::TEXT AS VARCHAR(64)) AS reportingpatterncode,
                $1:invoicefrequency::NUMBER AS invoicefrequency,
                CAST($1:billingid::TEXT AS VARCHAR(255)) AS billingid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:downpayment AS NUMBER(18,2)) AS downpayment,
                $1:downpayment_cur::NUMBER AS downpayment_cur,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:paymentplantype::NUMBER AS paymentplantype,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:totalfees AS NUMBER(18,2)) AS totalfees,
                $1:totalfees_cur::NUMBER AS totalfees_cur,
                $1:policyperiod::NUMBER AS policyperiod,
                CAST($1:installment AS NUMBER(18,2)) AS installment,
                $1:installment_cur::NUMBER AS installment_cur,
                CAST($1:tax AS NUMBER(18,2)) AS tax,
                $1:tax_cur::NUMBER AS tax_cur,
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
            FROM {{ source('gwpc', 'pc_paymentplansummary') }}
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
                                'notes',
                        'publicid',
                        'total',
                        'total_cur',
                        'fee',
                        'fee_cur',
                        'createtime',
                        'name',
                        'reportingpatterncode',
                        'invoicefrequency',
                        'billingid',
                        'updatetime',
                        'id',
                        'createuserid',
                        'downpayment',
                        'downpayment_cur',
                        'archivepartition',
                        'beanversion',
                        'paymentplantype',
                        'retired',
                        'updateuserid',
                        'totalfees',
                        'totalfees_cur',
                        'policyperiod',
                        'installment',
                        'installment_cur',
                        'tax',
                        'tax_cur'
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
        
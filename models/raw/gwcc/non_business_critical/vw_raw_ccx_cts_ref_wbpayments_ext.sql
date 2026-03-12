
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
                CAST(data_payload:Maximum::TEXT AS VARCHAR(255)) AS maximum,
                CAST(data_payload:TransAmount::TEXT AS VARCHAR(255)) AS transamount,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:WeeklyPayment::TEXT AS VARCHAR(255)) AS weeklypayment,
                CAST(data_payload:AWEUpperLimit::TEXT AS VARCHAR(255)) AS aweupperlimit,
                TO_TIMESTAMP_TZ(data_payload:ToDate::NUMBER/1000) AS todate,
                CAST(data_payload:Name::TEXT AS VARCHAR(50)) AS name,
                CAST(data_payload:Minimum::TEXT AS VARCHAR(255)) AS minimum,
                data_payload:Value::NUMBER AS value,
                CAST(data_payload:AdditionalChild::TEXT AS VARCHAR(255)) AS additionalchild,
                CAST(data_payload:Spouse::TEXT AS VARCHAR(255)) AS spouse,
                CAST(data_payload:Child1::TEXT AS VARCHAR(255)) AS child1,
                CAST(data_payload:Child2::TEXT AS VARCHAR(255)) AS child2,
                CAST(data_payload:Child3::TEXT AS VARCHAR(255)) AS child3,
                TO_TIMESTAMP_TZ(data_payload:FromDate::NUMBER/1000) AS fromdate,
                CAST(data_payload:Child4::TEXT AS VARCHAR(255)) AS child4,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
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
            FROM {{ source('gwcc', 'ccx_cts_ref_wbpayments_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:maximum::TEXT AS VARCHAR(255)) AS maximum,
                CAST($1:transamount::TEXT AS VARCHAR(255)) AS transamount,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:weeklypayment::TEXT AS VARCHAR(255)) AS weeklypayment,
                CAST($1:aweupperlimit::TEXT AS VARCHAR(255)) AS aweupperlimit,
                $1:todate::TIMESTAMP_TZ AS todate,
                CAST($1:name::TEXT AS VARCHAR(50)) AS name,
                CAST($1:minimum::TEXT AS VARCHAR(255)) AS minimum,
                $1:value::NUMBER AS value,
                CAST($1:additionalchild::TEXT AS VARCHAR(255)) AS additionalchild,
                CAST($1:spouse::TEXT AS VARCHAR(255)) AS spouse,
                CAST($1:child1::TEXT AS VARCHAR(255)) AS child1,
                CAST($1:child2::TEXT AS VARCHAR(255)) AS child2,
                CAST($1:child3::TEXT AS VARCHAR(255)) AS child3,
                $1:fromdate::TIMESTAMP_TZ AS fromdate,
                CAST($1:child4::TEXT AS VARCHAR(255)) AS child4,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
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
            FROM {{ source('gwcc', 'ccx_cts_ref_wbpayments_ext') }}
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
                        'maximum',
                        'transamount',
                        'publicid',
                        'weeklypayment',
                        'aweupperlimit',
                        'todate',
                        'name',
                        'minimum',
                        'value',
                        'additionalchild',
                        'spouse',
                        'child1',
                        'child2',
                        'child3',
                        'fromdate',
                        'child4',
                        'subtype',
                        'id'
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
        
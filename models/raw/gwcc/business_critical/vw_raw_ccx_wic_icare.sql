
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
                data_payload:CostCentreID::NUMBER AS costcentreid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:TariffRate AS NUMBER(10,4)) AS tariffrate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:PADescription::TEXT AS VARCHAR(255)) AS padescription,
                CAST(data_payload:Code::TEXT AS VARCHAR(60)) AS code,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PolicySystemId::TEXT AS VARCHAR(256)) AS policysystemid,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                CAST(data_payload:BusinessDescription::TEXT AS VARCHAR(500)) AS businessdescription,
                CAST(data_payload:PACode::TEXT AS VARCHAR(60)) AS pacode,
                data_payload:NoOfEmployers::NUMBER AS noofemployers,
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
            FROM {{ source('gwcc', 'ccx_wic_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:costcentreid::NUMBER AS costcentreid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:tariffrate AS NUMBER(10,4)) AS tariffrate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:padescription::TEXT AS VARCHAR(255)) AS padescription,
                CAST($1:code::TEXT AS VARCHAR(60)) AS code,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                CAST($1:policysystemid::TEXT AS VARCHAR(256)) AS policysystemid,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                CAST($1:businessdescription::TEXT AS VARCHAR(500)) AS businessdescription,
                CAST($1:pacode::TEXT AS VARCHAR(60)) AS pacode,
                $1:noofemployers::NUMBER AS noofemployers,
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
            FROM {{ source('gwcc', 'ccx_wic_icare') }}
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
                        'costcentreid',
                        'publicid',
                        'tariffrate',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'padescription',
                        'code',
                        'updateuserid',
                        'updatetime',
                        'subtype',
                        'id',
                        'policysystemid',
                        'description',
                        'businessdescription',
                        'pacode',
                        'noofemployers'
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
        
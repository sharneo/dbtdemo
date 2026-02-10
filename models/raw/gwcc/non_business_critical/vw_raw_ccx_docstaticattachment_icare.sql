
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
                CAST(data_payload:FileExtension::TEXT AS VARCHAR(255)) AS fileextension,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:FileName::TEXT AS VARCHAR(255)) AS filename,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:MimeType::TEXT AS VARCHAR(255)) AS mimetype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:DefaultSortOrder::NUMBER AS defaultsortorder,
                CAST(data_payload:DocId::TEXT AS VARCHAR(255)) AS docid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:DocumentID::NUMBER AS documentid,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:PhysicalFileName::TEXT AS VARCHAR(1333)) AS physicalfilename,
                data_payload:Required::BOOLEAN AS required,
                CAST(data_payload:DocumentPublicID::TEXT AS VARCHAR(64)) AS documentpublicid,
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
            FROM {{ source('gwcc', 'ccx_docstaticattachment_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:fileextension::TEXT AS VARCHAR(255)) AS fileextension,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:filename::TEXT AS VARCHAR(255)) AS filename,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:mimetype::TEXT AS VARCHAR(255)) AS mimetype,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:defaultsortorder::NUMBER AS defaultsortorder,
                CAST($1:docid::TEXT AS VARCHAR(255)) AS docid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:documentid::NUMBER AS documentid,
                $1:id::NUMBER AS id,
                CAST($1:physicalfilename::TEXT AS VARCHAR(1333)) AS physicalfilename,
                $1:required::BOOLEAN AS required,
                CAST($1:documentpublicid::TEXT AS VARCHAR(64)) AS documentpublicid,
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
            FROM {{ source('gwcc', 'ccx_docstaticattachment_icare') }}
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
                        'fileextension',
                        'publicid',
                        'filename',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'mimetype',
                        'updateuserid',
                        'defaultsortorder',
                        'docid',
                        'updatetime',
                        'documentid',
                        'id',
                        'physicalfilename',
                        'required',
                        'documentpublicid'
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
        
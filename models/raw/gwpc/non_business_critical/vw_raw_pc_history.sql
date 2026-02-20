
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
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:OriginalValue::TEXT AS VARCHAR(1333)) AS originalvalue,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:UserID::NUMBER AS userid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:AccountID::NUMBER AS accountid,
                data_payload:CustomType::NUMBER AS customtype,
                data_payload:PolicyID::NUMBER AS policyid,
                CAST(data_payload:NewValue::TEXT AS VARCHAR(1333)) AS newvalue,
                data_payload:PolicyPeriod::NUMBER AS policyperiod,
                CAST(data_payload:RuleUID::TEXT AS VARCHAR(255)) AS ruleuid,
                data_payload:PolicyTermID::NUMBER AS policytermid,
                data_payload:Type::NUMBER AS type,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Description::TEXT AS VARCHAR(1333)) AS description,
                TO_TIMESTAMP_TZ(data_payload:EventTimestamp::NUMBER/1000) AS eventtimestamp,
                data_payload:Job::NUMBER AS job,
                data_payload:Contact::NUMBER AS contact,
                data_payload:managingEntity::NUMBER AS managingentity,
                data_payload:BulkPolicyXfer::NUMBER AS bulkpolicyxfer,
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
            FROM {{ source('gwpc', 'pc_history') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:originalvalue::TEXT AS VARCHAR(1333)) AS originalvalue,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:userid::NUMBER AS userid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:accountid::NUMBER AS accountid,
                $1:customtype::NUMBER AS customtype,
                $1:policyid::NUMBER AS policyid,
                CAST($1:newvalue::TEXT AS VARCHAR(1333)) AS newvalue,
                $1:policyperiod::NUMBER AS policyperiod,
                CAST($1:ruleuid::TEXT AS VARCHAR(255)) AS ruleuid,
                $1:policytermid::NUMBER AS policytermid,
                $1:type::NUMBER AS type,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                CAST($1:description::TEXT AS VARCHAR(1333)) AS description,
                $1:eventtimestamp::TIMESTAMP_TZ AS eventtimestamp,
                $1:job::NUMBER AS job,
                $1:contact::NUMBER AS contact,
                $1:managingentity::NUMBER AS managingentity,
                $1:bulkpolicyxfer::NUMBER AS bulkpolicyxfer,
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
            FROM {{ source('gwpc', 'pc_history') }}
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
                        'originalvalue',
                        'publicid',
                        'userid',
                        'archivepartition',
                        'beanversion',
                        'accountid',
                        'customtype',
                        'policyid',
                        'newvalue',
                        'policyperiod',
                        'ruleuid',
                        'policytermid',
                        'type',
                        'subtype',
                        'id',
                        'description',
                        'eventtimestamp',
                        'job',
                        'contact',
                        'managingentity',
                        'bulkpolicyxfer'
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
        
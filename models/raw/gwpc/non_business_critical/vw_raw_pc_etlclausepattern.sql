
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CoverageCategory::TEXT AS VARCHAR(255)) AS coveragecategory,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:ClauseType::TEXT AS VARCHAR(255)) AS clausetype,
                CAST(data_payload:CoveredPartyType::TEXT AS VARCHAR(255)) AS coveredpartytype,
                CAST(data_payload:Name::TEXT AS VARCHAR(255)) AS name,
                CAST(data_payload:PatternID::TEXT AS VARCHAR(255)) AS patternid,
                CAST(data_payload:CoverageSubtype::TEXT AS VARCHAR(255)) AS coveragesubtype,
                CAST(data_payload:CodeIdentifier::TEXT AS VARCHAR(255)) AS codeidentifier,
                CAST(data_payload:OwningEntityType::TEXT AS VARCHAR(255)) AS owningentitytype,
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
            FROM {{ source('gwpc', 'pc_etlclausepattern') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:coveragecategory::TEXT AS VARCHAR(255)) AS coveragecategory,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:clausetype::TEXT AS VARCHAR(255)) AS clausetype,
                CAST($1:coveredpartytype::TEXT AS VARCHAR(255)) AS coveredpartytype,
                CAST($1:name::TEXT AS VARCHAR(255)) AS name,
                CAST($1:patternid::TEXT AS VARCHAR(255)) AS patternid,
                CAST($1:coveragesubtype::TEXT AS VARCHAR(255)) AS coveragesubtype,
                CAST($1:codeidentifier::TEXT AS VARCHAR(255)) AS codeidentifier,
                CAST($1:owningentitytype::TEXT AS VARCHAR(255)) AS owningentitytype,
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
            FROM {{ source('gwpc', 'pc_etlclausepattern') }}
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
                                'publicid',
                        'coveragecategory',
                        'beanversion',
                        'clausetype',
                        'coveredpartytype',
                        'name',
                        'patternid',
                        'coveragesubtype',
                        'codeidentifier',
                        'owningentitytype',
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
        
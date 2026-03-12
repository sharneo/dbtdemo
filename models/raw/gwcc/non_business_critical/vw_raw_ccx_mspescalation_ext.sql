
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
                TO_TIMESTAMP_TZ(data_payload:EscalationDate::NUMBER/1000) AS escalationdate,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:OtherRecommendation::NUMBER AS otherrecommendation,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:SpecialistID::NUMBER AS specialistid,
                data_payload:EscalationRecommendation::NUMBER AS escalationrecommendation,
                CAST(data_payload:ReviewSummary::TEXT AS VARCHAR(16777216)) AS reviewsummary,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Other::TEXT AS VARCHAR(255)) AS other,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ReferralID::NUMBER AS referralid,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:SpecialistName::TEXT AS VARCHAR(255)) AS specialistname,
                data_payload:EscalationStatus::NUMBER AS escalationstatus,
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
            FROM {{ source('gwcc', 'ccx_mspescalation_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:escalationdate::TIMESTAMP_TZ AS escalationdate,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:otherrecommendation::NUMBER AS otherrecommendation,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:specialistid::NUMBER AS specialistid,
                $1:escalationrecommendation::NUMBER AS escalationrecommendation,
                CAST($1:reviewsummary::TEXT AS VARCHAR(16777216)) AS reviewsummary,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:other::TEXT AS VARCHAR(255)) AS other,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:referralid::NUMBER AS referralid,
                $1:id::NUMBER AS id,
                CAST($1:specialistname::TEXT AS VARCHAR(255)) AS specialistname,
                $1:escalationstatus::NUMBER AS escalationstatus,
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
            FROM {{ source('gwcc', 'ccx_mspescalation_ext') }}
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
                        'escalationdate',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'otherrecommendation',
                        'retired',
                        'createtime',
                        'specialistid',
                        'escalationrecommendation',
                        'reviewsummary',
                        'updateuserid',
                        'other',
                        'updatetime',
                        'referralid',
                        'id',
                        'specialistname',
                        'escalationstatus'
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
        
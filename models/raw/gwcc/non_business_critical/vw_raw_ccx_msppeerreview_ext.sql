
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
                TO_TIMESTAMP_TZ(data_payload:ReviewCompletionDate::NUMBER/1000) AS reviewcompletiondate,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:OtherRecommendation::NUMBER AS otherrecommendation,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:SummaryAndRecommendation::TEXT AS VARCHAR(16777216)) AS summaryandrecommendation,
                CAST(data_payload:OtherRecommendationValues::TEXT AS VARCHAR(255)) AS otherrecommendationvalues,
                data_payload:SpecialistID::NUMBER AS specialistid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:PeerReviewRecommendation::NUMBER AS peerreviewrecommendation,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ReferralID::NUMBER AS referralid,
                data_payload:ID::NUMBER AS id,
                data_payload:PeerReviewStatus::NUMBER AS peerreviewstatus,
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
            FROM {{ source('gwcc', 'ccx_msppeerreview_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:reviewcompletiondate::TIMESTAMP_TZ AS reviewcompletiondate,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:otherrecommendation::NUMBER AS otherrecommendation,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:summaryandrecommendation::TEXT AS VARCHAR(16777216)) AS summaryandrecommendation,
                CAST($1:otherrecommendationvalues::TEXT AS VARCHAR(255)) AS otherrecommendationvalues,
                $1:specialistid::NUMBER AS specialistid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:peerreviewrecommendation::NUMBER AS peerreviewrecommendation,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:referralid::NUMBER AS referralid,
                $1:id::NUMBER AS id,
                $1:peerreviewstatus::NUMBER AS peerreviewstatus,
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
            FROM {{ source('gwcc', 'ccx_msppeerreview_ext') }}
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
                        'reviewcompletiondate',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'otherrecommendation',
                        'retired',
                        'createtime',
                        'summaryandrecommendation',
                        'otherrecommendationvalues',
                        'specialistid',
                        'updateuserid',
                        'peerreviewrecommendation',
                        'updatetime',
                        'referralid',
                        'id',
                        'peerreviewstatus'
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
        
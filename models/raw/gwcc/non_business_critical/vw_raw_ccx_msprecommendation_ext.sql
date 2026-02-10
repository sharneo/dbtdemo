
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
                data_payload:ReviewCompletionTime::NUMBER AS reviewcompletiontime,
                data_payload:MSPRecommendation::NUMBER AS msprecommendation,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CaseMgrContactedDate::NUMBER/1000) AS casemgrcontacteddate,
                data_payload:OtherRecommendation::NUMBER AS otherrecommendation,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:SpecialistID::NUMBER AS specialistid,
                CAST(data_payload:CaseSummaryForIME::TEXT AS VARCHAR(16777216)) AS casesummaryforime,
                CAST(data_payload:Other::TEXT AS VARCHAR(255)) AS other,
                data_payload:IsCaseManager::BOOLEAN AS iscasemanager,
                CAST(data_payload:QuestionsAddressedInIME::TEXT AS VARCHAR(16777216)) AS questionsaddressedinime,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:IMSName::TEXT AS VARCHAR(255)) AS imsname,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:IMSContactedDuration::TEXT AS VARCHAR(255)) AS imscontactedduration,
                TO_TIMESTAMP_TZ(data_payload:RecommendationDate::NUMBER/1000) AS recommendationdate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:SummaryAndRecommendation::TEXT AS VARCHAR(16777216)) AS summaryandrecommendation,
                CAST(data_payload:CaseMgrContactedDuration::TEXT AS VARCHAR(255)) AS casemgrcontactedduration,
                data_payload:IsInjMgmtSpecialist::BOOLEAN AS isinjmgmtspecialist,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:IMESpecialityRequired::TEXT AS VARCHAR(255)) AS imespecialityrequired,
                data_payload:ReferralID::NUMBER AS referralid,
                TO_TIMESTAMP_TZ(data_payload:IMSContactedDate::NUMBER/1000) AS imscontacteddate,
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
            FROM {{ source('gwcc', 'ccx_msprecommendation_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:reviewcompletiontime::NUMBER AS reviewcompletiontime,
                $1:msprecommendation::NUMBER AS msprecommendation,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:casemgrcontacteddate::TIMESTAMP_TZ AS casemgrcontacteddate,
                $1:otherrecommendation::NUMBER AS otherrecommendation,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:specialistid::NUMBER AS specialistid,
                CAST($1:casesummaryforime::TEXT AS VARCHAR(16777216)) AS casesummaryforime,
                CAST($1:other::TEXT AS VARCHAR(255)) AS other,
                $1:iscasemanager::BOOLEAN AS iscasemanager,
                CAST($1:questionsaddressedinime::TEXT AS VARCHAR(16777216)) AS questionsaddressedinime,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:imsname::TEXT AS VARCHAR(255)) AS imsname,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:imscontactedduration::TEXT AS VARCHAR(255)) AS imscontactedduration,
                $1:recommendationdate::TIMESTAMP_TZ AS recommendationdate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                CAST($1:summaryandrecommendation::TEXT AS VARCHAR(16777216)) AS summaryandrecommendation,
                CAST($1:casemgrcontactedduration::TEXT AS VARCHAR(255)) AS casemgrcontactedduration,
                $1:isinjmgmtspecialist::BOOLEAN AS isinjmgmtspecialist,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:imespecialityrequired::TEXT AS VARCHAR(255)) AS imespecialityrequired,
                $1:referralid::NUMBER AS referralid,
                $1:imscontacteddate::TIMESTAMP_TZ AS imscontacteddate,
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
            FROM {{ source('gwcc', 'ccx_msprecommendation_ext') }}
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
                        'reviewcompletiontime',
                        'msprecommendation',
                        'publicid',
                        'casemgrcontacteddate',
                        'otherrecommendation',
                        'createtime',
                        'specialistid',
                        'casesummaryforime',
                        'other',
                        'iscasemanager',
                        'questionsaddressedinime',
                        'updatetime',
                        'imsname',
                        'id',
                        'createuserid',
                        'imscontactedduration',
                        'recommendationdate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'summaryandrecommendation',
                        'casemgrcontactedduration',
                        'isinjmgmtspecialist',
                        'updateuserid',
                        'imespecialityrequired',
                        'referralid',
                        'imscontacteddate'
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
        
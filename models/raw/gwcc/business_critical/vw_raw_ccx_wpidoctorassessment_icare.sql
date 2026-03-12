
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:AssessmentDate_icare::NUMBER/1000) AS assessmentdate_icare,
                data_payload:AssessmentReport_icareID::NUMBER AS assessmentreport_icareid,
                data_payload:WPIInitiatedBy_icare::NUMBER AS wpiinitiatedby_icare,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ReportCorrect_icare::BOOLEAN AS reportcorrect_icare,
                data_payload:AssessingDoctor_icareID::NUMBER AS assessingdoctor_icareid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:Reviewer_icareID::NUMBER AS reviewer_icareid,
                data_payload:WPIAssessRecord_icareID::NUMBER AS wpiassessrecord_icareid,
                TO_TIMESTAMP_TZ(data_payload:ReportReceivedDate_icare::NUMBER/1000) AS reportreceiveddate_icare,
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
            FROM {{ source('gwcc', 'ccx_wpidoctorassessment_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:assessmentdate_icare::TIMESTAMP_TZ AS assessmentdate_icare,
                $1:assessmentreport_icareid::NUMBER AS assessmentreport_icareid,
                $1:wpiinitiatedby_icare::NUMBER AS wpiinitiatedby_icare,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:reportcorrect_icare::BOOLEAN AS reportcorrect_icare,
                $1:assessingdoctor_icareid::NUMBER AS assessingdoctor_icareid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:reviewer_icareid::NUMBER AS reviewer_icareid,
                $1:wpiassessrecord_icareid::NUMBER AS wpiassessrecord_icareid,
                $1:reportreceiveddate_icare::TIMESTAMP_TZ AS reportreceiveddate_icare,
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
            FROM {{ source('gwcc', 'ccx_wpidoctorassessment_icare') }}
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
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'createtime',
                        'assessmentdate_icare',
                        'assessmentreport_icareid',
                        'wpiinitiatedby_icare',
                        'updateuserid',
                        'reportcorrect_icare',
                        'assessingdoctor_icareid',
                        'updatetime',
                        'id',
                        'reviewer_icareid',
                        'wpiassessrecord_icareid',
                        'reportreceiveddate_icare'
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
        
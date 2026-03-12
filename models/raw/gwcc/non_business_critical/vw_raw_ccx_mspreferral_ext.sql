
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:SpecialistToReview::NUMBER AS specialisttoreview,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                data_payload:ReferralStatus::NUMBER AS referralstatus,
                CAST(data_payload:IssuesOrQuestions::TEXT AS VARCHAR(16777216)) AS issuesorquestions,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ReferralType::NUMBER AS referraltype,
                TO_TIMESTAMP_TZ(data_payload:ReferralSubmissionDate::NUMBER/1000) AS referralsubmissiondate,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:TreatmentReceived::BOOLEAN AS treatmentreceived,
                CAST(data_payload:ReasonWorkerNotInformed::TEXT AS VARCHAR(255)) AS reasonworkernotinformed,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:ReferralNumber::TEXT AS VARCHAR(255)) AS referralnumber,
                data_payload:MSPAdditionalInjury::BOOLEAN AS mspadditionalinjury,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ReferralInformedToMSP::BOOLEAN AS referralinformedtomsp,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:DateWorkerInformed::NUMBER/1000) AS dateworkerinformed,
                TO_TIMESTAMP_TZ(data_payload:LiabilityDueDate::NUMBER/1000) AS liabilityduedate,
                CAST(data_payload:InformationSummary::TEXT AS VARCHAR(16777216)) AS informationsummary,
                data_payload:LiabilityStatus::NUMBER AS liabilitystatus,
                CAST(data_payload:IMSName::TEXT AS VARCHAR(255)) AS imsname,
                data_payload:Priority::BOOLEAN AS priority,
                CAST(data_payload:IMSEmail::TEXT AS VARCHAR(80)) AS imsemail,
                CAST(data_payload:IMSPhone::TEXT AS VARCHAR(30)) AS imsphone,
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
            FROM {{ source('gwcc', 'ccx_mspreferral_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:specialisttoreview::NUMBER AS specialisttoreview,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                $1:referralstatus::NUMBER AS referralstatus,
                CAST($1:issuesorquestions::TEXT AS VARCHAR(16777216)) AS issuesorquestions,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:referraltype::NUMBER AS referraltype,
                $1:referralsubmissiondate::TIMESTAMP_TZ AS referralsubmissiondate,
                $1:claimid::NUMBER AS claimid,
                $1:treatmentreceived::BOOLEAN AS treatmentreceived,
                CAST($1:reasonworkernotinformed::TEXT AS VARCHAR(255)) AS reasonworkernotinformed,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:referralnumber::TEXT AS VARCHAR(255)) AS referralnumber,
                $1:mspadditionalinjury::BOOLEAN AS mspadditionalinjury,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:referralinformedtomsp::BOOLEAN AS referralinformedtomsp,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:dateworkerinformed::TIMESTAMP_TZ AS dateworkerinformed,
                $1:liabilityduedate::TIMESTAMP_TZ AS liabilityduedate,
                CAST($1:informationsummary::TEXT AS VARCHAR(16777216)) AS informationsummary,
                $1:liabilitystatus::NUMBER AS liabilitystatus,
                CAST($1:imsname::TEXT AS VARCHAR(255)) AS imsname,
                $1:priority::BOOLEAN AS priority,
                CAST($1:imsemail::TEXT AS VARCHAR(80)) AS imsemail,
                CAST($1:imsphone::TEXT AS VARCHAR(30)) AS imsphone,
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
            FROM {{ source('gwcc', 'ccx_mspreferral_ext') }}
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
                        'publicid',
                        'specialisttoreview',
                        'createtime',
                        'documentlinkableid',
                        'referralstatus',
                        'issuesorquestions',
                        'updatetime',
                        'referraltype',
                        'referralsubmissiondate',
                        'claimid',
                        'treatmentreceived',
                        'reasonworkernotinformed',
                        'id',
                        'createuserid',
                        'referralnumber',
                        'mspadditionalinjury',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'referralinformedtomsp',
                        'updateuserid',
                        'dateworkerinformed',
                        'liabilityduedate',
                        'informationsummary',
                        'liabilitystatus',
                        'imsname',
                        'priority',
                        'imsemail',
                        'imsphone'
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
        
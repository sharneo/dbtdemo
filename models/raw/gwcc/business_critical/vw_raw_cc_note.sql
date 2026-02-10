
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
                data_payload:Confidential::BOOLEAN AS confidential,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:CaseManagementServiceSubject::NUMBER AS casemanagementservicesubject,
                data_payload:RehabPlanServices_icareID::NUMBER AS rehabplanservices_icareid,
                data_payload:ServiceRequestID::NUMBER AS servicerequestid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:Language::NUMBER AS language,
                CAST(data_payload:PortalAuthorRealm_Ext::TEXT AS VARCHAR(45)) AS portalauthorrealm_ext,
                data_payload:Dispute_icareID::NUMBER AS dispute_icareid,
                data_payload:ID::NUMBER AS id,
                data_payload:MatterID::NUMBER AS matterid,
                data_payload:WorkCapacity_icareID::NUMBER AS workcapacity_icareid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ExposureID::NUMBER AS exposureid,
                CAST(data_payload:Body::TEXT AS VARCHAR(16777216)) AS body,
                TO_TIMESTAMP_TZ(data_payload:AuthoringDate::NUMBER/1000) AS authoringdate,
                data_payload:AuthorID::NUMBER AS authorid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ActivityID::NUMBER AS activityid,
                CAST(data_payload:Subject::TEXT AS VARCHAR(255)) AS subject,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Topic::NUMBER AS topic,
                CAST(data_payload:PortalAuthorUsername_Ext::TEXT AS VARCHAR(45)) AS portalauthorusername_ext,
                data_payload:SecurityType::NUMBER AS securitytype,
                data_payload:ClaimContactID::NUMBER AS claimcontactid,
                data_payload:MSPReferral_ExtID::NUMBER AS mspreferral_extid,
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
            FROM {{ source('gwcc', 'cc_note') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:confidential::BOOLEAN AS confidential,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:casemanagementservicesubject::NUMBER AS casemanagementservicesubject,
                $1:rehabplanservices_icareid::NUMBER AS rehabplanservices_icareid,
                $1:servicerequestid::NUMBER AS servicerequestid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:language::NUMBER AS language,
                CAST($1:portalauthorrealm_ext::TEXT AS VARCHAR(45)) AS portalauthorrealm_ext,
                $1:dispute_icareid::NUMBER AS dispute_icareid,
                $1:id::NUMBER AS id,
                $1:matterid::NUMBER AS matterid,
                $1:workcapacity_icareid::NUMBER AS workcapacity_icareid,
                $1:createuserid::NUMBER AS createuserid,
                $1:exposureid::NUMBER AS exposureid,
                CAST($1:body::TEXT AS VARCHAR(16777216)) AS body,
                $1:authoringdate::TIMESTAMP_TZ AS authoringdate,
                $1:authorid::NUMBER AS authorid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:activityid::NUMBER AS activityid,
                CAST($1:subject::TEXT AS VARCHAR(255)) AS subject,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:topic::NUMBER AS topic,
                CAST($1:portalauthorusername_ext::TEXT AS VARCHAR(45)) AS portalauthorusername_ext,
                $1:securitytype::NUMBER AS securitytype,
                $1:claimcontactid::NUMBER AS claimcontactid,
                $1:mspreferral_extid::NUMBER AS mspreferral_extid,
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
            FROM {{ source('gwcc', 'cc_note') }}
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
                        'confidential',
                        'createtime',
                        'casemanagementservicesubject',
                        'rehabplanservices_icareid',
                        'servicerequestid',
                        'updatetime',
                        'claimid',
                        'language',
                        'portalauthorrealm_ext',
                        'dispute_icareid',
                        'id',
                        'matterid',
                        'workcapacity_icareid',
                        'createuserid',
                        'exposureid',
                        'body',
                        'authoringdate',
                        'authorid',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'activityid',
                        'subject',
                        'updateuserid',
                        'topic',
                        'portalauthorusername_ext',
                        'securitytype',
                        'claimcontactid',
                        'mspreferral_extid'
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
        
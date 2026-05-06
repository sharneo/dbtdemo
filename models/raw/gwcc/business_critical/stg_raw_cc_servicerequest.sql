{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_servicerequest.
                                                servicerequest_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    transient=True,
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_servicerequest"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:HostABN_icare::TEXT AS VARCHAR(60)) AS hostabn_icare,
                data_payload:RequestedBy_icare::NUMBER AS requestedby_icare,
                data_payload:PreviousGroupID::NUMBER AS previousgroupid,
                CAST(data_payload:ServiceRequestNumber::TEXT AS VARCHAR(255)) AS servicerequestnumber,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:ExpectedServiceCompletionDate::NUMBER/1000) AS expectedservicecompletiondate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                data_payload:AssignedByUserID::NUMBER AS assignedbyuserid,
                data_payload:AssignedGroupID::NUMBER AS assignedgroupid,
                data_payload:SpecialistID::NUMBER AS specialistid,
                data_payload:SiraAccredRehabProvider_icare::NUMBER AS siraaccredrehabprovider_icare,
                TO_TIMESTAMP_TZ(data_payload:ExpectedQuoteCompletionDate::NUMBER/1000) AS expectedquotecompletiondate,
                data_payload:Currency::NUMBER AS currency,
                TO_TIMESTAMP_TZ(data_payload:RequestedServiceCompletionDate::NUMBER/1000) AS requestedservicecompletiondate,
                data_payload:Kind::NUMBER AS kind,
                data_payload:PreviousQueueID::NUMBER AS previousqueueid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                TO_TIMESTAMP_TZ(data_payload:RequestedQuoteCompletionDate::NUMBER/1000) AS requestedquotecompletiondate,
                CAST(data_payload:ServiceRequestReferenceNumber::TEXT AS VARCHAR(255)) AS servicerequestreferencenumber,
                data_payload:ID::NUMBER AS id,
                data_payload:PreviousUserID::NUMBER AS previoususerid,
                CAST(data_payload:CanceledReason::TEXT AS VARCHAR(16777216)) AS canceledreason,
                data_payload:AssignedQueueID::NUMBER AS assignedqueueid,
                data_payload:SpecialistCommMethod::NUMBER AS specialistcommmethod,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Tier::NUMBER AS tier,
                data_payload:ExposureID::NUMBER AS exposureid,
                TO_TIMESTAMP_TZ(data_payload:CloseDate::NUMBER/1000) AS closedate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:IncidentID::NUMBER AS incidentid,
                data_payload:Progress::NUMBER AS progress,
                data_payload:QuoteStatus::NUMBER AS quotestatus,
                data_payload:LinkedToCommonLawCase_icare::BOOLEAN AS linkedtocommonlawcase_icare,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:NullDate_icare::NUMBER/1000) AS nulldate_icare,
                data_payload:LinkToIMP_icare::BOOLEAN AS linktoimp_icare,
                data_payload:AssignedUserID::NUMBER AS assigneduserid,
                CAST(data_payload:Description_icare::TEXT AS VARCHAR(16777216)) AS description_icare,
                TO_TIMESTAMP_TZ(data_payload:AssignmentDate::NUMBER/1000) AS assignmentdate,
                TO_TIMESTAMP_TZ(data_payload:DateRequested_icare::NUMBER/1000) AS daterequested_icare,
                TO_TIMESTAMP_TZ(data_payload:LatestChangeTimestampDenorm::NUMBER/1000) AS latestchangetimestampdenorm,
                data_payload:AssignmentStatus::NUMBER AS assignmentstatus,
                CAST(data_payload:OtherOutcome_icare::TEXT AS VARCHAR(16777216)) AS otheroutcome_icare,
                data_payload:RequestOutcome_Ext::NUMBER AS requestoutcome_ext,
                CAST(data_payload:TotalApprovedAmount_Ext AS NUMBER(18,2)) AS totalapprovedamount_ext,
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
            FROM {{ source('gwcc', 'cc_servicerequest') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:hostabn_icare::TEXT AS VARCHAR(60)) AS hostabn_icare,
                $1:requestedby_icare::NUMBER AS requestedby_icare,
                $1:previousgroupid::NUMBER AS previousgroupid,
                CAST($1:servicerequestnumber::TEXT AS VARCHAR(255)) AS servicerequestnumber,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:expectedservicecompletiondate::TIMESTAMP_TZ AS expectedservicecompletiondate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                $1:assignedbyuserid::NUMBER AS assignedbyuserid,
                $1:assignedgroupid::NUMBER AS assignedgroupid,
                $1:specialistid::NUMBER AS specialistid,
                $1:siraaccredrehabprovider_icare::NUMBER AS siraaccredrehabprovider_icare,
                $1:expectedquotecompletiondate::TIMESTAMP_TZ AS expectedquotecompletiondate,
                $1:currency::NUMBER AS currency,
                $1:requestedservicecompletiondate::TIMESTAMP_TZ AS requestedservicecompletiondate,
                $1:kind::NUMBER AS kind,
                $1:previousqueueid::NUMBER AS previousqueueid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:requestedquotecompletiondate::TIMESTAMP_TZ AS requestedquotecompletiondate,
                CAST($1:servicerequestreferencenumber::TEXT AS VARCHAR(255)) AS servicerequestreferencenumber,
                $1:id::NUMBER AS id,
                $1:previoususerid::NUMBER AS previoususerid,
                CAST($1:canceledreason::TEXT AS VARCHAR(16777216)) AS canceledreason,
                $1:assignedqueueid::NUMBER AS assignedqueueid,
                $1:specialistcommmethod::NUMBER AS specialistcommmethod,
                $1:createuserid::NUMBER AS createuserid,
                $1:tier::NUMBER AS tier,
                $1:exposureid::NUMBER AS exposureid,
                $1:closedate::TIMESTAMP_TZ AS closedate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:incidentid::NUMBER AS incidentid,
                $1:progress::NUMBER AS progress,
                $1:quotestatus::NUMBER AS quotestatus,
                $1:linkedtocommonlawcase_icare::BOOLEAN AS linkedtocommonlawcase_icare,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:nulldate_icare::TIMESTAMP_TZ AS nulldate_icare,
                $1:linktoimp_icare::BOOLEAN AS linktoimp_icare,
                $1:assigneduserid::NUMBER AS assigneduserid,
                CAST($1:description_icare::TEXT AS VARCHAR(16777216)) AS description_icare,
                $1:assignmentdate::TIMESTAMP_TZ AS assignmentdate,
                $1:daterequested_icare::TIMESTAMP_TZ AS daterequested_icare,
                $1:latestchangetimestampdenorm::TIMESTAMP_TZ AS latestchangetimestampdenorm,
                $1:assignmentstatus::NUMBER AS assignmentstatus,
                CAST($1:otheroutcome_icare::TEXT AS VARCHAR(16777216)) AS otheroutcome_icare,
                $1:requestoutcome_ext::NUMBER AS requestoutcome_ext,
                CAST($1:totalapprovedamount_ext AS NUMBER(18,2)) AS totalapprovedamount_ext,
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
            FROM {{ source('gwcc', 'cc_servicerequest') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS servicerequest_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'hostabn_icare',
                        'requestedby_icare',
                        'previousgroupid',
                        'servicerequestnumber',
                        'publicid',
                        'expectedservicecompletiondate',
                        'createtime',
                        'documentlinkableid',
                        'assignedbyuserid',
                        'assignedgroupid',
                        'specialistid',
                        'siraaccredrehabprovider_icare',
                        'expectedquotecompletiondate',
                        'currency',
                        'requestedservicecompletiondate',
                        'kind',
                        'previousqueueid',
                        'updatetime',
                        'claimid',
                        'requestedquotecompletiondate',
                        'servicerequestreferencenumber',
                        'previoususerid',
                        'canceledreason',
                        'assignedqueueid',
                        'specialistcommmethod',
                        'createuserid',
                        'tier',
                        'exposureid',
                        'closedate',
                        'beanversion',
                        'archivepartition',
                        'incidentid',
                        'progress',
                        'quotestatus',
                        'linkedtocommonlawcase_icare',
                        'updateuserid',
                        'nulldate_icare',
                        'linktoimp_icare',
                        'assigneduserid',
                        'description_icare',
                        'assignmentdate',
                        'daterequested_icare',
                        'latestchangetimestampdenorm',
                        'assignmentstatus',
                        'otheroutcome_icare',
                        'requestoutcome_ext',
                        'totalapprovedamount_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}

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
                TO_TIMESTAMP_TZ(data_payload:DateRequested::NUMBER/1000) AS daterequested,
                CAST(data_payload:OtherOutcome::TEXT AS VARCHAR(16777216)) AS otheroutcome,
                CAST(data_payload:ServiceRequestNumber::TEXT AS VARCHAR(255)) AS servicerequestnumber,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:RequestedBy::NUMBER AS requestedby,
                TO_TIMESTAMP_TZ(data_payload:ExpectedServiceCompletionDate::NUMBER/1000) AS expectedservicecompletiondate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:SpecialistID::NUMBER AS specialistid,
                TO_TIMESTAMP_TZ(data_payload:ExpectedQuoteCompletionDate::NUMBER/1000) AS expectedquotecompletiondate,
                TO_TIMESTAMP_TZ(data_payload:RequestedServiceCompletionDate::NUMBER/1000) AS requestedservicecompletiondate,
                data_payload:Currency::NUMBER AS currency,
                data_payload:SiraAccredRehabProvider::NUMBER AS siraaccredrehabprovider,
                CAST(data_payload:TotalApprovedAmount AS NUMBER(18,2)) AS totalapprovedamount,
                data_payload:Kind::NUMBER AS kind,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                TO_TIMESTAMP_TZ(data_payload:RequestedQuoteCompletionDate::NUMBER/1000) AS requestedquotecompletiondate,
                CAST(data_payload:ServiceRequestReferenceNumber::TEXT AS VARCHAR(255)) AS servicerequestreferencenumber,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CanceledReason::TEXT AS VARCHAR(16777216)) AS canceledreason,
                data_payload:SpecialistCommMethod::NUMBER AS specialistcommmethod,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Tier::NUMBER AS tier,
                data_payload:RequestOutcome::NUMBER AS requestoutcome,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Progress::NUMBER AS progress,
                data_payload:QuoteStatus::NUMBER AS quotestatus,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:LinkedToCommonLawCase::BOOLEAN AS linkedtocommonlawcase,
                TO_TIMESTAMP_TZ(data_payload:NullDate::NUMBER/1000) AS nulldate,
                data_payload:LinkToIMP::BOOLEAN AS linktoimp,
                TO_TIMESTAMP_TZ(data_payload:LatestChangeTimestampDenorm::NUMBER/1000) AS latestchangetimestampdenorm,
                CAST(data_payload:Description::TEXT AS VARCHAR(16777216)) AS description,
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
            FROM {{ source('gwcc', 'ccx_dmservicerequest_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:daterequested::TIMESTAMP_TZ AS daterequested,
                CAST($1:otheroutcome::TEXT AS VARCHAR(16777216)) AS otheroutcome,
                CAST($1:servicerequestnumber::TEXT AS VARCHAR(255)) AS servicerequestnumber,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:requestedby::NUMBER AS requestedby,
                $1:expectedservicecompletiondate::TIMESTAMP_TZ AS expectedservicecompletiondate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:specialistid::NUMBER AS specialistid,
                $1:expectedquotecompletiondate::TIMESTAMP_TZ AS expectedquotecompletiondate,
                $1:requestedservicecompletiondate::TIMESTAMP_TZ AS requestedservicecompletiondate,
                $1:currency::NUMBER AS currency,
                $1:siraaccredrehabprovider::NUMBER AS siraaccredrehabprovider,
                CAST($1:totalapprovedamount AS NUMBER(18,2)) AS totalapprovedamount,
                $1:kind::NUMBER AS kind,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:requestedquotecompletiondate::TIMESTAMP_TZ AS requestedquotecompletiondate,
                CAST($1:servicerequestreferencenumber::TEXT AS VARCHAR(255)) AS servicerequestreferencenumber,
                $1:id::NUMBER AS id,
                CAST($1:canceledreason::TEXT AS VARCHAR(16777216)) AS canceledreason,
                $1:specialistcommmethod::NUMBER AS specialistcommmethod,
                $1:createuserid::NUMBER AS createuserid,
                $1:tier::NUMBER AS tier,
                $1:requestoutcome::NUMBER AS requestoutcome,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:progress::NUMBER AS progress,
                $1:quotestatus::NUMBER AS quotestatus,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:linkedtocommonlawcase::BOOLEAN AS linkedtocommonlawcase,
                $1:nulldate::TIMESTAMP_TZ AS nulldate,
                $1:linktoimp::BOOLEAN AS linktoimp,
                $1:latestchangetimestampdenorm::TIMESTAMP_TZ AS latestchangetimestampdenorm,
                CAST($1:description::TEXT AS VARCHAR(16777216)) AS description,
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
            FROM {{ source('gwcc', 'ccx_dmservicerequest_ext') }}
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
                        'daterequested',
                        'otheroutcome',
                        'servicerequestnumber',
                        'publicid',
                        'requestedby',
                        'expectedservicecompletiondate',
                        'createtime',
                        'specialistid',
                        'expectedquotecompletiondate',
                        'requestedservicecompletiondate',
                        'currency',
                        'siraaccredrehabprovider',
                        'totalapprovedamount',
                        'kind',
                        'updatetime',
                        'claimid',
                        'requestedquotecompletiondate',
                        'servicerequestreferencenumber',
                        'id',
                        'canceledreason',
                        'specialistcommmethod',
                        'createuserid',
                        'tier',
                        'requestoutcome',
                        'beanversion',
                        'archivepartition',
                        'progress',
                        'quotestatus',
                        'updateuserid',
                        'linkedtocommonlawcase',
                        'nulldate',
                        'linktoimp',
                        'latestchangetimestampdenorm',
                        'description'
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
        
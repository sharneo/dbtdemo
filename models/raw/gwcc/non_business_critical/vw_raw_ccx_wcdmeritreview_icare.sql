
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
                TO_TIMESTAMP_TZ(data_payload:DateApplicationReceived::NUMBER/1000) AS dateapplicationreceived,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ReviewDetailsID::NUMBER AS reviewdetailsid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:MeritReviewer::TEXT AS VARCHAR(255)) AS meritreviewer,
                TO_TIMESTAMP_TZ(data_payload:MeritReviewIssueDate::NUMBER/1000) AS meritreviewissuedate,
                data_payload:ApplicationLodgedBy::NUMBER AS applicationlodgedby,
                data_payload:SolicitorID::NUMBER AS solicitorid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:NewWCDRequired::BOOLEAN AS newwcdrequired,
                data_payload:ID::NUMBER AS id,
                data_payload:Decision43_1a_YesNo::BOOLEAN AS decision43_1a_yesno,
                CAST(data_payload:Decision43_1_Other::TEXT AS VARCHAR(255)) AS decision43_1_other,
                data_payload:Decision43_1b_YesNo::BOOLEAN AS decision43_1b_yesno,
                CAST(data_payload:Decision43_1d_i AS NUMBER(18,2)) AS decision43_1d_i,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:Decision43_1d_ii AS NUMBER(18,2)) AS decision43_1d_ii,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:FurtherInfoSubmitted::BOOLEAN AS furtherinfosubmitted,
                data_payload:Section54StillActive::BOOLEAN AS section54stillactive,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:ResponseDate::NUMBER/1000) AS responsedate,
                CAST(data_payload:Decision43_1a::TEXT AS VARCHAR(255)) AS decision43_1a,
                CAST(data_payload:Decision43_1b::TEXT AS VARCHAR(255)) AS decision43_1b,
                data_payload:ReviewRequestedWithin30Days::BOOLEAN AS reviewrequestedwithin30days,
                CAST(data_payload:Decision43_1c AS NUMBER(18,2)) AS decision43_1c,
                data_payload:StayApplicable::BOOLEAN AS stayapplicable,
                CAST(data_payload:Decision43_1e::TEXT AS VARCHAR(255)) AS decision43_1e,
                data_payload:FollowUpFlag::BOOLEAN AS followupflag,
                CAST(data_payload:Decision43_1f::TEXT AS VARCHAR(255)) AS decision43_1f,
                CAST(data_payload:CaseNumber::TEXT AS VARCHAR(255)) AS casenumber,
                TO_TIMESTAMP_TZ(data_payload:StayExpiryDate::NUMBER/1000) AS stayexpirydate,
                data_payload:ReviewOutcome::NUMBER AS reviewoutcome,
                data_payload:SectionS80StillActive::BOOLEAN AS sections80stillactive,
                CAST(data_payload:PIAWE AS NUMBER(18,2)) AS piawe,
                data_payload:WithinS80NoticePeriod::BOOLEAN AS withins80noticeperiod,
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
            FROM {{ source('gwcc', 'ccx_wcdmeritreview_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:dateapplicationreceived::TIMESTAMP_TZ AS dateapplicationreceived,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:reviewdetailsid::NUMBER AS reviewdetailsid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:meritreviewer::TEXT AS VARCHAR(255)) AS meritreviewer,
                $1:meritreviewissuedate::TIMESTAMP_TZ AS meritreviewissuedate,
                $1:applicationlodgedby::NUMBER AS applicationlodgedby,
                $1:solicitorid::NUMBER AS solicitorid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:newwcdrequired::BOOLEAN AS newwcdrequired,
                $1:id::NUMBER AS id,
                $1:decision43_1a_yesno::BOOLEAN AS decision43_1a_yesno,
                CAST($1:decision43_1_other::TEXT AS VARCHAR(255)) AS decision43_1_other,
                $1:decision43_1b_yesno::BOOLEAN AS decision43_1b_yesno,
                CAST($1:decision43_1d_i AS NUMBER(18,2)) AS decision43_1d_i,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:decision43_1d_ii AS NUMBER(18,2)) AS decision43_1d_ii,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:furtherinfosubmitted::BOOLEAN AS furtherinfosubmitted,
                $1:section54stillactive::BOOLEAN AS section54stillactive,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:responsedate::TIMESTAMP_TZ AS responsedate,
                CAST($1:decision43_1a::TEXT AS VARCHAR(255)) AS decision43_1a,
                CAST($1:decision43_1b::TEXT AS VARCHAR(255)) AS decision43_1b,
                $1:reviewrequestedwithin30days::BOOLEAN AS reviewrequestedwithin30days,
                CAST($1:decision43_1c AS NUMBER(18,2)) AS decision43_1c,
                $1:stayapplicable::BOOLEAN AS stayapplicable,
                CAST($1:decision43_1e::TEXT AS VARCHAR(255)) AS decision43_1e,
                $1:followupflag::BOOLEAN AS followupflag,
                CAST($1:decision43_1f::TEXT AS VARCHAR(255)) AS decision43_1f,
                CAST($1:casenumber::TEXT AS VARCHAR(255)) AS casenumber,
                $1:stayexpirydate::TIMESTAMP_TZ AS stayexpirydate,
                $1:reviewoutcome::NUMBER AS reviewoutcome,
                $1:sections80stillactive::BOOLEAN AS sections80stillactive,
                CAST($1:piawe AS NUMBER(18,2)) AS piawe,
                $1:withins80noticeperiod::BOOLEAN AS withins80noticeperiod,
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
            FROM {{ source('gwcc', 'ccx_wcdmeritreview_icare') }}
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
                        'dateapplicationreceived',
                        'publicid',
                        'reviewdetailsid',
                        'createtime',
                        'meritreviewer',
                        'meritreviewissuedate',
                        'applicationlodgedby',
                        'solicitorid',
                        'updatetime',
                        'newwcdrequired',
                        'id',
                        'decision43_1a_yesno',
                        'decision43_1_other',
                        'decision43_1b_yesno',
                        'decision43_1d_i',
                        'createuserid',
                        'beanversion',
                        'decision43_1d_ii',
                        'archivepartition',
                        'retired',
                        'furtherinfosubmitted',
                        'section54stillactive',
                        'updateuserid',
                        'responsedate',
                        'decision43_1a',
                        'decision43_1b',
                        'reviewrequestedwithin30days',
                        'decision43_1c',
                        'stayapplicable',
                        'decision43_1e',
                        'followupflag',
                        'decision43_1f',
                        'casenumber',
                        'stayexpirydate',
                        'reviewoutcome',
                        'sections80stillactive',
                        'piawe',
                        'withins80noticeperiod'
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
        
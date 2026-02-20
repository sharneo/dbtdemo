
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
                CAST(data_payload:LossRatio AS NUMBER(8,2)) AS lossratio,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:MostRecentTerm::BOOLEAN AS mostrecentterm,
                data_payload:PolicyID::NUMBER AS policyid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:GenerateReinsurables::BOOLEAN AS generatereinsurables,
                data_payload:FinalAuditOption::NUMBER AS finalauditoption,
                data_payload:ID::NUMBER AS id,
                data_payload:DepositReleased::BOOLEAN AS depositreleased,
                CAST(data_payload:TotalEstimatedPremium AS NUMBER(18,2)) AS totalestimatedpremium,
                data_payload:TotalEstimatedPremium_cur::NUMBER AS totalestimatedpremium_cur,
                CAST(data_payload:TotalReportedPremium AS NUMBER(18,2)) AS totalreportedpremium,
                data_payload:TotalReportedPremium_cur::NUMBER AS totalreportedpremium_cur,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:NonRenewReason::NUMBER AS nonrenewreason,
                TO_TIMESTAMP_TZ(data_payload:LastRestoreDate::NUMBER/1000) AS lastrestoredate,
                TO_TIMESTAMP_TZ(data_payload:NextArchiveCheckDate::NUMBER/1000) AS nextarchivecheckdate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:PreRenewalDirection::NUMBER AS prerenewaldirection,
                data_payload:DaysReported::NUMBER AS daysreported,
                data_payload:PolicyTermArchiveState::NUMBER AS policytermarchivestate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:LossRatioCalculationDate::NUMBER/1000) AS lossratiocalculationdate,
                data_payload:ClaimSystemTotalIncurred_cur::NUMBER AS claimsystemtotalincurred_cur,
                TO_TIMESTAMP_TZ(data_payload:NextRenewalCheckDate::NUMBER/1000) AS nextrenewalcheckdate,
                data_payload:AffinityGroupID::NUMBER AS affinitygroupid,
                CAST(data_payload:DepositAmount AS NUMBER(18,2)) AS depositamount,
                data_payload:DepositAmount_cur::NUMBER AS depositamount_cur,
                CAST(data_payload:NonRenewAddExplanation::TEXT AS VARCHAR(255)) AS nonrenewaddexplanation,
                CAST(data_payload:ClaimSystemTotalIncurred_amt AS NUMBER(18,2)) AS claimsystemtotalincurred_amt,
                data_payload:Bound::BOOLEAN AS bound,
                data_payload:NilAdjustFailed_icare::BOOLEAN AS niladjustfailed_icare,
                data_payload:ExcludedFromArchive::BOOLEAN AS excludedfromarchive,
                data_payload:ArchiveState::NUMBER AS archivestate,
                data_payload:ArchiveSchemaInfo::NUMBER AS archiveschemainfo,
                data_payload:DoNotDestroy::BOOLEAN AS donotdestroy,
                data_payload:ArchiveFailureDetailsID::NUMBER AS archivefailuredetailsid,
                CAST(data_payload:ExcludeReason::TEXT AS VARCHAR(255)) AS excludereason,
                data_payload:ArchiveFailureID::NUMBER AS archivefailureid,
                TO_TIMESTAMP_TZ(data_payload:ArchiveDate::NUMBER/1000) AS archivedate,
                data_payload:NilAdjustTermStatus_icare::NUMBER AS niladjusttermstatus_icare,
                data_payload:PreRenewalDirectionQueue_Ext::NUMBER AS prerenewaldirectionqueue_ext,
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
            FROM {{ source('gwpc', 'pc_policyterm') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:lossratio AS NUMBER(8,2)) AS lossratio,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:mostrecentterm::BOOLEAN AS mostrecentterm,
                $1:policyid::NUMBER AS policyid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:generatereinsurables::BOOLEAN AS generatereinsurables,
                $1:finalauditoption::NUMBER AS finalauditoption,
                $1:id::NUMBER AS id,
                $1:depositreleased::BOOLEAN AS depositreleased,
                CAST($1:totalestimatedpremium AS NUMBER(18,2)) AS totalestimatedpremium,
                $1:totalestimatedpremium_cur::NUMBER AS totalestimatedpremium_cur,
                CAST($1:totalreportedpremium AS NUMBER(18,2)) AS totalreportedpremium,
                $1:totalreportedpremium_cur::NUMBER AS totalreportedpremium_cur,
                $1:createuserid::NUMBER AS createuserid,
                $1:nonrenewreason::NUMBER AS nonrenewreason,
                $1:lastrestoredate::TIMESTAMP_TZ AS lastrestoredate,
                $1:nextarchivecheckdate::TIMESTAMP_TZ AS nextarchivecheckdate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:prerenewaldirection::NUMBER AS prerenewaldirection,
                $1:daysreported::NUMBER AS daysreported,
                $1:policytermarchivestate::NUMBER AS policytermarchivestate,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:lossratiocalculationdate::TIMESTAMP_TZ AS lossratiocalculationdate,
                $1:claimsystemtotalincurred_cur::NUMBER AS claimsystemtotalincurred_cur,
                $1:nextrenewalcheckdate::TIMESTAMP_TZ AS nextrenewalcheckdate,
                $1:affinitygroupid::NUMBER AS affinitygroupid,
                CAST($1:depositamount AS NUMBER(18,2)) AS depositamount,
                $1:depositamount_cur::NUMBER AS depositamount_cur,
                CAST($1:nonrenewaddexplanation::TEXT AS VARCHAR(255)) AS nonrenewaddexplanation,
                CAST($1:claimsystemtotalincurred_amt AS NUMBER(18,2)) AS claimsystemtotalincurred_amt,
                $1:bound::BOOLEAN AS bound,
                $1:niladjustfailed_icare::BOOLEAN AS niladjustfailed_icare,
                $1:excludedfromarchive::BOOLEAN AS excludedfromarchive,
                $1:archivestate::NUMBER AS archivestate,
                $1:archiveschemainfo::NUMBER AS archiveschemainfo,
                $1:donotdestroy::BOOLEAN AS donotdestroy,
                $1:archivefailuredetailsid::NUMBER AS archivefailuredetailsid,
                CAST($1:excludereason::TEXT AS VARCHAR(255)) AS excludereason,
                $1:archivefailureid::NUMBER AS archivefailureid,
                $1:archivedate::TIMESTAMP_TZ AS archivedate,
                $1:niladjusttermstatus_icare::NUMBER AS niladjusttermstatus_icare,
                $1:prerenewaldirectionqueue_ext::NUMBER AS prerenewaldirectionqueue_ext,
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
            FROM {{ source('gwpc', 'pc_policyterm') }}
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
                        'lossratio',
                        'createtime',
                        'mostrecentterm',
                        'policyid',
                        'updatetime',
                        'generatereinsurables',
                        'finalauditoption',
                        'id',
                        'depositreleased',
                        'totalestimatedpremium',
                        'totalestimatedpremium_cur',
                        'totalreportedpremium',
                        'totalreportedpremium_cur',
                        'createuserid',
                        'nonrenewreason',
                        'lastrestoredate',
                        'nextarchivecheckdate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'prerenewaldirection',
                        'daysreported',
                        'policytermarchivestate',
                        'updateuserid',
                        'lossratiocalculationdate',
                        'claimsystemtotalincurred_cur',
                        'nextrenewalcheckdate',
                        'affinitygroupid',
                        'depositamount',
                        'depositamount_cur',
                        'nonrenewaddexplanation',
                        'claimsystemtotalincurred_amt',
                        'bound',
                        'niladjustfailed_icare',
                        'excludedfromarchive',
                        'archivestate',
                        'archiveschemainfo',
                        'donotdestroy',
                        'archivefailuredetailsid',
                        'excludereason',
                        'archivefailureid',
                        'archivedate',
                        'niladjusttermstatus_icare',
                        'prerenewaldirectionqueue_ext'
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
        

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
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:LossHistoryEntry::NUMBER AS losshistoryentry,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(60)) AS claimnumber,
                CAST(data_payload:OverwrittenClaimsCost AS NUMBER(18,2)) AS overwrittenclaimscost,
                data_payload:OverwrittenClaimsCost_cur::NUMBER AS overwrittenclaimscost_cur,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:PolicyID::NUMBER AS policyid,
                data_payload:PolicyPeriodID::NUMBER AS policyperiodid,
                CAST(data_payload:TotalPaid AS NUMBER(18,2)) AS totalpaid,
                TO_TIMESTAMP_TZ(data_payload:LossDate::NUMBER/1000) AS lossdate,
                data_payload:TotalPaid_cur::NUMBER AS totalpaid_cur,
                CAST(data_payload:RTWI AS NUMBER(18,2)) AS rtwi,
                data_payload:RTWI_cur::NUMBER AS rtwi_cur,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                CAST(data_payload:CPRClaimsCost AS NUMBER(18,2)) AS cprclaimscost,
                data_payload:CPRClaimsCost_cur::NUMBER AS cprclaimscost_cur,
                data_payload:DirectWageID::NUMBER AS directwageid,
                data_payload:ClaimStatus::NUMBER AS claimstatus,
                data_payload:RTWIPercentage::NUMBER AS rtwipercentage,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:ClaimantName::TEXT AS VARCHAR(60)) AS claimantname,
                data_payload:RecoveriesOrEstimates_cur::NUMBER AS recoveriesorestimates_cur,
                CAST(data_payload:RecoveriesOrEstimates_amt AS NUMBER(18,2)) AS recoveriesorestimates_amt,
                data_payload:PolicyTermID::NUMBER AS policytermid,
                data_payload:isCZeroClaim::BOOLEAN AS isczeroclaim,
                data_payload:isExternalData::BOOLEAN AS isexternaldata,
                data_payload:AddedToPolicyPeriodTerm::NUMBER AS addedtopolicyperiodterm,
                data_payload:Estimates_cur::NUMBER AS estimates_cur,
                CAST(data_payload:Estimates_amt AS NUMBER(18,2)) AS estimates_amt,
                data_payload:isClaimCenterClaim::BOOLEAN AS isclaimcenterclaim,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(60)) AS policynumber,
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
            FROM {{ source('gwpc', 'pcx_claim_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:losshistoryentry::NUMBER AS losshistoryentry,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:claimnumber::TEXT AS VARCHAR(60)) AS claimnumber,
                CAST($1:overwrittenclaimscost AS NUMBER(18,2)) AS overwrittenclaimscost,
                $1:overwrittenclaimscost_cur::NUMBER AS overwrittenclaimscost_cur,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:policyid::NUMBER AS policyid,
                $1:policyperiodid::NUMBER AS policyperiodid,
                CAST($1:totalpaid AS NUMBER(18,2)) AS totalpaid,
                $1:lossdate::TIMESTAMP_TZ AS lossdate,
                $1:totalpaid_cur::NUMBER AS totalpaid_cur,
                CAST($1:rtwi AS NUMBER(18,2)) AS rtwi,
                $1:rtwi_cur::NUMBER AS rtwi_cur,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                CAST($1:cprclaimscost AS NUMBER(18,2)) AS cprclaimscost,
                $1:cprclaimscost_cur::NUMBER AS cprclaimscost_cur,
                $1:directwageid::NUMBER AS directwageid,
                $1:claimstatus::NUMBER AS claimstatus,
                $1:rtwipercentage::NUMBER AS rtwipercentage,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:claimantname::TEXT AS VARCHAR(60)) AS claimantname,
                $1:recoveriesorestimates_cur::NUMBER AS recoveriesorestimates_cur,
                CAST($1:recoveriesorestimates_amt AS NUMBER(18,2)) AS recoveriesorestimates_amt,
                $1:policytermid::NUMBER AS policytermid,
                $1:isczeroclaim::BOOLEAN AS isczeroclaim,
                $1:isexternaldata::BOOLEAN AS isexternaldata,
                $1:addedtopolicyperiodterm::NUMBER AS addedtopolicyperiodterm,
                $1:estimates_cur::NUMBER AS estimates_cur,
                CAST($1:estimates_amt AS NUMBER(18,2)) AS estimates_amt,
                $1:isclaimcenterclaim::BOOLEAN AS isclaimcenterclaim,
                CAST($1:policynumber::TEXT AS VARCHAR(60)) AS policynumber,
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
            FROM {{ source('gwpc', 'pcx_claim_icare') }}
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
                        'losshistoryentry',
                        'publicid',
                        'claimnumber',
                        'overwrittenclaimscost',
                        'overwrittenclaimscost_cur',
                        'createtime',
                        'policyid',
                        'policyperiodid',
                        'totalpaid',
                        'lossdate',
                        'totalpaid_cur',
                        'rtwi',
                        'rtwi_cur',
                        'updatetime',
                        'id',
                        'createuserid',
                        'beanversion',
                        'archivepartition',
                        'cprclaimscost',
                        'cprclaimscost_cur',
                        'directwageid',
                        'claimstatus',
                        'rtwipercentage',
                        'updateuserid',
                        'claimantname',
                        'recoveriesorestimates_cur',
                        'recoveriesorestimates_amt',
                        'policytermid',
                        'isczeroclaim',
                        'isexternaldata',
                        'addedtopolicyperiodterm',
                        'estimates_cur',
                        'estimates_amt',
                        'isclaimcenterclaim',
                        'policynumber'
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
        
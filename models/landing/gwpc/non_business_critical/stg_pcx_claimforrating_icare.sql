{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_claimforrating_icare.
                                                claimforrating_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_claimforrating_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:EffectiveDatedFieldsID::NUMBER AS effectivedatedfieldsid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:ClaimNumber::TEXT AS VARCHAR(60)) AS claimnumber,
                CAST(data_payload:OverwrittenClaimsCost AS NUMBER(18,2)) AS overwrittenclaimscost,
                data_payload:OverwrittenClaimsCost_cur::NUMBER AS overwrittenclaimscost_cur,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:FixedID::NUMBER AS fixedid,
                CAST(data_payload:TotalPaid AS NUMBER(18,2)) AS totalpaid,
                data_payload:CZeroClaim::BOOLEAN AS czeroclaim,
                TO_TIMESTAMP_TZ(data_payload:LossDate::NUMBER/1000) AS lossdate,
                data_payload:TotalPaid_cur::NUMBER AS totalpaid_cur,
                CAST(data_payload:RTWI AS NUMBER(18,2)) AS rtwi,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                data_payload:RTWI_cur::NUMBER AS rtwi_cur,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:LossHistoryEntryID::NUMBER AS losshistoryentryid,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ClaimOnPolicyID::NUMBER AS claimonpolicyid,
                CAST(data_payload:CPRClaimsCost AS NUMBER(18,2)) AS cprclaimscost,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:CPRClaimsCost_cur::NUMBER AS cprclaimscost_cur,
                data_payload:DirectWageID::NUMBER AS directwageid,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:ClaimStatus::NUMBER AS claimstatus,
                data_payload:RTWIPercentage::NUMBER AS rtwipercentage,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ExternalData::BOOLEAN AS externaldata,
                data_payload:RecoveriesOrEstimates_cur::NUMBER AS recoveriesorestimates_cur,
                CAST(data_payload:ClaimantName::TEXT AS VARCHAR(60)) AS claimantname,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:BranchID::NUMBER AS branchid,
                CAST(data_payload:RecoveriesOrEstimates_amt AS NUMBER(18,2)) AS recoveriesorestimates_amt,
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
                'AVRO' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_claimforrating_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:effectivedatedfieldsid::NUMBER AS effectivedatedfieldsid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:claimnumber::TEXT AS VARCHAR(60)) AS claimnumber,
                CAST($1:overwrittenclaimscost AS NUMBER(18,2)) AS overwrittenclaimscost,
                $1:overwrittenclaimscost_cur::NUMBER AS overwrittenclaimscost_cur,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:fixedid::NUMBER AS fixedid,
                CAST($1:totalpaid AS NUMBER(18,2)) AS totalpaid,
                $1:czeroclaim::BOOLEAN AS czeroclaim,
                $1:lossdate::TIMESTAMP_TZ AS lossdate,
                $1:totalpaid_cur::NUMBER AS totalpaid_cur,
                CAST($1:rtwi AS NUMBER(18,2)) AS rtwi,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:rtwi_cur::NUMBER AS rtwi_cur,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:losshistoryentryid::NUMBER AS losshistoryentryid,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:claimonpolicyid::NUMBER AS claimonpolicyid,
                CAST($1:cprclaimscost AS NUMBER(18,2)) AS cprclaimscost,
                $1:changetype::NUMBER AS changetype,
                $1:cprclaimscost_cur::NUMBER AS cprclaimscost_cur,
                $1:directwageid::NUMBER AS directwageid,
                $1:basedonid::NUMBER AS basedonid,
                $1:claimstatus::NUMBER AS claimstatus,
                $1:rtwipercentage::NUMBER AS rtwipercentage,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:externaldata::BOOLEAN AS externaldata,
                $1:recoveriesorestimates_cur::NUMBER AS recoveriesorestimates_cur,
                CAST($1:claimantname::TEXT AS VARCHAR(60)) AS claimantname,
                $1:subtype::NUMBER AS subtype,
                $1:branchid::NUMBER AS branchid,
                CAST($1:recoveriesorestimates_amt AS NUMBER(18,2)) AS recoveriesorestimates_amt,
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
                'PARQUET' as file_type,
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_claimforrating_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS claimforrating_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'effectivedatedfieldsid',
                        'publicid',
                        'claimnumber',
                        'overwrittenclaimscost',
                        'overwrittenclaimscost_cur',
                        'createtime',
                        'fixedid',
                        'totalpaid',
                        'czeroclaim',
                        'lossdate',
                        'totalpaid_cur',
                        'rtwi',
                        'effectivedate',
                        'rtwi_cur',
                        'updatetime',
                        'losshistoryentryid',
                        'expirationdate',
                        'createuserid',
                        'archivepartition',
                        'beanversion',
                        'claimonpolicyid',
                        'cprclaimscost',
                        'changetype',
                        'cprclaimscost_cur',
                        'directwageid',
                        'basedonid',
                        'claimstatus',
                        'rtwipercentage',
                        'updateuserid',
                        'externaldata',
                        'recoveriesorestimates_cur',
                        'claimantname',
                        'subtype',
                        'branchid',
                        'recoveriesorestimates_amt',
                        'estimates_cur',
                        'estimates_amt',
                        'isclaimcenterclaim',
                        'policynumber'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
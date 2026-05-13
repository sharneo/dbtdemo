{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_losshistforrating_icare.
                                                losshistforrating_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_losshistforrating_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:LocationName::TEXT AS VARCHAR(500)) AS locationname,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:EffectiveDatedFieldsID::NUMBER AS effectivedatedfieldsid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:Wages1 AS NUMBER(18,2)) AS wages1,
                CAST(data_payload:Wages2 AS NUMBER(18,2)) AS wages2,
                data_payload:Wages1_cur::NUMBER AS wages1_cur,
                CAST(data_payload:Wages3 AS NUMBER(18,2)) AS wages3,
                data_payload:Wages2_cur::NUMBER AS wages2_cur,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Wages3_cur::NUMBER AS wages3_cur,
                data_payload:FixedID::NUMBER AS fixedid,
                data_payload:isLossAdded::BOOLEAN AS islossadded,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:ClaimYears1 AS NUMBER(18,2)) AS claimyears1,
                data_payload:ClaimYears1_cur::NUMBER AS claimyears1_cur,
                CAST(data_payload:ClaimYears2 AS NUMBER(18,2)) AS claimyears2,
                data_payload:ClaimYears2_cur::NUMBER AS claimyears2_cur,
                CAST(data_payload:ClaimYears3 AS NUMBER(18,2)) AS claimyears3,
                data_payload:ClaimYears3_cur::NUMBER AS claimyears3_cur,
                data_payload:ID::NUMBER AS id,
                data_payload:LossHistoryEntryID::NUMBER AS losshistoryentryid,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:CostCenterNo::TEXT AS VARCHAR(60)) AS costcenterno,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:DirectWageID::NUMBER AS directwageid,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:BTPYear1 AS NUMBER(18,2)) AS btpyear1,
                data_payload:BTPYear1_cur::NUMBER AS btpyear1_cur,
                CAST(data_payload:BTPYear2 AS NUMBER(18,2)) AS btpyear2,
                CAST(data_payload:BTPYear3 AS NUMBER(18,2)) AS btpyear3,
                data_payload:BTPYear2_cur::NUMBER AS btpyear2_cur,
                data_payload:BTPYear3_cur::NUMBER AS btpyear3_cur,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:NumOfPerCapUnit1::NUMBER AS numofpercapunit1,
                data_payload:NumOfPerCapUnit2::NUMBER AS numofpercapunit2,
                data_payload:BranchID::NUMBER AS branchid,
                data_payload:NumOfPerCapUnit3::NUMBER AS numofpercapunit3,
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
            FROM {{ source('gwpc', 'pcx_losshistforrating_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:locationname::TEXT AS VARCHAR(500)) AS locationname,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:effectivedatedfieldsid::NUMBER AS effectivedatedfieldsid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:wages1 AS NUMBER(18,2)) AS wages1,
                CAST($1:wages2 AS NUMBER(18,2)) AS wages2,
                $1:wages1_cur::NUMBER AS wages1_cur,
                CAST($1:wages3 AS NUMBER(18,2)) AS wages3,
                $1:wages2_cur::NUMBER AS wages2_cur,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:wages3_cur::NUMBER AS wages3_cur,
                $1:fixedid::NUMBER AS fixedid,
                $1:islossadded::BOOLEAN AS islossadded,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:claimyears1 AS NUMBER(18,2)) AS claimyears1,
                $1:claimyears1_cur::NUMBER AS claimyears1_cur,
                CAST($1:claimyears2 AS NUMBER(18,2)) AS claimyears2,
                $1:claimyears2_cur::NUMBER AS claimyears2_cur,
                CAST($1:claimyears3 AS NUMBER(18,2)) AS claimyears3,
                $1:claimyears3_cur::NUMBER AS claimyears3_cur,
                $1:id::NUMBER AS id,
                $1:losshistoryentryid::NUMBER AS losshistoryentryid,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:costcenterno::TEXT AS VARCHAR(60)) AS costcenterno,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:changetype::NUMBER AS changetype,
                $1:directwageid::NUMBER AS directwageid,
                $1:basedonid::NUMBER AS basedonid,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:btpyear1 AS NUMBER(18,2)) AS btpyear1,
                $1:btpyear1_cur::NUMBER AS btpyear1_cur,
                CAST($1:btpyear2 AS NUMBER(18,2)) AS btpyear2,
                CAST($1:btpyear3 AS NUMBER(18,2)) AS btpyear3,
                $1:btpyear2_cur::NUMBER AS btpyear2_cur,
                $1:btpyear3_cur::NUMBER AS btpyear3_cur,
                $1:subtype::NUMBER AS subtype,
                $1:numofpercapunit1::NUMBER AS numofpercapunit1,
                $1:numofpercapunit2::NUMBER AS numofpercapunit2,
                $1:branchid::NUMBER AS branchid,
                $1:numofpercapunit3::NUMBER AS numofpercapunit3,
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
            FROM {{ source('gwpc', 'pcx_losshistforrating_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS losshistforrating_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'locationname',
                        'loadcommandid',
                        'effectivedatedfieldsid',
                        'publicid',
                        'wages1',
                        'wages2',
                        'wages1_cur',
                        'wages3',
                        'wages2_cur',
                        'createtime',
                        'wages3_cur',
                        'fixedid',
                        'islossadded',
                        'effectivedate',
                        'updatetime',
                        'claimyears1',
                        'claimyears1_cur',
                        'claimyears2',
                        'claimyears2_cur',
                        'claimyears3',
                        'claimyears3_cur',
                        'losshistoryentryid',
                        'expirationdate',
                        'createuserid',
                        'costcenterno',
                        'archivepartition',
                        'beanversion',
                        'changetype',
                        'directwageid',
                        'basedonid',
                        'updateuserid',
                        'btpyear1',
                        'btpyear1_cur',
                        'btpyear2',
                        'btpyear3',
                        'btpyear2_cur',
                        'btpyear3_cur',
                        'subtype',
                        'numofpercapunit1',
                        'numofpercapunit2',
                        'branchid',
                        'numofpercapunit3'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
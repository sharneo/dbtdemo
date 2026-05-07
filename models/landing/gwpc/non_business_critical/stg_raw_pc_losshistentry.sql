{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_losshistentry.
                                                losshistentry_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_policy_centre", "policy_centre", "non_business_critical", "pc_losshistentry"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:Wages1_icare_cur::NUMBER AS wages1_icare_cur,
                data_payload:Wages2_icare_cur::NUMBER AS wages2_icare_cur,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Wages3_icare_cur::NUMBER AS wages3_icare_cur,
                CAST(data_payload:Wages1 AS NUMBER(18,2)) AS wages1,
                CAST(data_payload:Wages2 AS NUMBER(18,2)) AS wages2,
                CAST(data_payload:Wages3 AS NUMBER(18,2)) AS wages3,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:PolicyID::NUMBER AS policyid,
                data_payload:isLossAdded_icare::BOOLEAN AS islossadded_icare,
                data_payload:PolicyPeriodID::NUMBER AS policyperiodid,
                data_payload:ClaimYears1_icare_cur::NUMBER AS claimyears1_icare_cur,
                data_payload:ClaimYears2_icare_cur::NUMBER AS claimyears2_icare_cur,
                data_payload:ClaimYears3_icare_cur::NUMBER AS claimyears3_icare_cur,
                data_payload:LossStatus::NUMBER AS lossstatus,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:PolicyLinePatternCode::TEXT AS VARCHAR(64)) AS policylinepatterncode,
                CAST(data_payload:ClaimYears1 AS NUMBER(18,2)) AS claimyears1,
                CAST(data_payload:ClaimYears2 AS NUMBER(18,2)) AS claimyears2,
                CAST(data_payload:ClaimYears3 AS NUMBER(18,2)) AS claimyears3,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CostCenterNo_icare::TEXT AS VARCHAR(60)) AS costcenterno_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:AmountPaid AS NUMBER(18,2)) AS amountpaid,
                data_payload:AmountPaid_cur::NUMBER AS amountpaid_cur,
                data_payload:LossCause::NUMBER AS losscause,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:DirectWageID::NUMBER AS directwageid,
                CAST(data_payload:AmountResv AS NUMBER(18,2)) AS amountresv,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:AmountResv_cur::NUMBER AS amountresv_cur,
                data_payload:BTPYear1_icare_cur::NUMBER AS btpyear1_icare_cur,
                data_payload:BTPYear2_icare_cur::NUMBER AS btpyear2_icare_cur,
                data_payload:BTPYear3_icare_cur::NUMBER AS btpyear3_icare_cur,
                CAST(data_payload:BTPYear1 AS NUMBER(18,2)) AS btpyear1,
                data_payload:NumOfPerCapUnit1_icare::NUMBER AS numofpercapunit1_icare,
                CAST(data_payload:BTPYear2 AS NUMBER(18,2)) AS btpyear2,
                data_payload:NumOfPerCapUnit2_icare::NUMBER AS numofpercapunit2_icare,
                CAST(data_payload:BTPYear3 AS NUMBER(18,2)) AS btpyear3,
                data_payload:NumOfPerCapUnit3_icare::NUMBER AS numofpercapunit3_icare,
                CAST(data_payload:TotalIncurred AS NUMBER(18,2)) AS totalincurred,
                data_payload:TotalIncurred_cur::NUMBER AS totalincurred_cur,
                CAST(data_payload:LocationName_icare::TEXT AS VARCHAR(500)) AS locationname_icare,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                TO_TIMESTAMP_TZ(data_payload:OccurrenceDate::NUMBER/1000) AS occurrencedate,
                data_payload:AddedToPolicyPeriodTerm_icare::NUMBER AS addedtopolicyperiodterm_icare,
                data_payload:LossHistorySource_icare::NUMBER AS losshistorysource_icare,
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
            FROM {{ source('gwpc', 'pc_losshistentry') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:wages1_icare_cur::NUMBER AS wages1_icare_cur,
                $1:wages2_icare_cur::NUMBER AS wages2_icare_cur,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:wages3_icare_cur::NUMBER AS wages3_icare_cur,
                CAST($1:wages1 AS NUMBER(18,2)) AS wages1,
                CAST($1:wages2 AS NUMBER(18,2)) AS wages2,
                CAST($1:wages3 AS NUMBER(18,2)) AS wages3,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:policyid::NUMBER AS policyid,
                $1:islossadded_icare::BOOLEAN AS islossadded_icare,
                $1:policyperiodid::NUMBER AS policyperiodid,
                $1:claimyears1_icare_cur::NUMBER AS claimyears1_icare_cur,
                $1:claimyears2_icare_cur::NUMBER AS claimyears2_icare_cur,
                $1:claimyears3_icare_cur::NUMBER AS claimyears3_icare_cur,
                $1:lossstatus::NUMBER AS lossstatus,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:policylinepatterncode::TEXT AS VARCHAR(64)) AS policylinepatterncode,
                CAST($1:claimyears1 AS NUMBER(18,2)) AS claimyears1,
                CAST($1:claimyears2 AS NUMBER(18,2)) AS claimyears2,
                CAST($1:claimyears3 AS NUMBER(18,2)) AS claimyears3,
                $1:id::NUMBER AS id,
                CAST($1:costcenterno_icare::TEXT AS VARCHAR(60)) AS costcenterno_icare,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:amountpaid AS NUMBER(18,2)) AS amountpaid,
                $1:amountpaid_cur::NUMBER AS amountpaid_cur,
                $1:losscause::NUMBER AS losscause,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:directwageid::NUMBER AS directwageid,
                CAST($1:amountresv AS NUMBER(18,2)) AS amountresv,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:amountresv_cur::NUMBER AS amountresv_cur,
                $1:btpyear1_icare_cur::NUMBER AS btpyear1_icare_cur,
                $1:btpyear2_icare_cur::NUMBER AS btpyear2_icare_cur,
                $1:btpyear3_icare_cur::NUMBER AS btpyear3_icare_cur,
                CAST($1:btpyear1 AS NUMBER(18,2)) AS btpyear1,
                $1:numofpercapunit1_icare::NUMBER AS numofpercapunit1_icare,
                CAST($1:btpyear2 AS NUMBER(18,2)) AS btpyear2,
                $1:numofpercapunit2_icare::NUMBER AS numofpercapunit2_icare,
                CAST($1:btpyear3 AS NUMBER(18,2)) AS btpyear3,
                $1:numofpercapunit3_icare::NUMBER AS numofpercapunit3_icare,
                CAST($1:totalincurred AS NUMBER(18,2)) AS totalincurred,
                $1:totalincurred_cur::NUMBER AS totalincurred_cur,
                CAST($1:locationname_icare::TEXT AS VARCHAR(500)) AS locationname_icare,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:occurrencedate::TIMESTAMP_TZ AS occurrencedate,
                $1:addedtopolicyperiodterm_icare::NUMBER AS addedtopolicyperiodterm_icare,
                $1:losshistorysource_icare::NUMBER AS losshistorysource_icare,
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
            FROM {{ source('gwpc', 'pc_losshistentry') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS losshistentry_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'wages1_icare_cur',
                        'wages2_icare_cur',
                        'publicid',
                        'wages3_icare_cur',
                        'wages1',
                        'wages2',
                        'wages3',
                        'createtime',
                        'policyid',
                        'islossadded_icare',
                        'policyperiodid',
                        'claimyears1_icare_cur',
                        'claimyears2_icare_cur',
                        'claimyears3_icare_cur',
                        'lossstatus',
                        'updatetime',
                        'policylinepatterncode',
                        'claimyears1',
                        'claimyears2',
                        'claimyears3',
                        'costcenterno_icare',
                        'createuserid',
                        'amountpaid',
                        'amountpaid_cur',
                        'losscause',
                        'beanversion',
                        'archivepartition',
                        'directwageid',
                        'amountresv',
                        'updateuserid',
                        'amountresv_cur',
                        'btpyear1_icare_cur',
                        'btpyear2_icare_cur',
                        'btpyear3_icare_cur',
                        'btpyear1',
                        'numofpercapunit1_icare',
                        'btpyear2',
                        'numofpercapunit2_icare',
                        'btpyear3',
                        'numofpercapunit3_icare',
                        'totalincurred',
                        'totalincurred_cur',
                        'locationname_icare',
                        'description',
                        'occurrencedate',
                        'addedtopolicyperiodterm_icare',
                        'losshistorysource_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
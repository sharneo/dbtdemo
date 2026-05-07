{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_rehabplanitem_ext.
                                                rehabplanitem_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_rehabplanitem_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:StrategyGoalType::NUMBER AS strategygoaltype,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                CAST(data_payload:Goal_icare::TEXT AS VARCHAR(512)) AS goal_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Relevant_icare::BOOLEAN AS relevant_icare,
                data_payload:ID::NUMBER AS id,
                data_payload:IncludeOnImp_icare::BOOLEAN AS includeonimp_icare,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ActionStatus_icare::NUMBER AS actionstatus_icare,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:OwnerContactID::NUMBER AS ownercontactid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ActivityID::NUMBER AS activityid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:BarrierType::NUMBER AS barriertype,
                data_payload:Importance::NUMBER AS importance,
                data_payload:CompletedFlag::BOOLEAN AS completedflag,
                data_payload:ReferralRequired_icare::BOOLEAN AS referralrequired_icare,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:RehabPlanID::NUMBER AS rehabplanid,
                data_payload:Difficulty::NUMBER AS difficulty,
                CAST(data_payload:Description::TEXT AS VARCHAR(2048)) AS description,
                TO_TIMESTAMP_TZ(data_payload:TargetDate::NUMBER/1000) AS targetdate,
                data_payload:CollaborationReviewID::NUMBER AS collaborationreviewid,
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
            FROM {{ source('gwcc', 'ccx_rehabplanitem_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:strategygoaltype::NUMBER AS strategygoaltype,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                CAST($1:goal_icare::TEXT AS VARCHAR(512)) AS goal_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:relevant_icare::BOOLEAN AS relevant_icare,
                $1:id::NUMBER AS id,
                $1:includeonimp_icare::BOOLEAN AS includeonimp_icare,
                $1:createuserid::NUMBER AS createuserid,
                $1:actionstatus_icare::NUMBER AS actionstatus_icare,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:ownercontactid::NUMBER AS ownercontactid,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:activityid::NUMBER AS activityid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:barriertype::NUMBER AS barriertype,
                $1:importance::NUMBER AS importance,
                $1:completedflag::BOOLEAN AS completedflag,
                $1:referralrequired_icare::BOOLEAN AS referralrequired_icare,
                $1:subtype::NUMBER AS subtype,
                $1:rehabplanid::NUMBER AS rehabplanid,
                $1:difficulty::NUMBER AS difficulty,
                CAST($1:description::TEXT AS VARCHAR(2048)) AS description,
                $1:targetdate::TIMESTAMP_TZ AS targetdate,
                $1:collaborationreviewid::NUMBER AS collaborationreviewid,
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
            FROM {{ source('gwcc', 'ccx_rehabplanitem_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS rehabplanitem_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'strategygoaltype',
                        'publicid',
                        'createtime',
                        'documentlinkableid',
                        'goal_icare',
                        'updatetime',
                        'relevant_icare',
                        'includeonimp_icare',
                        'createuserid',
                        'actionstatus_icare',
                        'archivepartition',
                        'ownercontactid',
                        'beanversion',
                        'retired',
                        'activityid',
                        'updateuserid',
                        'barriertype',
                        'importance',
                        'completedflag',
                        'referralrequired_icare',
                        'subtype',
                        'rehabplanid',
                        'difficulty',
                        'description',
                        'targetdate',
                        'collaborationreviewid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
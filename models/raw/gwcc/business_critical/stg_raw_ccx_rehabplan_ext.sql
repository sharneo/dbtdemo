{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_rehabplan_ext.
                                                rehabplan_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "ccx_rehabplan_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                TO_TIMESTAMP_TZ(data_payload:SignedDate::NUMBER/1000) AS signeddate,
                data_payload:Goal_icare::NUMBER AS goal_icare,
                data_payload:RtwPlanDocument_icareID::NUMBER AS rtwplandocument_icareid,
                TO_TIMESTAMP_TZ(data_payload:EstCompletionDate::NUMBER/1000) AS estcompletiondate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:IMPDocumentEmp_icareID::NUMBER AS impdocumentemp_icareid,
                data_payload:ComplexIMP_icare::BOOLEAN AS compleximp_icare,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:Summary::TEXT AS VARCHAR(1333)) AS summary,
                data_payload:CopyToEmployer_icare::BOOLEAN AS copytoemployer_icare,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:IMPDocumentNtd_icareID::NUMBER AS impdocumentntd_icareid,
                TO_TIMESTAMP_TZ(data_payload:StrategyGoalDate_icare::NUMBER/1000) AS strategygoaldate_icare,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Status::NUMBER AS status,
                data_payload:ClaimsStrategyGoal_icare::NUMBER AS claimsstrategygoal_icare,
                data_payload:IMPDocument_icareID::NUMBER AS impdocument_icareid,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                TO_TIMESTAMP_TZ(data_payload:LatestRiskAssessmentDate_Ext::NUMBER/1000) AS latestriskassessmentdate_ext,
                data_payload:WCDStrategy::NUMBER AS wcdstrategy,
                TO_TIMESTAMP_TZ(data_payload:ProposedWCDDate::NUMBER/1000) AS proposedwcddate,
                CAST(data_payload:Other_RTW_RecoveryGoal::TEXT AS VARCHAR(70)) AS other_rtw_recoverygoal,
                CAST(data_payload:OtherClaimsStrategyGoal::TEXT AS VARCHAR(255)) AS otherclaimsstrategygoal,
                data_payload:IMPDocumentRehabSpec_ExtID::NUMBER AS impdocumentrehabspec_extid,
                data_payload:RehabPlanSpeclist_ExtID::NUMBER AS rehabplanspeclist_extid,
                data_payload:CopytoRehabProvider::BOOLEAN AS copytorehabprovider,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime::NUMBER/1000) AS legacycreatetime,
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
            FROM {{ source('gwcc', 'ccx_rehabplan_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:signeddate::TIMESTAMP_TZ AS signeddate,
                $1:goal_icare::NUMBER AS goal_icare,
                $1:rtwplandocument_icareid::NUMBER AS rtwplandocument_icareid,
                $1:estcompletiondate::TIMESTAMP_TZ AS estcompletiondate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:impdocumentemp_icareid::NUMBER AS impdocumentemp_icareid,
                $1:compleximp_icare::BOOLEAN AS compleximp_icare,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:summary::TEXT AS VARCHAR(1333)) AS summary,
                $1:copytoemployer_icare::BOOLEAN AS copytoemployer_icare,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:impdocumentntd_icareid::NUMBER AS impdocumentntd_icareid,
                $1:strategygoaldate_icare::TIMESTAMP_TZ AS strategygoaldate_icare,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:status::NUMBER AS status,
                $1:claimsstrategygoal_icare::NUMBER AS claimsstrategygoal_icare,
                $1:impdocument_icareid::NUMBER AS impdocument_icareid,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                $1:latestriskassessmentdate_ext::TIMESTAMP_TZ AS latestriskassessmentdate_ext,
                $1:wcdstrategy::NUMBER AS wcdstrategy,
                $1:proposedwcddate::TIMESTAMP_TZ AS proposedwcddate,
                CAST($1:other_rtw_recoverygoal::TEXT AS VARCHAR(70)) AS other_rtw_recoverygoal,
                CAST($1:otherclaimsstrategygoal::TEXT AS VARCHAR(255)) AS otherclaimsstrategygoal,
                $1:impdocumentrehabspec_extid::NUMBER AS impdocumentrehabspec_extid,
                $1:rehabplanspeclist_extid::NUMBER AS rehabplanspeclist_extid,
                $1:copytorehabprovider::BOOLEAN AS copytorehabprovider,
                $1:legacycreatetime::TIMESTAMP_TZ AS legacycreatetime,
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
            FROM {{ source('gwcc', 'ccx_rehabplan_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS rehabplan_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'createtime',
                        'signeddate',
                        'goal_icare',
                        'rtwplandocument_icareid',
                        'estcompletiondate',
                        'updatetime',
                        'impdocumentemp_icareid',
                        'compleximp_icare',
                        'claimid',
                        'createuserid',
                        'summary',
                        'copytoemployer_icare',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'impdocumentntd_icareid',
                        'strategygoaldate_icare',
                        'updateuserid',
                        'status',
                        'claimsstrategygoal_icare',
                        'impdocument_icareid',
                        'documentlinkableid',
                        'latestriskassessmentdate_ext',
                        'wcdstrategy',
                        'proposedwcddate',
                        'other_rtw_recoverygoal',
                        'otherclaimsstrategygoal',
                        'impdocumentrehabspec_extid',
                        'rehabplanspeclist_extid',
                        'copytorehabprovider',
                        'legacycreatetime'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
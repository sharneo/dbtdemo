{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_cts_ref_wbnlrules_ext.
                                                cts_ref_wbnlrules_ext_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_cts_ref_wbnlrules_ext"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:WorkStatus::TEXT AS VARCHAR(50)) AS workstatus,
                CAST(data_payload:HoursWorkedStart AS NUMBER(5,2)) AS hoursworkedstart,
                CAST(data_payload:HasEarnings::TEXT AS VARCHAR(50)) AS hasearnings,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:EndWeek::NUMBER AS endweek,
                CAST(data_payload:PIStartLimit AS NUMBER(4,1)) AS pistartlimit,
                data_payload:StartWeek::NUMBER AS startweek,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:CalcMethodIdentifier::TEXT AS VARCHAR(50)) AS calcmethodidentifier,
                CAST(data_payload:WEarningsEndLimit AS NUMBER(18,2)) AS wearningsendlimit,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:Section::TEXT AS VARCHAR(50)) AS section,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PIEndLimit AS NUMBER(4,1)) AS piendlimit,
                CAST(data_payload:WEarningsStartLimit AS NUMBER(18,2)) AS wearningsstartlimit,
                CAST(data_payload:Capacity::TEXT AS VARCHAR(50)) AS capacity,
                CAST(data_payload:HoursWorkedEnd AS NUMBER(5,2)) AS hoursworkedend,
                CAST(data_payload:S39TransitionEligible::TEXT AS VARCHAR(50)) AS s39transitioneligible,
                CAST(data_payload:Section38DecEligible::TEXT AS VARCHAR(50)) AS section38deceligible,
                CAST(data_payload:Section41Eligible::TEXT AS VARCHAR(50)) AS section41eligible,
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
            FROM {{ source('gwcc', 'ccx_cts_ref_wbnlrules_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:workstatus::TEXT AS VARCHAR(50)) AS workstatus,
                CAST($1:hoursworkedstart AS NUMBER(5,2)) AS hoursworkedstart,
                CAST($1:hasearnings::TEXT AS VARCHAR(50)) AS hasearnings,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:endweek::NUMBER AS endweek,
                CAST($1:pistartlimit AS NUMBER(4,1)) AS pistartlimit,
                $1:startweek::NUMBER AS startweek,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:calcmethodidentifier::TEXT AS VARCHAR(50)) AS calcmethodidentifier,
                CAST($1:wearningsendlimit AS NUMBER(18,2)) AS wearningsendlimit,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:section::TEXT AS VARCHAR(50)) AS section,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:piendlimit AS NUMBER(4,1)) AS piendlimit,
                CAST($1:wearningsstartlimit AS NUMBER(18,2)) AS wearningsstartlimit,
                CAST($1:capacity::TEXT AS VARCHAR(50)) AS capacity,
                CAST($1:hoursworkedend AS NUMBER(5,2)) AS hoursworkedend,
                CAST($1:s39transitioneligible::TEXT AS VARCHAR(50)) AS s39transitioneligible,
                CAST($1:section38deceligible::TEXT AS VARCHAR(50)) AS section38deceligible,
                CAST($1:section41eligible::TEXT AS VARCHAR(50)) AS section41eligible,
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
            FROM {{ source('gwcc', 'ccx_cts_ref_wbnlrules_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS cts_ref_wbnlrules_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'workstatus',
                        'hoursworkedstart',
                        'hasearnings',
                        'publicid',
                        'createtime',
                        'endweek',
                        'pistartlimit',
                        'startweek',
                        'updatetime',
                        'calcmethodidentifier',
                        'wearningsendlimit',
                        'createuserid',
                        'section',
                        'beanversion',
                        'retired',
                        'updateuserid',
                        'piendlimit',
                        'wearningsstartlimit',
                        'capacity',
                        'hoursworkedend',
                        's39transitioneligible',
                        'section38deceligible',
                        'section41eligible'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
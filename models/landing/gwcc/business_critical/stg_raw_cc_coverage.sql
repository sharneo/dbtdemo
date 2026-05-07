{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_coverage.
                                                coverage_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_coverage"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:Notes::TEXT AS VARCHAR(255)) AS notes,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:PolicyID::NUMBER AS policyid,
                CAST(data_payload:ReplaceAggLimit AS NUMBER(18,2)) AS replaceagglimit,
                data_payload:State::NUMBER AS state,
                data_payload:Currency::NUMBER AS currency,
                CAST(data_payload:ExposureLimit AS NUMBER(18,2)) AS exposurelimit,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                CAST(data_payload:Deductible AS NUMBER(18,2)) AS deductible,
                data_payload:CoverageBasis::NUMBER AS coveragebasis,
                CAST(data_payload:ClaimAggLimit AS NUMBER(18,2)) AS claimagglimit,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:IncidentLimit AS NUMBER(18,2)) AS incidentlimit,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:Coinsurance AS NUMBER(4,1)) AS coinsurance,
                data_payload:RiskUnitID::NUMBER AS riskunitid,
                CAST(data_payload:PersonAggLimit AS NUMBER(18,2)) AS personagglimit,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:LimitsIndicator::NUMBER AS limitsindicator,
                CAST(data_payload:NonmedAggLimit AS NUMBER(18,2)) AS nonmedagglimit,
                data_payload:Type::NUMBER AS type,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:PolicySystemId::TEXT AS VARCHAR(256)) AS policysystemid,
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
            FROM {{ source('gwcc', 'cc_coverage') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:notes::TEXT AS VARCHAR(255)) AS notes,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:policyid::NUMBER AS policyid,
                CAST($1:replaceagglimit AS NUMBER(18,2)) AS replaceagglimit,
                $1:state::NUMBER AS state,
                $1:currency::NUMBER AS currency,
                CAST($1:exposurelimit AS NUMBER(18,2)) AS exposurelimit,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                CAST($1:deductible AS NUMBER(18,2)) AS deductible,
                $1:coveragebasis::NUMBER AS coveragebasis,
                CAST($1:claimagglimit AS NUMBER(18,2)) AS claimagglimit,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:incidentlimit AS NUMBER(18,2)) AS incidentlimit,
                $1:retired::NUMBER AS retired,
                CAST($1:coinsurance AS NUMBER(4,1)) AS coinsurance,
                $1:riskunitid::NUMBER AS riskunitid,
                CAST($1:personagglimit AS NUMBER(18,2)) AS personagglimit,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:limitsindicator::NUMBER AS limitsindicator,
                CAST($1:nonmedagglimit AS NUMBER(18,2)) AS nonmedagglimit,
                $1:type::NUMBER AS type,
                $1:subtype::NUMBER AS subtype,
                CAST($1:policysystemid::TEXT AS VARCHAR(256)) AS policysystemid,
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
            FROM {{ source('gwcc', 'cc_coverage') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS coverage_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'notes',
                        'publicid',
                        'createtime',
                        'policyid',
                        'replaceagglimit',
                        'state',
                        'currency',
                        'exposurelimit',
                        'effectivedate',
                        'deductible',
                        'coveragebasis',
                        'claimagglimit',
                        'updatetime',
                        'expirationdate',
                        'createuserid',
                        'archivepartition',
                        'beanversion',
                        'incidentlimit',
                        'retired',
                        'coinsurance',
                        'riskunitid',
                        'personagglimit',
                        'updateuserid',
                        'limitsindicator',
                        'nonmedagglimit',
                        'type',
                        'subtype',
                        'policysystemid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
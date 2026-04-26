{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_injurydiagnosis.
                                                injurydiagnosis_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_injurydiagnosis"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:InjuryIncidentID::NUMBER AS injuryincidentid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:DateEnded::NUMBER/1000) AS dateended,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:InjurySeverity_icare::NUMBER AS injuryseverity_icare,
                TO_TIMESTAMP_TZ(data_payload:DateStarted::NUMBER/1000) AS datestarted,
                CAST(data_payload:Comments::TEXT AS VARCHAR(250)) AS comments,
                data_payload:Compensable::BOOLEAN AS compensable,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ICDCode::NUMBER AS icdcode,
                data_payload:IsPrimary::BOOLEAN AS isprimary,
                data_payload:ID::NUMBER AS id,
                data_payload:ContactID::NUMBER AS contactid,
                data_payload:Payable_icare::BOOLEAN AS payable_icare,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime_Ext::NUMBER/1000) AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_injurydiagnosis') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:injuryincidentid::NUMBER AS injuryincidentid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:dateended::TIMESTAMP_TZ AS dateended,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:injuryseverity_icare::NUMBER AS injuryseverity_icare,
                $1:datestarted::TIMESTAMP_TZ AS datestarted,
                CAST($1:comments::TEXT AS VARCHAR(250)) AS comments,
                $1:compensable::BOOLEAN AS compensable,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:icdcode::NUMBER AS icdcode,
                $1:isprimary::BOOLEAN AS isprimary,
                $1:id::NUMBER AS id,
                $1:contactid::NUMBER AS contactid,
                $1:payable_icare::BOOLEAN AS payable_icare,
                $1:legacycreatetime_ext::TIMESTAMP_TZ AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_injurydiagnosis') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS injurydiagnosis_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'injuryincidentid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'dateended',
                        'updateuserid',
                        'injuryseverity_icare',
                        'datestarted',
                        'comments',
                        'compensable',
                        'updatetime',
                        'icdcode',
                        'isprimary',
                        'contactid',
                        'payable_icare',
                        'legacycreatetime_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
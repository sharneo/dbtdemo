{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_triagehistory_icare.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:ProposedSegment::NUMBER AS proposedsegment,
                data_payload:Outcome::NUMBER AS outcome,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:CurrentSegment::NUMBER AS currentsegment,
                data_payload:Source::NUMBER AS source,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:LocationOfInjuryToocs::TEXT AS VARCHAR(255)) AS locationofinjurytoocs,
                TO_TIMESTAMP_TZ(data_payload:TriageDate::NUMBER/1000) AS triagedate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:RiskScore::TEXT AS VARCHAR(255)) AS riskscore,
                TO_TIMESTAMP_TZ(data_payload:DateReviewed::NUMBER/1000) AS datereviewed,
                data_payload:UserID::NUMBER AS userid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:NatureOfInjuryToocs::TEXT AS VARCHAR(255)) AS natureofinjurytoocs,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Comments::TEXT AS VARCHAR(255)) AS comments,
                CAST(data_payload:ICDCode::TEXT AS VARCHAR(255)) AS icdcode,
                data_payload:SegmentReason::NUMBER AS segmentreason,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS VARCHAR(300)) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_triagehistory_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:proposedsegment::NUMBER AS proposedsegment,
                $1:outcome::NUMBER AS outcome,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:currentsegment::NUMBER AS currentsegment,
                $1:source::NUMBER AS source,
                $1:id::NUMBER AS id,
                CAST($1:locationofinjurytoocs::TEXT AS VARCHAR(255)) AS locationofinjurytoocs,
                $1:triagedate::TIMESTAMP_TZ AS triagedate,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:riskscore::TEXT AS VARCHAR(255)) AS riskscore,
                $1:datereviewed::TIMESTAMP_TZ AS datereviewed,
                $1:userid::NUMBER AS userid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                CAST($1:natureofinjurytoocs::TEXT AS VARCHAR(255)) AS natureofinjurytoocs,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:comments::TEXT AS VARCHAR(255)) AS comments,
                CAST($1:icdcode::TEXT AS VARCHAR(255)) AS icdcode,
                $1:segmentreason::NUMBER AS segmentreason,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::VARCHAR(300) as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_triagehistory_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS triagehistory_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'createtime',
                        'proposedsegment',
                        'outcome',
                        'updatetime',
                        'claimid',
                        'currentsegment',
                        'source',
                        'locationofinjurytoocs',
                        'triagedate',
                        'createuserid',
                        'riskscore',
                        'datereviewed',
                        'userid',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'natureofinjurytoocs',
                        'updateuserid',
                        'comments',
                        'icdcode',
                        'segmentreason'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

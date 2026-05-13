{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_bodypart.
                                                bodypart_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_bodypart"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:CompensabilityComments::TEXT AS VARCHAR(60)) AS compensabilitycomments,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:DetailedBodyPart::NUMBER AS detailedbodypart,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:CompensabilityDecision::NUMBER AS compensabilitydecision,
                data_payload:IncidentID::NUMBER AS incidentid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:SideOfBody::NUMBER AS sideofbody,
                data_payload:Settled_icare::BOOLEAN AS settled_icare,
                data_payload:Ordering::NUMBER AS ordering,
                data_payload:WPIClaimedPart_icareID::NUMBER AS wpiclaimedpart_icareid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ImpairmentPercentage::NUMBER AS impairmentpercentage,
                data_payload:DetailedBodyPartDesc::NUMBER AS detailedbodypartdesc,
                data_payload:PrimaryBodyPart::NUMBER AS primarybodypart,
                TO_TIMESTAMP_TZ(data_payload:CompensabilityDecisionDate::NUMBER/1000) AS compensabilitydecisiondate,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ReasonForChange_icare::TEXT AS VARCHAR(255)) AS reasonforchange_icare,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_bodypart') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:compensabilitycomments::TEXT AS VARCHAR(60)) AS compensabilitycomments,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:detailedbodypart::NUMBER AS detailedbodypart,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:compensabilitydecision::NUMBER AS compensabilitydecision,
                $1:incidentid::NUMBER AS incidentid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:sideofbody::NUMBER AS sideofbody,
                $1:settled_icare::BOOLEAN AS settled_icare,
                $1:ordering::NUMBER AS ordering,
                $1:wpiclaimedpart_icareid::NUMBER AS wpiclaimedpart_icareid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:impairmentpercentage::NUMBER AS impairmentpercentage,
                $1:detailedbodypartdesc::NUMBER AS detailedbodypartdesc,
                $1:primarybodypart::NUMBER AS primarybodypart,
                $1:compensabilitydecisiondate::TIMESTAMP_TZ AS compensabilitydecisiondate,
                $1:id::NUMBER AS id,
                CAST($1:reasonforchange_icare::TEXT AS VARCHAR(255)) AS reasonforchange_icare,
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
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_bodypart') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS bodypart_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'compensabilitycomments',
                        'publicid',
                        'detailedbodypart',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'compensabilitydecision',
                        'incidentid',
                        'updateuserid',
                        'sideofbody',
                        'settled_icare',
                        'ordering',
                        'wpiclaimedpart_icareid',
                        'updatetime',
                        'impairmentpercentage',
                        'detailedbodypartdesc',
                        'primarybodypart',
                        'compensabilitydecisiondate',
                        'reasonforchange_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
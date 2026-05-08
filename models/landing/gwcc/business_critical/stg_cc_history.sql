{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_history.
                                                history_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "cc_history"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ExposureID::NUMBER AS exposureid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:SubrogationID::NUMBER AS subrogationid,
                data_payload:UserID::NUMBER AS userid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:CustomType::NUMBER AS customtype,
                CAST(data_payload:RuleUID::TEXT AS VARCHAR(255)) AS ruleuid,
                data_payload:BulkInvoiceID::NUMBER AS bulkinvoiceid,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:Dispute_icareID::NUMBER AS dispute_icareid,
                data_payload:Type::NUMBER AS type,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Description::TEXT AS VARCHAR(1333)) AS description,
                TO_TIMESTAMP_TZ(data_payload:EventTimestamp::NUMBER/1000) AS eventtimestamp,
                data_payload:MatterID::NUMBER AS matterid,
                data_payload:WorkCapacityDecision_icareID::NUMBER AS workcapacitydecision_icareid,
                data_payload:TransactionSetID::NUMBER AS transactionsetid,
                data_payload:MSPReferral_ExtID::NUMBER AS mspreferral_extid,
                data_payload:OCRInvoice_ExtID::NUMBER AS ocrinvoice_extid,
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
            FROM {{ source('gwcc', 'cc_history') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:exposureid::NUMBER AS exposureid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:subrogationid::NUMBER AS subrogationid,
                $1:userid::NUMBER AS userid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:customtype::NUMBER AS customtype,
                CAST($1:ruleuid::TEXT AS VARCHAR(255)) AS ruleuid,
                $1:bulkinvoiceid::NUMBER AS bulkinvoiceid,
                $1:claimid::NUMBER AS claimid,
                $1:dispute_icareid::NUMBER AS dispute_icareid,
                $1:type::NUMBER AS type,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                CAST($1:description::TEXT AS VARCHAR(1333)) AS description,
                $1:eventtimestamp::TIMESTAMP_TZ AS eventtimestamp,
                $1:matterid::NUMBER AS matterid,
                $1:workcapacitydecision_icareid::NUMBER AS workcapacitydecision_icareid,
                $1:transactionsetid::NUMBER AS transactionsetid,
                $1:mspreferral_extid::NUMBER AS mspreferral_extid,
                $1:ocrinvoice_extid::NUMBER AS ocrinvoice_extid,
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
            FROM {{ source('gwcc', 'cc_history') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS history_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'exposureid',
                        'publicid',
                        'subrogationid',
                        'userid',
                        'archivepartition',
                        'beanversion',
                        'customtype',
                        'ruleuid',
                        'bulkinvoiceid',
                        'claimid',
                        'dispute_icareid',
                        'type',
                        'subtype',
                        'description',
                        'eventtimestamp',
                        'matterid',
                        'workcapacitydecision_icareid',
                        'transactionsetid',
                        'mspreferral_extid',
                        'ocrinvoice_extid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
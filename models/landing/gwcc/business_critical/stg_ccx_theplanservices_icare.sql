{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_theplanservices_icare.
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
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:ServiceCost AS NUMBER(9,2)) AS servicecost,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ServiceStatus::NUMBER AS servicestatus,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:TotalAmtRequested AS NUMBER(18,2)) AS totalamtrequested,
                data_payload:DocumentLinkableID::NUMBER AS documentlinkableid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:ServiceType::NUMBER AS servicetype,
                CAST(data_payload:Outcome::TEXT AS VARCHAR(16777216)) AS outcome,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ServReqLinkableID::NUMBER AS servreqlinkableid,
                CAST(data_payload:ExpectedOutcome::TEXT AS VARCHAR(16777216)) AS expectedoutcome,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:KeyRecommendations::TEXT AS VARCHAR(520)) AS keyrecommendations,
                data_payload:Claim::NUMBER AS claim,
                data_payload:MedTreatLinkableID::NUMBER AS medtreatlinkableid,
                TO_TIMESTAMP_TZ(data_payload:DateCompleted::NUMBER/1000) AS datecompleted,
                CAST(data_payload:Purpose::TEXT AS VARCHAR(16777216)) AS purpose,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime::NUMBER/1000) AS legacycreatetime,
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
            FROM {{ source('gwcc', 'ccx_theplanservices_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:servicecost AS NUMBER(9,2)) AS servicecost,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:servicestatus::NUMBER AS servicestatus,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:totalamtrequested AS NUMBER(18,2)) AS totalamtrequested,
                $1:documentlinkableid::NUMBER AS documentlinkableid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:servicetype::NUMBER AS servicetype,
                CAST($1:outcome::TEXT AS VARCHAR(16777216)) AS outcome,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:servreqlinkableid::NUMBER AS servreqlinkableid,
                CAST($1:expectedoutcome::TEXT AS VARCHAR(16777216)) AS expectedoutcome,
                $1:id::NUMBER AS id,
                CAST($1:keyrecommendations::TEXT AS VARCHAR(520)) AS keyrecommendations,
                $1:claim::NUMBER AS claim,
                $1:medtreatlinkableid::NUMBER AS medtreatlinkableid,
                $1:datecompleted::TIMESTAMP_TZ AS datecompleted,
                CAST($1:purpose::TEXT AS VARCHAR(16777216)) AS purpose,
                $1:legacycreatetime::TIMESTAMP_TZ AS legacycreatetime,
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
            FROM {{ source('gwcc', 'ccx_theplanservices_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS theplanservices_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'servicecost',
                        'publicid',
                        'servicestatus',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'totalamtrequested',
                        'documentlinkableid',
                        'updateuserid',
                        'servicetype',
                        'outcome',
                        'updatetime',
                        'servreqlinkableid',
                        'expectedoutcome',
                        'keyrecommendations',
                        'claim',
                        'medtreatlinkableid',
                        'datecompleted',
                        'purpose',
                        'legacycreatetime'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_nlbenefitperiodweek_icare.
                                                nlbenefitperiodweek_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_nlbenefitperiodweek_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:NLBenefitPeriodWeeksID::NUMBER AS nlbenefitperiodweeksid,
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Section40LineItemID::NUMBER AS section40lineitemid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Section41LineItemID::NUMBER AS section41lineitemid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:Deductions AS NUMBER(18,2)) AS deductions,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Capacity::TEXT AS VARCHAR(512)) AS capacity,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:HoursWorked AS NUMBER(5,2)) AS hoursworked,
                CAST(data_payload:PIAWEPost52Weeks AS NUMBER(18,2)) AS piawepost52weeks,
                CAST(data_payload:PIAWEPre52Weeks AS NUMBER(18,2)) AS piawepre52weeks,
                CAST(data_payload:PIAWE AS NUMBER(18,2)) AS piawe,
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
            FROM {{ source('gwcc', 'ccx_nlbenefitperiodweek_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:nlbenefitperiodweeksid::NUMBER AS nlbenefitperiodweeksid,
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:section40lineitemid::NUMBER AS section40lineitemid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:section41lineitemid::NUMBER AS section41lineitemid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:deductions AS NUMBER(18,2)) AS deductions,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:capacity::TEXT AS VARCHAR(512)) AS capacity,
                $1:id::NUMBER AS id,
                CAST($1:hoursworked AS NUMBER(5,2)) AS hoursworked,
                CAST($1:piawepost52weeks AS NUMBER(18,2)) AS piawepost52weeks,
                CAST($1:piawepre52weeks AS NUMBER(18,2)) AS piawepre52weeks,
                CAST($1:piawe AS NUMBER(18,2)) AS piawe,
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
            FROM {{ source('gwcc', 'ccx_nlbenefitperiodweek_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS nlbenefitperiodweek_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'nlbenefitperiodweeksid',
                        'loadcommandid',
                        'createuserid',
                        'section40lineitemid',
                        'publicid',
                        'section41lineitemid',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'createtime',
                        'deductions',
                        'updateuserid',
                        'startdate',
                        'updatetime',
                        'capacity',
                        'hoursworked',
                        'piawepost52weeks',
                        'piawepre52weeks',
                        'piawe'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
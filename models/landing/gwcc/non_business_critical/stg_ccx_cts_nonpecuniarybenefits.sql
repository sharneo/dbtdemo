{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_cts_nonpecuniarybenefits.
                                                cts_nonpecuniarybenefits_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_cts_nonpecuniarybenefits"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:Details::TEXT AS VARCHAR(255)) AS details,
                data_payload:FringeBenefitTaxApplicable::BOOLEAN AS fringebenefittaxapplicable,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:FringeBenenfitValue AS NUMBER(18,2)) AS fringebenenfitvalue,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:NonPecuniaryBenefitsType::NUMBER AS nonpecuniarybenefitstype,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                CAST(data_payload:Value AS NUMBER(18,2)) AS value,
                data_payload:EmploymentDataID::NUMBER AS employmentdataid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                data_payload:NPBBasis::NUMBER AS npbbasis,
                data_payload:Weeks::NUMBER AS weeks,
                data_payload:Withdrawn::BOOLEAN AS withdrawn,
                TO_TIMESTAMP_TZ(data_payload:WithdrawnDate::NUMBER/1000) AS withdrawndate,
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
            FROM {{ source('gwcc', 'ccx_cts_nonpecuniarybenefits') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:details::TEXT AS VARCHAR(255)) AS details,
                $1:fringebenefittaxapplicable::BOOLEAN AS fringebenefittaxapplicable,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:fringebenenfitvalue AS NUMBER(18,2)) AS fringebenenfitvalue,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:nonpecuniarybenefitstype::NUMBER AS nonpecuniarybenefitstype,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                CAST($1:value AS NUMBER(18,2)) AS value,
                $1:employmentdataid::NUMBER AS employmentdataid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:npbbasis::NUMBER AS npbbasis,
                $1:weeks::NUMBER AS weeks,
                $1:withdrawn::BOOLEAN AS withdrawn,
                $1:withdrawndate::TIMESTAMP_TZ AS withdrawndate,
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
            FROM {{ source('gwcc', 'ccx_cts_nonpecuniarybenefits') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS cts_nonpecuniarybenefits_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'details',
                        'fringebenefittaxapplicable',
                        'publicid',
                        'fringebenenfitvalue',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'enddate',
                        'updateuserid',
                        'nonpecuniarybenefitstype',
                        'startdate',
                        'value',
                        'employmentdataid',
                        'updatetime',
                        'subtype',
                        'npbbasis',
                        'weeks',
                        'withdrawn',
                        'withdrawndate'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
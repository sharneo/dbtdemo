{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_group.
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
                data_payload:WorldVisible::BOOLEAN AS worldvisible,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:SupervisorID::NUMBER AS supervisorid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:SecurityZoneID::NUMBER AS securityzoneid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ValidationLevel::NUMBER AS validationlevel,
                CAST(data_payload:Name::TEXT AS VARCHAR(100)) AS name,
                CAST(data_payload:NameKanji::TEXT AS VARCHAR(100)) AS namekanji,
                data_payload:OrganizationID::NUMBER AS organizationid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:LoadFactor::NUMBER AS loadfactor,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:GroupType::NUMBER AS grouptype,
                data_payload:BehaviouralClaims_icare::BOOLEAN AS behaviouralclaims_icare,
                data_payload:ID::NUMBER AS id,
                data_payload:FatalityClaims_icare::BOOLEAN AS fatalityclaims_icare,
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
            FROM {{ source('gwcc', 'cc_group') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:worldvisible::BOOLEAN AS worldvisible,
                $1:createuserid::NUMBER AS createuserid,
                $1:supervisorid::NUMBER AS supervisorid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:securityzoneid::NUMBER AS securityzoneid,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:validationlevel::NUMBER AS validationlevel,
                CAST($1:name::TEXT AS VARCHAR(100)) AS name,
                CAST($1:namekanji::TEXT AS VARCHAR(100)) AS namekanji,
                $1:organizationid::NUMBER AS organizationid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:loadfactor::NUMBER AS loadfactor,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:grouptype::NUMBER AS grouptype,
                $1:behaviouralclaims_icare::BOOLEAN AS behaviouralclaims_icare,
                $1:id::NUMBER AS id,
                $1:fatalityclaims_icare::BOOLEAN AS fatalityclaims_icare,
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
            FROM {{ source('gwcc', 'cc_group') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS group_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'worldvisible',
                        'createuserid',
                        'supervisorid',
                        'publicid',
                        'securityzoneid',
                        'beanversion',
                        'createtime',
                        'retired',
                        'validationlevel',
                        'name',
                        'namekanji',
                        'organizationid',
                        'updateuserid',
                        'loadfactor',
                        'updatetime',
                        'grouptype',
                        'behaviouralclaims_icare',
                        'fatalityclaims_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

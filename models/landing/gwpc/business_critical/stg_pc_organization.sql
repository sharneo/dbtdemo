{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_organization.
                                                organization_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "business_critical", "pc_organization"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Tier::NUMBER AS tier,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ProducerStatus::NUMBER AS producerstatus,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:Code_icare::TEXT AS VARCHAR(255)) AS code_icare,
                CAST(data_payload:NameDenorm::TEXT AS VARCHAR(60)) AS namedenorm,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:Name::TEXT AS VARCHAR(60)) AS name,
                CAST(data_payload:NameKanji::TEXT AS VARCHAR(120)) AS namekanji,
                data_payload:MasterAdmin::BOOLEAN AS masteradmin,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Carrier::BOOLEAN AS carrier,
                data_payload:Type::NUMBER AS type,
                data_payload:ID::NUMBER AS id,
                data_payload:ContactID::NUMBER AS contactid,
                data_payload:ReceiveComunication_icare::BOOLEAN AS receivecomunication_icare,
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
            FROM {{ source('gwpc', 'pc_organization') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                $1:tier::NUMBER AS tier,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:producerstatus::NUMBER AS producerstatus,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:code_icare::TEXT AS VARCHAR(255)) AS code_icare,
                CAST($1:namedenorm::TEXT AS VARCHAR(60)) AS namedenorm,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:name::TEXT AS VARCHAR(60)) AS name,
                CAST($1:namekanji::TEXT AS VARCHAR(120)) AS namekanji,
                $1:masteradmin::BOOLEAN AS masteradmin,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:carrier::BOOLEAN AS carrier,
                $1:type::NUMBER AS type,
                $1:id::NUMBER AS id,
                $1:contactid::NUMBER AS contactid,
                $1:receivecomunication_icare::BOOLEAN AS receivecomunication_icare,
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
            FROM {{ source('gwpc', 'pc_organization') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS organization_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'createuserid',
                        'tier',
                        'publicid',
                        'producerstatus',
                        'beanversion',
                        'code_icare',
                        'namedenorm',
                        'createtime',
                        'retired',
                        'name',
                        'namekanji',
                        'masteradmin',
                        'updateuserid',
                        'updatetime',
                        'carrier',
                        'type',
                        'contactid',
                        'receivecomunication_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
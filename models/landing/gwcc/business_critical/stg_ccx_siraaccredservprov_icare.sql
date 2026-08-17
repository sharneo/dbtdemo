{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_siraaccredservprov_icare.
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
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:Suburb::TEXT AS VARCHAR(40)) AS suburb,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:CompanyName::TEXT AS VARCHAR(255)) AS companyname,
                CAST(data_payload:GivenName::TEXT AS VARCHAR(40)) AS givenname,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                CAST(data_payload:AddressLine1::TEXT AS VARCHAR(255)) AS addressline1,
                CAST(data_payload:AddressLine2::TEXT AS VARCHAR(255)) AS addressline2,
                data_payload:ProviderType::NUMBER AS providertype,
                CAST(data_payload:PostCode::TEXT AS VARCHAR(20)) AS postcode,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:State::NUMBER AS state,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Country::NUMBER AS country,
                CAST(data_payload:ProviderNumber::TEXT AS VARCHAR(60)) AS providernumber,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Surname::TEXT AS VARCHAR(80)) AS surname,
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
            FROM {{ source('gwcc', 'ccx_siraaccredservprov_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:suburb::TEXT AS VARCHAR(40)) AS suburb,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:companyname::TEXT AS VARCHAR(255)) AS companyname,
                CAST($1:givenname::TEXT AS VARCHAR(40)) AS givenname,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                CAST($1:addressline1::TEXT AS VARCHAR(255)) AS addressline1,
                CAST($1:addressline2::TEXT AS VARCHAR(255)) AS addressline2,
                $1:providertype::NUMBER AS providertype,
                CAST($1:postcode::TEXT AS VARCHAR(20)) AS postcode,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:state::NUMBER AS state,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:country::NUMBER AS country,
                CAST($1:providernumber::TEXT AS VARCHAR(60)) AS providernumber,
                $1:id::NUMBER AS id,
                CAST($1:surname::TEXT AS VARCHAR(80)) AS surname,
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
            FROM {{ source('gwcc', 'ccx_siraaccredservprov_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS siraaccredservprov_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'publicid',
                        'suburb',
                        'beanversion',
                        'companyname',
                        'givenname',
                        'createtime',
                        'retired',
                        'enddate',
                        'addressline1',
                        'addressline2',
                        'providertype',
                        'postcode',
                        'updateuserid',
                        'state',
                        'effectivedate',
                        'updatetime',
                        'country',
                        'providernumber',
                        'surname'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

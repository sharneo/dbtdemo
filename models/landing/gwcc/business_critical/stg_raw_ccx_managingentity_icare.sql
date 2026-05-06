{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_managingentity_icare.
                                                managingentity_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "ccx_managingentity_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:PostalAddress::TEXT AS VARCHAR(255)) AS postaladdress,
                CAST(data_payload:AddressLine1::TEXT AS VARCHAR(60)) AS addressline1,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:AddressLine2::TEXT AS VARCHAR(60)) AS addressline2,
                CAST(data_payload:Name::TEXT AS VARCHAR(80)) AS name,
                CAST(data_payload:ContactPhone::TEXT AS VARCHAR(20)) AS contactphone,
                data_payload:State::NUMBER AS state,
                data_payload:Country::NUMBER AS country,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ManagingEntityOrgID::NUMBER AS managingentityorgid,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Footer::TEXT AS VARCHAR(255)) AS footer,
                CAST(data_payload:Website::TEXT AS VARCHAR(255)) AS website,
                CAST(data_payload:WebsiteWithoutPrefix::TEXT AS VARCHAR(255)) AS websitewithoutprefix,
                CAST(data_payload:PortalDisplayName::TEXT AS VARCHAR(255)) AS portaldisplayname,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:City::TEXT AS VARCHAR(60)) AS city,
                data_payload:PolicyType::NUMBER AS policytype,
                CAST(data_payload:Code::TEXT AS VARCHAR(15)) AS code,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:Role::NUMBER AS role,
                CAST(data_payload:PostalCode::TEXT AS VARCHAR(20)) AS postalcode,
                data_payload:Branding::NUMBER AS branding,
                CAST(data_payload:ContactEmail::TEXT AS VARCHAR(255)) AS contactemail,
                data_payload:CSPVisibilityStatus::BOOLEAN AS cspvisibilitystatus,
                CAST(data_payload:AvailableCSP::TEXT AS VARCHAR(255)) AS availablecsp,
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
            FROM {{ source('gwcc', 'ccx_managingentity_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:postaladdress::TEXT AS VARCHAR(255)) AS postaladdress,
                CAST($1:addressline1::TEXT AS VARCHAR(60)) AS addressline1,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:addressline2::TEXT AS VARCHAR(60)) AS addressline2,
                CAST($1:name::TEXT AS VARCHAR(80)) AS name,
                CAST($1:contactphone::TEXT AS VARCHAR(20)) AS contactphone,
                $1:state::NUMBER AS state,
                $1:country::NUMBER AS country,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:managingentityorgid::NUMBER AS managingentityorgid,
                $1:id::NUMBER AS id,
                CAST($1:footer::TEXT AS VARCHAR(255)) AS footer,
                CAST($1:website::TEXT AS VARCHAR(255)) AS website,
                CAST($1:websitewithoutprefix::TEXT AS VARCHAR(255)) AS websitewithoutprefix,
                CAST($1:portaldisplayname::TEXT AS VARCHAR(255)) AS portaldisplayname,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                CAST($1:city::TEXT AS VARCHAR(60)) AS city,
                $1:policytype::NUMBER AS policytype,
                CAST($1:code::TEXT AS VARCHAR(15)) AS code,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:role::NUMBER AS role,
                CAST($1:postalcode::TEXT AS VARCHAR(20)) AS postalcode,
                $1:branding::NUMBER AS branding,
                CAST($1:contactemail::TEXT AS VARCHAR(255)) AS contactemail,
                $1:cspvisibilitystatus::BOOLEAN AS cspvisibilitystatus,
                CAST($1:availablecsp::TEXT AS VARCHAR(255)) AS availablecsp,
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
            FROM {{ source('gwcc', 'ccx_managingentity_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS managingentity_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'postaladdress',
                        'addressline1',
                        'createtime',
                        'addressline2',
                        'name',
                        'contactphone',
                        'state',
                        'country',
                        'updatetime',
                        'managingentityorgid',
                        'footer',
                        'website',
                        'websitewithoutprefix',
                        'portaldisplayname',
                        'createuserid',
                        'beanversion',
                        'retired',
                        'city',
                        'policytype',
                        'code',
                        'updateuserid',
                        'role',
                        'postalcode',
                        'branding',
                        'contactemail',
                        'cspvisibilitystatus',
                        'availablecsp'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
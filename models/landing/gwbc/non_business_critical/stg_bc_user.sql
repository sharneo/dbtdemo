{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for bc_user.
                                                user_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwbc", "billing_centre", "non_business_critical", "bc_user"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:UserSettingsID::NUMBER AS usersettingsid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:SessionTimeoutSecs::NUMBER AS sessiontimeoutsecs,
                data_payload:OrganizationID::NUMBER AS organizationid,
                data_payload:VacationStatus::NUMBER AS vacationstatus,
                CAST(data_payload:Department::TEXT AS VARCHAR(255)) AS department,
                data_payload:ExternalUser::BOOLEAN AS externaluser,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Language::NUMBER AS language,
                data_payload:ExperienceLevel::NUMBER AS experiencelevel,
                data_payload:Locale::NUMBER AS locale,
                data_payload:ID::NUMBER AS id,
                data_payload:AuthorityProfileID::NUMBER AS authorityprofileid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:DefaultPhoneCountry::NUMBER AS defaultphonecountry,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ValidationLevel::NUMBER AS validationlevel,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:CredentialID::NUMBER AS credentialid,
                data_payload:SystemUserType::NUMBER AS systemusertype,
                data_payload:DefaultCountry::NUMBER AS defaultcountry,
                data_payload:TimeZone::NUMBER AS timezone,
                data_payload:ContactID::NUMBER AS contactid,
                CAST(data_payload:JobTitle::TEXT AS VARCHAR(255)) AS jobtitle,
                CAST(data_payload:OktaID_icare::TEXT AS VARCHAR(20)) AS oktaid_icare,
                data_payload:ObfuscatedInternal::BOOLEAN AS obfuscatedinternal,
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
            FROM {{ source('gwbc', 'bc_user') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:usersettingsid::NUMBER AS usersettingsid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:sessiontimeoutsecs::NUMBER AS sessiontimeoutsecs,
                $1:organizationid::NUMBER AS organizationid,
                $1:vacationstatus::NUMBER AS vacationstatus,
                CAST($1:department::TEXT AS VARCHAR(255)) AS department,
                $1:externaluser::BOOLEAN AS externaluser,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:language::NUMBER AS language,
                $1:experiencelevel::NUMBER AS experiencelevel,
                $1:locale::NUMBER AS locale,
                $1:id::NUMBER AS id,
                $1:authorityprofileid::NUMBER AS authorityprofileid,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:defaultphonecountry::NUMBER AS defaultphonecountry,
                $1:retired::NUMBER AS retired,
                $1:validationlevel::NUMBER AS validationlevel,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:credentialid::NUMBER AS credentialid,
                $1:systemusertype::NUMBER AS systemusertype,
                $1:defaultcountry::NUMBER AS defaultcountry,
                $1:timezone::NUMBER AS timezone,
                $1:contactid::NUMBER AS contactid,
                CAST($1:jobtitle::TEXT AS VARCHAR(255)) AS jobtitle,
                CAST($1:oktaid_icare::TEXT AS VARCHAR(20)) AS oktaid_icare,
                $1:obfuscatedinternal::BOOLEAN AS obfuscatedinternal,
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
            FROM {{ source('gwbc', 'bc_user') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS user_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'usersettingsid',
                        'createtime',
                        'sessiontimeoutsecs',
                        'organizationid',
                        'vacationstatus',
                        'department',
                        'externaluser',
                        'updatetime',
                        'language',
                        'experiencelevel',
                        'locale',
                        'authorityprofileid',
                        'createuserid',
                        'beanversion',
                        'defaultphonecountry',
                        'retired',
                        'validationlevel',
                        'updateuserid',
                        'credentialid',
                        'systemusertype',
                        'defaultcountry',
                        'timezone',
                        'contactid',
                        'jobtitle',
                        'oktaid_icare',
                        'obfuscatedinternal'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
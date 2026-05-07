{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_user.
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_user"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:ObfuscatedInternal::BOOLEAN AS obfuscatedinternal,
                TO_TIMESTAMP_TZ(data_payload:OffsetStatsUpdateTime::NUMBER/1000) AS offsetstatsupdatetime,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:UserSettingsID::NUMBER AS usersettingsid,
                CAST(data_payload:SpatialPointDenorm::TEXT AS VARCHAR(16777216)) AS spatialpointdenorm,
                data_payload:SessionTimeoutSecs::NUMBER AS sessiontimeoutsecs,
                data_payload:OrganizationID::NUMBER AS organizationid,
                data_payload:VacationStatus::NUMBER AS vacationstatus,
                CAST(data_payload:Department::TEXT AS VARCHAR(255)) AS department,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ExternalUser::BOOLEAN AS externaluser,
                data_payload:Language::NUMBER AS language,
                data_payload:ExperienceLevel::NUMBER AS experiencelevel,
                data_payload:Locale::NUMBER AS locale,
                data_payload:ID::NUMBER AS id,
                data_payload:LossType::NUMBER AS losstype,
                CAST(data_payload:OktaID_icare::TEXT AS VARCHAR(20)) AS oktaid_icare,
                data_payload:AuthorityProfileID::NUMBER AS authorityprofileid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:NewlyAssignedActivities::NUMBER AS newlyassignedactivities,
                data_payload:Retired::NUMBER AS retired,
                data_payload:DefaultPhoneCountry::NUMBER AS defaultphonecountry,
                data_payload:ValidationLevel::NUMBER AS validationlevel,
                data_payload:PolicyType::NUMBER AS policytype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:QuickClaim::NUMBER AS quickclaim,
                data_payload:CredentialID::NUMBER AS credentialid,
                data_payload:SystemUserType::NUMBER AS systemusertype,
                data_payload:DefaultCountry::NUMBER AS defaultcountry,
                data_payload:TimeZone::NUMBER AS timezone,
                data_payload:ContactID::NUMBER AS contactid,
                CAST(data_payload:JobTitle::TEXT AS VARCHAR(255)) AS jobtitle,
                data_payload:ManagingEntity_icareID::NUMBER AS managingentity_icareid,
                data_payload:MSPSpeciality_Ext::NUMBER AS mspspeciality_ext,
                CAST(data_payload:DenormUserName_Ext::TEXT AS VARCHAR(255)) AS denormusername_ext,
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
            FROM {{ source('gwcc', 'cc_user') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:obfuscatedinternal::BOOLEAN AS obfuscatedinternal,
                $1:offsetstatsupdatetime::TIMESTAMP_TZ AS offsetstatsupdatetime,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:usersettingsid::NUMBER AS usersettingsid,
                CAST($1:spatialpointdenorm::TEXT AS VARCHAR(16777216)) AS spatialpointdenorm,
                $1:sessiontimeoutsecs::NUMBER AS sessiontimeoutsecs,
                $1:organizationid::NUMBER AS organizationid,
                $1:vacationstatus::NUMBER AS vacationstatus,
                CAST($1:department::TEXT AS VARCHAR(255)) AS department,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:externaluser::BOOLEAN AS externaluser,
                $1:language::NUMBER AS language,
                $1:experiencelevel::NUMBER AS experiencelevel,
                $1:locale::NUMBER AS locale,
                $1:id::NUMBER AS id,
                $1:losstype::NUMBER AS losstype,
                CAST($1:oktaid_icare::TEXT AS VARCHAR(20)) AS oktaid_icare,
                $1:authorityprofileid::NUMBER AS authorityprofileid,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:newlyassignedactivities::NUMBER AS newlyassignedactivities,
                $1:retired::NUMBER AS retired,
                $1:defaultphonecountry::NUMBER AS defaultphonecountry,
                $1:validationlevel::NUMBER AS validationlevel,
                $1:policytype::NUMBER AS policytype,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:quickclaim::NUMBER AS quickclaim,
                $1:credentialid::NUMBER AS credentialid,
                $1:systemusertype::NUMBER AS systemusertype,
                $1:defaultcountry::NUMBER AS defaultcountry,
                $1:timezone::NUMBER AS timezone,
                $1:contactid::NUMBER AS contactid,
                CAST($1:jobtitle::TEXT AS VARCHAR(255)) AS jobtitle,
                $1:managingentity_icareid::NUMBER AS managingentity_icareid,
                $1:mspspeciality_ext::NUMBER AS mspspeciality_ext,
                CAST($1:denormusername_ext::TEXT AS VARCHAR(255)) AS denormusername_ext,
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
            FROM {{ source('gwcc', 'cc_user') }}
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
                        'obfuscatedinternal',
                        'offsetstatsupdatetime',
                        'publicid',
                        'createtime',
                        'usersettingsid',
                        'spatialpointdenorm',
                        'sessiontimeoutsecs',
                        'organizationid',
                        'vacationstatus',
                        'department',
                        'updatetime',
                        'externaluser',
                        'language',
                        'experiencelevel',
                        'locale',
                        'losstype',
                        'oktaid_icare',
                        'authorityprofileid',
                        'createuserid',
                        'beanversion',
                        'newlyassignedactivities',
                        'retired',
                        'defaultphonecountry',
                        'validationlevel',
                        'policytype',
                        'updateuserid',
                        'quickclaim',
                        'credentialid',
                        'systemusertype',
                        'defaultcountry',
                        'timezone',
                        'contactid',
                        'jobtitle',
                        'managingentity_icareid',
                        'mspspeciality_ext',
                        'denormusername_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
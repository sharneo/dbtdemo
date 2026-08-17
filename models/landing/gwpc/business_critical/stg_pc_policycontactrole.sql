{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_policycontactrole.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwpc", "policy_centre", "business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:LicenseNumberInternal::TEXT AS VARCHAR(20)) AS licensenumberinternal,
                data_payload:ExcludedInternal::BOOLEAN AS excludedinternal,
                CAST(data_payload:CompanyNameKanjiInternalDenorm::TEXT AS VARCHAR(255)) AS companynamekanjiinternaldenorm,
                data_payload:FixedID::NUMBER AS fixedid,
                data_payload:State::NUMBER AS state,
                data_payload:OwnershipPct::NUMBER AS ownershippct,
                data_payload:CommercialPropertyLine::NUMBER AS commercialpropertyline,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:DateOfBirthInternal::NUMBER/1000) AS dateofbirthinternal,
                data_payload:GeneralLiabilityLine::NUMBER AS generalliabilityline,
                CAST(data_payload:LastNameInternalDenorm::TEXT AS VARCHAR(250)) AS lastnameinternaldenorm,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:MaritalStatusInternal::NUMBER AS maritalstatusinternal,
                data_payload:PolicyLine::NUMBER AS policyline,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:LicenseStateInternal::NUMBER AS licensestateinternal,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:FirstNameInternalDenorm::TEXT AS VARCHAR(250)) AS firstnameinternaldenorm,
                data_payload:QuickQuoteNumber::NUMBER AS quickquotenumber,
                data_payload:WorkersCompLine::NUMBER AS workerscompline,
                CAST(data_payload:Relationship::TEXT AS VARCHAR(1333)) AS relationship,
                data_payload:Included::NUMBER AS included,
                data_payload:BranchID::NUMBER AS branchid,
                data_payload:ClassCodeID::NUMBER AS classcodeid,
                CAST(data_payload:CompanyNameInternalDenorm::TEXT AS VARCHAR(255)) AS companynameinternaldenorm,
                data_payload:DoNotOrderMVR::BOOLEAN AS donotordermvr,
                CAST(data_payload:ParticleInternal::TEXT AS VARCHAR(255)) AS particleinternal,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:CompanyNameInternal::TEXT AS VARCHAR(255)) AS companynameinternal,
                data_payload:ApplicableGoodDriverDiscount::BOOLEAN AS applicablegooddriverdiscount,
                data_payload:InlandMarineLine::NUMBER AS inlandmarineline,
                data_payload:AccountContactRole::NUMBER AS accountcontactrole,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:CompanyNameKanjiInternal::TEXT AS VARCHAR(255)) AS companynamekanjiinternal,
                data_payload:NumberOfViolations::NUMBER AS numberofviolations,
                CAST(data_payload:LastNameKanjiInternalDenorm::TEXT AS VARCHAR(250)) AS lastnamekanjiinternaldenorm,
                CAST(data_payload:LastNameKanjiInternal::TEXT AS VARCHAR(250)) AS lastnamekanjiinternal,
                data_payload:BusinessOwnersLine::NUMBER AS businessownersline,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:ContactDenorm::NUMBER AS contactdenorm,
                data_payload:RelationshipTitleInternal::NUMBER AS relationshiptitleinternal,
                CAST(data_payload:LastNameInternal::TEXT AS VARCHAR(250)) AS lastnameinternal,
                data_payload:BusinessAutoLine::NUMBER AS businessautoline,
                data_payload:PersonalAutoLine::NUMBER AS personalautoline,
                data_payload:NumberOfAccidents::NUMBER AS numberofaccidents,
                data_payload:SeqNumber::NUMBER AS seqnumber,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:BasedOnID::NUMBER AS basedonid,
                CAST(data_payload:FirstNameKanjiInternalDenorm::TEXT AS VARCHAR(250)) AS firstnamekanjiinternaldenorm,
                CAST(data_payload:FirstNameInternal::TEXT AS VARCHAR(250)) AS firstnameinternal,
                CAST(data_payload:FirstNameKanjiInternal::TEXT AS VARCHAR(250)) AS firstnamekanjiinternal,
                data_payload:Subtype::NUMBER AS subtype,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pc_policycontactrole') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:licensenumberinternal::TEXT AS VARCHAR(20)) AS licensenumberinternal,
                $1:excludedinternal::BOOLEAN AS excludedinternal,
                CAST($1:companynamekanjiinternaldenorm::TEXT AS VARCHAR(255)) AS companynamekanjiinternaldenorm,
                $1:fixedid::NUMBER AS fixedid,
                $1:state::NUMBER AS state,
                $1:ownershippct::NUMBER AS ownershippct,
                $1:commercialpropertyline::NUMBER AS commercialpropertyline,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:dateofbirthinternal::TIMESTAMP_TZ AS dateofbirthinternal,
                $1:generalliabilityline::NUMBER AS generalliabilityline,
                CAST($1:lastnameinternaldenorm::TEXT AS VARCHAR(250)) AS lastnameinternaldenorm,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:maritalstatusinternal::NUMBER AS maritalstatusinternal,
                $1:policyline::NUMBER AS policyline,
                $1:beanversion::NUMBER AS beanversion,
                $1:licensestateinternal::NUMBER AS licensestateinternal,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:firstnameinternaldenorm::TEXT AS VARCHAR(250)) AS firstnameinternaldenorm,
                $1:quickquotenumber::NUMBER AS quickquotenumber,
                $1:workerscompline::NUMBER AS workerscompline,
                CAST($1:relationship::TEXT AS VARCHAR(1333)) AS relationship,
                $1:included::NUMBER AS included,
                $1:branchid::NUMBER AS branchid,
                $1:classcodeid::NUMBER AS classcodeid,
                CAST($1:companynameinternaldenorm::TEXT AS VARCHAR(255)) AS companynameinternaldenorm,
                $1:donotordermvr::BOOLEAN AS donotordermvr,
                CAST($1:particleinternal::TEXT AS VARCHAR(255)) AS particleinternal,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:companynameinternal::TEXT AS VARCHAR(255)) AS companynameinternal,
                $1:applicablegooddriverdiscount::BOOLEAN AS applicablegooddriverdiscount,
                $1:inlandmarineline::NUMBER AS inlandmarineline,
                $1:accountcontactrole::NUMBER AS accountcontactrole,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:companynamekanjiinternal::TEXT AS VARCHAR(255)) AS companynamekanjiinternal,
                $1:numberofviolations::NUMBER AS numberofviolations,
                CAST($1:lastnamekanjiinternaldenorm::TEXT AS VARCHAR(250)) AS lastnamekanjiinternaldenorm,
                CAST($1:lastnamekanjiinternal::TEXT AS VARCHAR(250)) AS lastnamekanjiinternal,
                $1:businessownersline::NUMBER AS businessownersline,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:contactdenorm::NUMBER AS contactdenorm,
                $1:relationshiptitleinternal::NUMBER AS relationshiptitleinternal,
                CAST($1:lastnameinternal::TEXT AS VARCHAR(250)) AS lastnameinternal,
                $1:businessautoline::NUMBER AS businessautoline,
                $1:personalautoline::NUMBER AS personalautoline,
                $1:numberofaccidents::NUMBER AS numberofaccidents,
                $1:seqnumber::NUMBER AS seqnumber,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:changetype::NUMBER AS changetype,
                $1:basedonid::NUMBER AS basedonid,
                CAST($1:firstnamekanjiinternaldenorm::TEXT AS VARCHAR(250)) AS firstnamekanjiinternaldenorm,
                CAST($1:firstnameinternal::TEXT AS VARCHAR(250)) AS firstnameinternal,
                CAST($1:firstnamekanjiinternal::TEXT AS VARCHAR(250)) AS firstnamekanjiinternal,
                $1:subtype::NUMBER AS subtype,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pc_policycontactrole') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS policycontactrole_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'licensenumberinternal',
                        'excludedinternal',
                        'companynamekanjiinternaldenorm',
                        'fixedid',
                        'state',
                        'ownershippct',
                        'commercialpropertyline',
                        'updatetime',
                        'dateofbirthinternal',
                        'generalliabilityline',
                        'lastnameinternaldenorm',
                        'createuserid',
                        'maritalstatusinternal',
                        'policyline',
                        'beanversion',
                        'licensestateinternal',
                        'updateuserid',
                        'firstnameinternaldenorm',
                        'quickquotenumber',
                        'workerscompline',
                        'relationship',
                        'included',
                        'branchid',
                        'classcodeid',
                        'companynameinternaldenorm',
                        'donotordermvr',
                        'particleinternal',
                        'publicid',
                        'companynameinternal',
                        'applicablegooddriverdiscount',
                        'inlandmarineline',
                        'accountcontactrole',
                        'createtime',
                        'companynamekanjiinternal',
                        'numberofviolations',
                        'lastnamekanjiinternaldenorm',
                        'lastnamekanjiinternal',
                        'businessownersline',
                        'effectivedate',
                        'expirationdate',
                        'contactdenorm',
                        'relationshiptitleinternal',
                        'lastnameinternal',
                        'businessautoline',
                        'personalautoline',
                        'numberofaccidents',
                        'seqnumber',
                        'archivepartition',
                        'changetype',
                        'basedonid',
                        'firstnamekanjiinternaldenorm',
                        'firstnameinternal',
                        'firstnamekanjiinternal',
                        'subtype'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

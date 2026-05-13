{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_crproject_icare.
                                                crproject_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "non_business_critical", "pcx_crproject_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:GeoLocation::BOOLEAN AS geolocation,
                CAST(data_payload:AcctPayEmail::TEXT AS VARCHAR(100)) AS acctpayemail,
                CAST(data_payload:WBS::TEXT AS VARCHAR(60)) AS wbs,
                data_payload:ContactPhoneCountry::NUMBER AS contactphonecountry,
                data_payload:FixedID::NUMBER AS fixedid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:ConstructionEndDate::NUMBER/1000) AS constructionenddate,
                CAST(data_payload:ContactFirstName::TEXT AS VARCHAR(250)) AS contactfirstname,
                data_payload:ID::NUMBER AS id,
                data_payload:InitialExclusionsCreated::BOOLEAN AS initialexclusionscreated,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:ProjectName::TEXT AS VARCHAR(500)) AS projectname,
                data_payload:CRAddress::NUMBER AS craddress,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:Principal::TEXT AS VARCHAR(200)) AS principal,
                CAST(data_payload:Longitude::TEXT AS VARCHAR(60)) AS longitude,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:DataMigrationCost::NUMBER AS datamigrationcost,
                CAST(data_payload:ContactPhoneExtension::TEXT AS VARCHAR(60)) AS contactphoneextension,
                TO_TIMESTAMP_TZ(data_payload:ReferenceDateInternal::NUMBER/1000) AS referencedateinternal,
                data_payload:PostCompletionPeriod::NUMBER AS postcompletionperiod,
                data_payload:BranchID::NUMBER AS branchid,
                CAST(data_payload:ContactLastName::TEXT AS VARCHAR(250)) AS contactlastname,
                data_payload:InitialCoveragesCreated::BOOLEAN AS initialcoveragescreated,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:TypeOfCover::NUMBER AS typeofcover,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:ContactPhone::TEXT AS VARCHAR(30)) AS contactphone,
                data_payload:TestingPeriod::NUMBER AS testingperiod,
                data_payload:ProjectType::NUMBER AS projecttype,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                CAST(data_payload:ContractNumber::TEXT AS VARCHAR(50)) AS contractnumber,
                data_payload:ContaminatedSite::BOOLEAN AS contaminatedsite,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:TestingPeriodOther::NUMBER AS testingperiodother,
                TO_TIMESTAMP_TZ(data_payload:ConstructionStartDate::NUMBER/1000) AS constructionstartdate,
                data_payload:CRLine::NUMBER AS crline,
                data_payload:AdditionalCoveragesRequired::BOOLEAN AS additionalcoveragesrequired,
                data_payload:PolicyLocation::NUMBER AS policylocation,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:ChangeType::NUMBER AS changetype,
                CAST(data_payload:Latitude::TEXT AS VARCHAR(60)) AS latitude,
                data_payload:InitialConditionsCreated::BOOLEAN AS initialconditionscreated,
                CAST(data_payload:PurchaseOrder::TEXT AS VARCHAR(100)) AS purchaseorder,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:PostCompletionPeriodOther::NUMBER AS postcompletionperiodother,
                CAST(data_payload:ProjectValue AS NUMBER(18,2)) AS projectvalue,
                data_payload:PreferredCoverageCurrency::NUMBER AS preferredcoveragecurrency,
                CAST(data_payload:ContactEmail::TEXT AS VARCHAR(100)) AS contactemail,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_crproject_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:geolocation::BOOLEAN AS geolocation,
                CAST($1:acctpayemail::TEXT AS VARCHAR(100)) AS acctpayemail,
                CAST($1:wbs::TEXT AS VARCHAR(60)) AS wbs,
                $1:contactphonecountry::NUMBER AS contactphonecountry,
                $1:fixedid::NUMBER AS fixedid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:constructionenddate::TIMESTAMP_TZ AS constructionenddate,
                CAST($1:contactfirstname::TEXT AS VARCHAR(250)) AS contactfirstname,
                $1:id::NUMBER AS id,
                $1:initialexclusionscreated::BOOLEAN AS initialexclusionscreated,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:projectname::TEXT AS VARCHAR(500)) AS projectname,
                $1:craddress::NUMBER AS craddress,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:principal::TEXT AS VARCHAR(200)) AS principal,
                CAST($1:longitude::TEXT AS VARCHAR(60)) AS longitude,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:datamigrationcost::NUMBER AS datamigrationcost,
                CAST($1:contactphoneextension::TEXT AS VARCHAR(60)) AS contactphoneextension,
                $1:referencedateinternal::TIMESTAMP_TZ AS referencedateinternal,
                $1:postcompletionperiod::NUMBER AS postcompletionperiod,
                $1:branchid::NUMBER AS branchid,
                CAST($1:contactlastname::TEXT AS VARCHAR(250)) AS contactlastname,
                $1:initialcoveragescreated::BOOLEAN AS initialcoveragescreated,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:typeofcover::NUMBER AS typeofcover,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:contactphone::TEXT AS VARCHAR(30)) AS contactphone,
                $1:testingperiod::NUMBER AS testingperiod,
                $1:projecttype::NUMBER AS projecttype,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                CAST($1:contractnumber::TEXT AS VARCHAR(50)) AS contractnumber,
                $1:contaminatedsite::BOOLEAN AS contaminatedsite,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:testingperiodother::NUMBER AS testingperiodother,
                $1:constructionstartdate::TIMESTAMP_TZ AS constructionstartdate,
                $1:crline::NUMBER AS crline,
                $1:additionalcoveragesrequired::BOOLEAN AS additionalcoveragesrequired,
                $1:policylocation::NUMBER AS policylocation,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:changetype::NUMBER AS changetype,
                CAST($1:latitude::TEXT AS VARCHAR(60)) AS latitude,
                $1:initialconditionscreated::BOOLEAN AS initialconditionscreated,
                CAST($1:purchaseorder::TEXT AS VARCHAR(100)) AS purchaseorder,
                $1:basedonid::NUMBER AS basedonid,
                $1:postcompletionperiodother::NUMBER AS postcompletionperiodother,
                CAST($1:projectvalue AS NUMBER(18,2)) AS projectvalue,
                $1:preferredcoveragecurrency::NUMBER AS preferredcoveragecurrency,
                CAST($1:contactemail::TEXT AS VARCHAR(100)) AS contactemail,
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
                'GWPC' as source_system
            FROM {{ source('gwpc', 'pcx_crproject_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS crproject_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'geolocation',
                        'acctpayemail',
                        'wbs',
                        'contactphonecountry',
                        'fixedid',
                        'updatetime',
                        'constructionenddate',
                        'contactfirstname',
                        'initialexclusionscreated',
                        'createuserid',
                        'projectname',
                        'craddress',
                        'beanversion',
                        'principal',
                        'longitude',
                        'updateuserid',
                        'datamigrationcost',
                        'contactphoneextension',
                        'referencedateinternal',
                        'postcompletionperiod',
                        'branchid',
                        'contactlastname',
                        'initialcoveragescreated',
                        'publicid',
                        'typeofcover',
                        'createtime',
                        'contactphone',
                        'testingperiod',
                        'projecttype',
                        'effectivedate',
                        'contractnumber',
                        'contaminatedsite',
                        'expirationdate',
                        'testingperiodother',
                        'constructionstartdate',
                        'crline',
                        'additionalcoveragesrequired',
                        'policylocation',
                        'archivepartition',
                        'changetype',
                        'latitude',
                        'initialconditionscreated',
                        'purchaseorder',
                        'basedonid',
                        'postcompletionperiodother',
                        'projectvalue',
                        'preferredcoveragecurrency',
                        'contactemail'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
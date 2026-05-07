{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pcx_crifaccountdm_icare.
                                                crifaccountdm_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_policy_centre", "policy_centre", "non_business_critical", "pcx_crifaccountdm_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:AddressLine1::TEXT AS VARCHAR(100)) AS addressline1,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:AddressLine2::TEXT AS VARCHAR(100)) AS addressline2,
                CAST(data_payload:ResultCode::TEXT AS VARCHAR(255)) AS resultcode,
                CAST(data_payload:CRMUniqueID::TEXT AS VARCHAR(100)) AS crmuniqueid,
                data_payload:BatchId::NUMBER AS batchid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Email::TEXT AS VARCHAR(100)) AS email,
                data_payload:Seq::NUMBER AS seq,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:TreasuryPrimeNumber::TEXT AS VARCHAR(100)) AS treasuryprimenumber,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:City::TEXT AS VARCHAR(100)) AS city,
                CAST(data_payload:ABN::TEXT AS VARCHAR(100)) AS abn,
                CAST(data_payload:EntityName::TEXT AS VARCHAR(100)) AS entityname,
                CAST(data_payload:ResultDesc::TEXT AS VARCHAR(1000)) AS resultdesc,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:Status::TEXT AS VARCHAR(255)) AS status,
                CAST(data_payload:DefaultOfferingCode::TEXT AS VARCHAR(100)) AS defaultofferingcode,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(100)) AS accountnumber,
                CAST(data_payload:PostalCode::TEXT AS VARCHAR(100)) AS postalcode,
                data_payload:CRMVersion::NUMBER AS crmversion,
                data_payload:GSTRegistration::BOOLEAN AS gstregistration,
                data_payload:IsHealthPortfolio::BOOLEAN AS ishealthportfolio,
                data_payload:IsMigrated::BOOLEAN AS ismigrated,
                CAST(data_payload:SAPCustomerNumber::TEXT AS VARCHAR(60)) AS sapcustomernumber,
                CAST(data_payload:NewAccountReason::TEXT AS VARCHAR(60)) AS newaccountreason,
                CAST(data_payload:ITCEntitlement::TEXT AS VARCHAR(15)) AS itcentitlement,
                CAST(data_payload:ICP::TEXT AS VARCHAR(15)) AS icp,
                CAST(data_payload:Pool::TEXT AS VARCHAR(60)) AS pool,
                CAST(data_payload:PortfolioOrAgencyCode::TEXT AS VARCHAR(60)) AS portfoliooragencycode,
                data_payload:IsPortfolio::BOOLEAN AS isportfolio,
                CAST(data_payload:ActiveFromDate::TEXT AS VARCHAR(60)) AS activefromdate,
                CAST(data_payload:PortfolioOrAgencyStatus::TEXT AS VARCHAR(60)) AS portfoliooragencystatus,
                CAST(data_payload:AgencyPortfolio::TEXT AS VARCHAR(255)) AS agencyportfolio,
                data_payload:IsAgency::BOOLEAN AS isagency,
                CAST(data_payload:ActiveToDate::TEXT AS VARCHAR(60)) AS activetodate,
                CAST(data_payload:ActionType::TEXT AS VARCHAR(15)) AS actiontype,
                CAST(data_payload:AccountPublicID::TEXT AS VARCHAR(64)) AS accountpublicid,
                data_payload:IsCRIF::BOOLEAN AS iscrif,
                CAST(data_payload:LPCContactID::TEXT AS VARCHAR(255)) AS lpccontactid,
                CAST(data_payload:AgencyType::TEXT AS VARCHAR(60)) AS agencytype,
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
            FROM {{ source('gwpc', 'pcx_crifaccountdm_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:addressline1::TEXT AS VARCHAR(100)) AS addressline1,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:addressline2::TEXT AS VARCHAR(100)) AS addressline2,
                CAST($1:resultcode::TEXT AS VARCHAR(255)) AS resultcode,
                CAST($1:crmuniqueid::TEXT AS VARCHAR(100)) AS crmuniqueid,
                $1:batchid::NUMBER AS batchid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:email::TEXT AS VARCHAR(100)) AS email,
                $1:seq::NUMBER AS seq,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:treasuryprimenumber::TEXT AS VARCHAR(100)) AS treasuryprimenumber,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                CAST($1:city::TEXT AS VARCHAR(100)) AS city,
                CAST($1:abn::TEXT AS VARCHAR(100)) AS abn,
                CAST($1:entityname::TEXT AS VARCHAR(100)) AS entityname,
                CAST($1:resultdesc::TEXT AS VARCHAR(1000)) AS resultdesc,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:status::TEXT AS VARCHAR(255)) AS status,
                CAST($1:defaultofferingcode::TEXT AS VARCHAR(100)) AS defaultofferingcode,
                CAST($1:accountnumber::TEXT AS VARCHAR(100)) AS accountnumber,
                CAST($1:postalcode::TEXT AS VARCHAR(100)) AS postalcode,
                $1:crmversion::NUMBER AS crmversion,
                $1:gstregistration::BOOLEAN AS gstregistration,
                $1:ishealthportfolio::BOOLEAN AS ishealthportfolio,
                $1:ismigrated::BOOLEAN AS ismigrated,
                CAST($1:sapcustomernumber::TEXT AS VARCHAR(60)) AS sapcustomernumber,
                CAST($1:newaccountreason::TEXT AS VARCHAR(60)) AS newaccountreason,
                CAST($1:itcentitlement::TEXT AS VARCHAR(15)) AS itcentitlement,
                CAST($1:icp::TEXT AS VARCHAR(15)) AS icp,
                CAST($1:pool::TEXT AS VARCHAR(60)) AS pool,
                CAST($1:portfoliooragencycode::TEXT AS VARCHAR(60)) AS portfoliooragencycode,
                $1:isportfolio::BOOLEAN AS isportfolio,
                CAST($1:activefromdate::TEXT AS VARCHAR(60)) AS activefromdate,
                CAST($1:portfoliooragencystatus::TEXT AS VARCHAR(60)) AS portfoliooragencystatus,
                CAST($1:agencyportfolio::TEXT AS VARCHAR(255)) AS agencyportfolio,
                $1:isagency::BOOLEAN AS isagency,
                CAST($1:activetodate::TEXT AS VARCHAR(60)) AS activetodate,
                CAST($1:actiontype::TEXT AS VARCHAR(15)) AS actiontype,
                CAST($1:accountpublicid::TEXT AS VARCHAR(64)) AS accountpublicid,
                $1:iscrif::BOOLEAN AS iscrif,
                CAST($1:lpccontactid::TEXT AS VARCHAR(255)) AS lpccontactid,
                CAST($1:agencytype::TEXT AS VARCHAR(60)) AS agencytype,
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
            FROM {{ source('gwpc', 'pcx_crifaccountdm_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS crifaccountdm_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'addressline1',
                        'createtime',
                        'addressline2',
                        'resultcode',
                        'crmuniqueid',
                        'batchid',
                        'updatetime',
                        'email',
                        'seq',
                        'createuserid',
                        'treasuryprimenumber',
                        'beanversion',
                        'retired',
                        'city',
                        'abn',
                        'entityname',
                        'resultdesc',
                        'updateuserid',
                        'status',
                        'defaultofferingcode',
                        'accountnumber',
                        'postalcode',
                        'crmversion',
                        'gstregistration',
                        'ishealthportfolio',
                        'ismigrated',
                        'sapcustomernumber',
                        'newaccountreason',
                        'itcentitlement',
                        'icp',
                        'pool',
                        'portfoliooragencycode',
                        'isportfolio',
                        'activefromdate',
                        'portfoliooragencystatus',
                        'agencyportfolio',
                        'isagency',
                        'activetodate',
                        'actiontype',
                        'accountpublicid',
                        'iscrif',
                        'lpccontactid',
                        'agencytype'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
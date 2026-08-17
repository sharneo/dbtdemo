{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_policystubrefdata_ext.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "non_business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:PolicyLocationID::NUMBER AS policylocationid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ClaimServiceProviderID::NUMBER AS claimserviceproviderid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:WICDescription::TEXT AS VARCHAR(50)) AS wicdescription,
                CAST(data_payload:InsuredWorkPhone::TEXT AS VARCHAR(20)) AS insuredworkphone,
                CAST(data_payload:MainContactFirstName::TEXT AS VARCHAR(40)) AS maincontactfirstname,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:MainContactWorkPhone::TEXT AS VARCHAR(20)) AS maincontactworkphone,
                data_payload:PolicyEstimationType_Ext::NUMBER AS policyestimationtype_ext,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:InsuredAddressID::NUMBER AS insuredaddressid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:MainContactLastName::TEXT AS VARCHAR(80)) AS maincontactlastname,
                data_payload:MainContactAddressID::NUMBER AS maincontactaddressid,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:PolicyType::NUMBER AS policytype,
                CAST(data_payload:ClaimAllocationExceptionGroup::TEXT AS VARCHAR(100)) AS claimallocationexceptiongroup,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:CostCentreNumber::NUMBER AS costcentrenumber,
                data_payload:ITCPercentage::NUMBER AS itcpercentage,
                CAST(data_payload:CostCentreName::TEXT AS VARCHAR(50)) AS costcentrename,
                CAST(data_payload:InsuredName::TEXT AS VARCHAR(255)) AS insuredname,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(40)) AS policynumber,
                CAST(data_payload:WICCode::TEXT AS VARCHAR(20)) AS wiccode,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Offering::TEXT AS VARCHAR(10)) AS offering,
                CAST(data_payload:AgencyCode::TEXT AS VARCHAR(10)) AS agencycode,
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
            FROM {{ source('gwcc', 'ccx_policystubrefdata_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:policylocationid::NUMBER AS policylocationid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:claimserviceproviderid::NUMBER AS claimserviceproviderid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:wicdescription::TEXT AS VARCHAR(50)) AS wicdescription,
                CAST($1:insuredworkphone::TEXT AS VARCHAR(20)) AS insuredworkphone,
                CAST($1:maincontactfirstname::TEXT AS VARCHAR(40)) AS maincontactfirstname,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:maincontactworkphone::TEXT AS VARCHAR(20)) AS maincontactworkphone,
                $1:policyestimationtype_ext::NUMBER AS policyestimationtype_ext,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:createuserid::NUMBER AS createuserid,
                $1:insuredaddressid::NUMBER AS insuredaddressid,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:maincontactlastname::TEXT AS VARCHAR(80)) AS maincontactlastname,
                $1:maincontactaddressid::NUMBER AS maincontactaddressid,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:policytype::NUMBER AS policytype,
                CAST($1:claimallocationexceptiongroup::TEXT AS VARCHAR(100)) AS claimallocationexceptiongroup,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:costcentrenumber::NUMBER AS costcentrenumber,
                $1:itcpercentage::NUMBER AS itcpercentage,
                CAST($1:costcentrename::TEXT AS VARCHAR(50)) AS costcentrename,
                CAST($1:insuredname::TEXT AS VARCHAR(255)) AS insuredname,
                $1:subtype::NUMBER AS subtype,
                CAST($1:policynumber::TEXT AS VARCHAR(40)) AS policynumber,
                CAST($1:wiccode::TEXT AS VARCHAR(20)) AS wiccode,
                $1:id::NUMBER AS id,
                CAST($1:offering::TEXT AS VARCHAR(10)) AS offering,
                CAST($1:agencycode::TEXT AS VARCHAR(10)) AS agencycode,
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
            FROM {{ source('gwcc', 'ccx_policystubrefdata_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS policystubrefdata_ext_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'policylocationid',
                        'publicid',
                        'claimserviceproviderid',
                        'createtime',
                        'wicdescription',
                        'insuredworkphone',
                        'maincontactfirstname',
                        'effectivedate',
                        'updatetime',
                        'maincontactworkphone',
                        'policyestimationtype_ext',
                        'expirationdate',
                        'createuserid',
                        'insuredaddressid',
                        'beanversion',
                        'maincontactlastname',
                        'maincontactaddressid',
                        'archivepartition',
                        'retired',
                        'policytype',
                        'claimallocationexceptiongroup',
                        'updateuserid',
                        'costcentrenumber',
                        'itcpercentage',
                        'costcentrename',
                        'insuredname',
                        'subtype',
                        'policynumber',
                        'wiccode',
                        'offering',
                        'agencycode'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

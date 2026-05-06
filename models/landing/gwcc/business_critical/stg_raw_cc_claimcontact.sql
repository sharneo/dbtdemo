{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_claimcontact.
                                                claimcontact_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "business_critical", "cc_claimcontact"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PersonFirstNameDenorm::TEXT AS VARCHAR(40)) AS personfirstnamedenorm,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:ContactValidFrom::NUMBER/1000) AS contactvalidfrom,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:PolicyID::NUMBER AS policyid,
                TO_TIMESTAMP_TZ(data_payload:BenefitEndDate::NUMBER/1000) AS benefitenddate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                TO_TIMESTAMP_TZ(data_payload:ContactValidTo::NUMBER/1000) AS contactvalidto,
                data_payload:ClaimantFlag::BOOLEAN AS claimantflag,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                data_payload:ContactProhibited::BOOLEAN AS contactprohibited,
                data_payload:EssentialServiceType::NUMBER AS essentialservicetype,
                data_payload:BenefitEndReasonType::NUMBER AS benefitendreasontype,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:ContactNameDenorm::TEXT AS VARCHAR(255)) AS contactnamedenorm,
                CAST(data_payload:BenefitEndReason::TEXT AS VARCHAR(255)) AS benefitendreason,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:ProviderType::NUMBER AS providertype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PersonLastNameDenorm::TEXT AS VARCHAR(80)) AS personlastnamedenorm,
                CAST(data_payload:Service::TEXT AS VARCHAR(1333)) AS service,
                data_payload:DependentType::NUMBER AS dependenttype,
                data_payload:ContactID::NUMBER AS contactid,
                CAST(data_payload:Details_icare::TEXT AS VARCHAR(1024)) AS details_icare,
                TO_TIMESTAMP_TZ(data_payload:BenefitStartDate_Ext::NUMBER/1000) AS benefitstartdate_ext,
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
            FROM {{ source('gwcc', 'cc_claimcontact') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:personfirstnamedenorm::TEXT AS VARCHAR(40)) AS personfirstnamedenorm,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:contactvalidfrom::TIMESTAMP_TZ AS contactvalidfrom,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:policyid::NUMBER AS policyid,
                $1:benefitenddate::TIMESTAMP_TZ AS benefitenddate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:contactvalidto::TIMESTAMP_TZ AS contactvalidto,
                $1:claimantflag::BOOLEAN AS claimantflag,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                $1:contactprohibited::BOOLEAN AS contactprohibited,
                $1:essentialservicetype::NUMBER AS essentialservicetype,
                $1:benefitendreasontype::NUMBER AS benefitendreasontype,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:contactnamedenorm::TEXT AS VARCHAR(255)) AS contactnamedenorm,
                CAST($1:benefitendreason::TEXT AS VARCHAR(255)) AS benefitendreason,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:providertype::NUMBER AS providertype,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:personlastnamedenorm::TEXT AS VARCHAR(80)) AS personlastnamedenorm,
                CAST($1:service::TEXT AS VARCHAR(1333)) AS service,
                $1:dependenttype::NUMBER AS dependenttype,
                $1:contactid::NUMBER AS contactid,
                CAST($1:details_icare::TEXT AS VARCHAR(1024)) AS details_icare,
                $1:benefitstartdate_ext::TIMESTAMP_TZ AS benefitstartdate_ext,
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
            FROM {{ source('gwcc', 'cc_claimcontact') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS claimcontact_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'personfirstnamedenorm',
                        'publicid',
                        'contactvalidfrom',
                        'createtime',
                        'policyid',
                        'benefitenddate',
                        'updatetime',
                        'contactvalidto',
                        'claimantflag',
                        'claimid',
                        'contactprohibited',
                        'essentialservicetype',
                        'benefitendreasontype',
                        'createuserid',
                        'beanversion',
                        'contactnamedenorm',
                        'benefitendreason',
                        'archivepartition',
                        'retired',
                        'providertype',
                        'updateuserid',
                        'personlastnamedenorm',
                        'service',
                        'dependenttype',
                        'contactid',
                        'details_icare',
                        'benefitstartdate_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
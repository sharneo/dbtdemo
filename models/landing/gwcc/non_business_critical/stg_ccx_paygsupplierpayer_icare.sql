{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_paygsupplierpayer_icare.
                                                paygsupplierpayer_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_paygsupplierpayer_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:FinancialYear::TEXT AS VARCHAR(32)) AS financialyear,
                data_payload:Active::BOOLEAN AS active,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:PayerTelephone::TEXT AS VARCHAR(30)) AS payertelephone,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:PayerFaxExtension::TEXT AS VARCHAR(60)) AS payerfaxextension,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:AuthPersonMiddleName::TEXT AS VARCHAR(40)) AS authpersonmiddlename,
                CAST(data_payload:AuthPersonFirstName::TEXT AS VARCHAR(40)) AS authpersonfirstname,
                CAST(data_payload:AuthPersonLastName::TEXT AS VARCHAR(80)) AS authpersonlastname,
                CAST(data_payload:PayerName::TEXT AS VARCHAR(128)) AS payername,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:BranchNo::TEXT AS VARCHAR(128)) AS branchno,
                CAST(data_payload:PayerPhoneExtension::TEXT AS VARCHAR(60)) AS payerphoneextension,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:DateType::TEXT AS VARCHAR(128)) AS datetype,
                CAST(data_payload:PayerFax::TEXT AS VARCHAR(30)) AS payerfax,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:PolicyType::NUMBER AS policytype,
                data_payload:PayerFaxCountry::NUMBER AS payerfaxcountry,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PayerContactName::TEXT AS VARCHAR(128)) AS payercontactname,
                CAST(data_payload:PayerTradingName::TEXT AS VARCHAR(128)) AS payertradingname,
                CAST(data_payload:PayerABN::TEXT AS VARCHAR(60)) AS payerabn,
                data_payload:Subtype::NUMBER AS subtype,
                CAST(data_payload:PayerBSB::TEXT AS VARCHAR(128)) AS payerbsb,
                data_payload:PayerPhoneCountry::NUMBER AS payerphonecountry,
                data_payload:PayerAddress::NUMBER AS payeraddress,
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
            FROM {{ source('gwcc', 'ccx_paygsupplierpayer_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:financialyear::TEXT AS VARCHAR(32)) AS financialyear,
                $1:active::BOOLEAN AS active,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:payertelephone::TEXT AS VARCHAR(30)) AS payertelephone,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:payerfaxextension::TEXT AS VARCHAR(60)) AS payerfaxextension,
                $1:id::NUMBER AS id,
                CAST($1:authpersonmiddlename::TEXT AS VARCHAR(40)) AS authpersonmiddlename,
                CAST($1:authpersonfirstname::TEXT AS VARCHAR(40)) AS authpersonfirstname,
                CAST($1:authpersonlastname::TEXT AS VARCHAR(80)) AS authpersonlastname,
                CAST($1:payername::TEXT AS VARCHAR(128)) AS payername,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:branchno::TEXT AS VARCHAR(128)) AS branchno,
                CAST($1:payerphoneextension::TEXT AS VARCHAR(60)) AS payerphoneextension,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:datetype::TEXT AS VARCHAR(128)) AS datetype,
                CAST($1:payerfax::TEXT AS VARCHAR(30)) AS payerfax,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:policytype::NUMBER AS policytype,
                $1:payerfaxcountry::NUMBER AS payerfaxcountry,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:payercontactname::TEXT AS VARCHAR(128)) AS payercontactname,
                CAST($1:payertradingname::TEXT AS VARCHAR(128)) AS payertradingname,
                CAST($1:payerabn::TEXT AS VARCHAR(60)) AS payerabn,
                $1:subtype::NUMBER AS subtype,
                CAST($1:payerbsb::TEXT AS VARCHAR(128)) AS payerbsb,
                $1:payerphonecountry::NUMBER AS payerphonecountry,
                $1:payeraddress::NUMBER AS payeraddress,
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
            FROM {{ source('gwcc', 'ccx_paygsupplierpayer_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS paygsupplierpayer_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'financialyear',
                        'active',
                        'createtime',
                        'payertelephone',
                        'updatetime',
                        'payerfaxextension',
                        'authpersonmiddlename',
                        'authpersonfirstname',
                        'authpersonlastname',
                        'payername',
                        'createuserid',
                        'branchno',
                        'payerphoneextension',
                        'beanversion',
                        'datetype',
                        'payerfax',
                        'archivepartition',
                        'retired',
                        'policytype',
                        'payerfaxcountry',
                        'updateuserid',
                        'payercontactname',
                        'payertradingname',
                        'payerabn',
                        'subtype',
                        'payerbsb',
                        'payerphonecountry',
                        'payeraddress'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_eftdata.
                                                eftdata_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "business_critical", "cc_eftdata"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:BankName::TEXT AS VARCHAR(100)) AS bankname,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:BankRoutingNumber::TEXT AS VARCHAR(20)) AS bankroutingnumber,
                CAST(data_payload:AddressBookUID::TEXT AS VARCHAR(64)) AS addressbookuid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:IsPrimary::BOOLEAN AS isprimary,
                data_payload:ID::NUMBER AS id,
                data_payload:WestpacFail_icare::BOOLEAN AS westpacfail_icare,
                CAST(data_payload:ExternalLinkID::TEXT AS VARCHAR(64)) AS externallinkid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:BankBranchName_icare::TEXT AS VARCHAR(255)) AS bankbranchname_icare,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                data_payload:BankAccountType::NUMBER AS bankaccounttype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:BankType_icare::NUMBER AS banktype_icare,
                data_payload:Approved_icare::BOOLEAN AS approved_icare,
                CAST(data_payload:AccountName::TEXT AS VARCHAR(100)) AS accountname,
                CAST(data_payload:BankAccountNumber::TEXT AS VARCHAR(20)) AS bankaccountnumber,
                data_payload:ContactID::NUMBER AS contactid,
                CAST(data_payload:BankSwiftCode_icare::TEXT AS VARCHAR(20)) AS bankswiftcode_icare,
                data_payload:EFTSource_Ext::NUMBER AS eftsource_ext,
                data_payload:IsEdited_Ext::BOOLEAN AS isedited_ext,
                TO_TIMESTAMP_TZ(data_payload:Timestamp_Ext::NUMBER/1000) AS timestamp_ext,
                CAST(data_payload:ProviderRefID_Ext::TEXT AS VARCHAR(32)) AS providerrefid_ext,
                data_payload:Document_ExtID::NUMBER AS document_extid,
                TO_TIMESTAMP_TZ(data_payload:LegacyCreateTime_Ext::NUMBER/1000) AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_eftdata') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:bankname::TEXT AS VARCHAR(100)) AS bankname,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:bankroutingnumber::TEXT AS VARCHAR(20)) AS bankroutingnumber,
                CAST($1:addressbookuid::TEXT AS VARCHAR(64)) AS addressbookuid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:isprimary::BOOLEAN AS isprimary,
                $1:id::NUMBER AS id,
                $1:westpacfail_icare::BOOLEAN AS westpacfail_icare,
                CAST($1:externallinkid::TEXT AS VARCHAR(64)) AS externallinkid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:bankbranchname_icare::TEXT AS VARCHAR(255)) AS bankbranchname_icare,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:bankaccounttype::NUMBER AS bankaccounttype,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:banktype_icare::NUMBER AS banktype_icare,
                $1:approved_icare::BOOLEAN AS approved_icare,
                CAST($1:accountname::TEXT AS VARCHAR(100)) AS accountname,
                CAST($1:bankaccountnumber::TEXT AS VARCHAR(20)) AS bankaccountnumber,
                $1:contactid::NUMBER AS contactid,
                CAST($1:bankswiftcode_icare::TEXT AS VARCHAR(20)) AS bankswiftcode_icare,
                $1:eftsource_ext::NUMBER AS eftsource_ext,
                $1:isedited_ext::BOOLEAN AS isedited_ext,
                $1:timestamp_ext::TIMESTAMP_TZ AS timestamp_ext,
                CAST($1:providerrefid_ext::TEXT AS VARCHAR(32)) AS providerrefid_ext,
                $1:document_extid::NUMBER AS document_extid,
                $1:legacycreatetime_ext::TIMESTAMP_TZ AS legacycreatetime_ext,
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
            FROM {{ source('gwcc', 'cc_eftdata') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS eftdata_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'publicid',
                        'bankname',
                        'createtime',
                        'bankroutingnumber',
                        'addressbookuid',
                        'updatetime',
                        'isprimary',
                        'westpacfail_icare',
                        'externallinkid',
                        'createuserid',
                        'bankbranchname_icare',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'bankaccounttype',
                        'updateuserid',
                        'banktype_icare',
                        'approved_icare',
                        'accountname',
                        'bankaccountnumber',
                        'contactid',
                        'bankswiftcode_icare',
                        'eftsource_ext',
                        'isedited_ext',
                        'timestamp_ext',
                        'providerrefid_ext',
                        'document_extid',
                        'legacycreatetime_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
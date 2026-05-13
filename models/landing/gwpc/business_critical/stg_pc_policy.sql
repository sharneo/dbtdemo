{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_policy.
                                                policy_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "business_critical", "pc_policy"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:DoNotDestroy::BOOLEAN AS donotdestroy,
                data_payload:IsPortalPolicy_icare::BOOLEAN AS isportalpolicy_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:PriorPremiums AS NUMBER(18,2)) AS priorpremiums,
                TO_TIMESTAMP_TZ(data_payload:IssueDate::NUMBER/1000) AS issuedate,
                data_payload:PriorPremiums_cur::NUMBER AS priorpremiums_cur,
                data_payload:MovedPolicySourceAccountID::NUMBER AS movedpolicysourceaccountid,
                data_payload:AccountID::NUMBER AS accountid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:LossHistoryType::NUMBER AS losshistorytype,
                data_payload:ExcludedFromArchive::BOOLEAN AS excludedfromarchive,
                data_payload:ArchiveState::NUMBER AS archivestate,
                data_payload:ArchiveSchemaInfo::NUMBER AS archiveschemainfo,
                data_payload:ArchiveFailureDetailsID::NUMBER AS archivefailuredetailsid,
                data_payload:PackageRisk::NUMBER AS packagerisk,
                data_payload:NumPriorLosses::NUMBER AS numpriorlosses,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:PrimaryLanguage::NUMBER AS primarylanguage,
                data_payload:DoNotArchive::BOOLEAN AS donotarchive,
                data_payload:ID::NUMBER AS id,
                data_payload:PrimaryLocale::NUMBER AS primarylocale,
                CAST(data_payload:ProductCode::TEXT AS VARCHAR(64)) AS productcode,
                CAST(data_payload:ExcludeReason::TEXT AS VARCHAR(255)) AS excludereason,
                data_payload:GroupNumberFromPortal::NUMBER AS groupnumberfromportal,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ArchiveFailureID::NUMBER AS archivefailureid,
                CAST(data_payload:CRNNumber_icare::TEXT AS VARCHAR(60)) AS crnnumber_icare,
                TO_TIMESTAMP_TZ(data_payload:OriginalEffectiveDate::NUMBER/1000) AS originaleffectivedate,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:Retired::NUMBER AS retired,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PriorTotalIncurred AS NUMBER(18,2)) AS priortotalincurred,
                TO_TIMESTAMP_TZ(data_payload:ArchiveDate::NUMBER/1000) AS archivedate,
                data_payload:PriorTotalIncurred_cur::NUMBER AS priortotalincurred_cur,
                data_payload:ProducerCodeOfServiceID::NUMBER AS producercodeofserviceid,
                data_payload:NewProducerCode_Ext::NUMBER AS newproducercode_ext,
                data_payload:NewClaimSchemeAgent_iCare::NUMBER AS newclaimschemeagent_icare,
                CAST(data_payload:MovedPolSrcAcctPubID::TEXT AS VARCHAR(64)) AS movedpolsrcacctpubid,
                CAST(data_payload:AgencyContactDetails_Ext::TEXT AS VARCHAR(255)) AS agencycontactdetails_ext,
                CAST(data_payload:AgencyContactEmail_Ext::TEXT AS VARCHAR(255)) AS agencycontactemail_ext,
                CAST(data_payload:AgencyContactNumber_Ext::TEXT AS VARCHAR(255)) AS agencycontactnumber_ext,
                data_payload:InsuranceBook_ExtID::NUMBER AS insurancebook_extid,
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
            FROM {{ source('gwpc', 'pc_policy') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:donotdestroy::BOOLEAN AS donotdestroy,
                $1:isportalpolicy_icare::BOOLEAN AS isportalpolicy_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:priorpremiums AS NUMBER(18,2)) AS priorpremiums,
                $1:issuedate::TIMESTAMP_TZ AS issuedate,
                $1:priorpremiums_cur::NUMBER AS priorpremiums_cur,
                $1:movedpolicysourceaccountid::NUMBER AS movedpolicysourceaccountid,
                $1:accountid::NUMBER AS accountid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:losshistorytype::NUMBER AS losshistorytype,
                $1:excludedfromarchive::BOOLEAN AS excludedfromarchive,
                $1:archivestate::NUMBER AS archivestate,
                $1:archiveschemainfo::NUMBER AS archiveschemainfo,
                $1:archivefailuredetailsid::NUMBER AS archivefailuredetailsid,
                $1:packagerisk::NUMBER AS packagerisk,
                $1:numpriorlosses::NUMBER AS numpriorlosses,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:primarylanguage::NUMBER AS primarylanguage,
                $1:donotarchive::BOOLEAN AS donotarchive,
                $1:id::NUMBER AS id,
                $1:primarylocale::NUMBER AS primarylocale,
                CAST($1:productcode::TEXT AS VARCHAR(64)) AS productcode,
                CAST($1:excludereason::TEXT AS VARCHAR(255)) AS excludereason,
                $1:groupnumberfromportal::NUMBER AS groupnumberfromportal,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivefailureid::NUMBER AS archivefailureid,
                CAST($1:crnnumber_icare::TEXT AS VARCHAR(60)) AS crnnumber_icare,
                $1:originaleffectivedate::TIMESTAMP_TZ AS originaleffectivedate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:priortotalincurred AS NUMBER(18,2)) AS priortotalincurred,
                $1:archivedate::TIMESTAMP_TZ AS archivedate,
                $1:priortotalincurred_cur::NUMBER AS priortotalincurred_cur,
                $1:producercodeofserviceid::NUMBER AS producercodeofserviceid,
                $1:newproducercode_ext::NUMBER AS newproducercode_ext,
                $1:newclaimschemeagent_icare::NUMBER AS newclaimschemeagent_icare,
                CAST($1:movedpolsrcacctpubid::TEXT AS VARCHAR(64)) AS movedpolsrcacctpubid,
                CAST($1:agencycontactdetails_ext::TEXT AS VARCHAR(255)) AS agencycontactdetails_ext,
                CAST($1:agencycontactemail_ext::TEXT AS VARCHAR(255)) AS agencycontactemail_ext,
                CAST($1:agencycontactnumber_ext::TEXT AS VARCHAR(255)) AS agencycontactnumber_ext,
                $1:insurancebook_extid::NUMBER AS insurancebook_extid,
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
            FROM {{ source('gwpc', 'pc_policy') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS policy_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'donotdestroy',
                        'isportalpolicy_icare',
                        'publicid',
                        'priorpremiums',
                        'issuedate',
                        'priorpremiums_cur',
                        'movedpolicysourceaccountid',
                        'accountid',
                        'createtime',
                        'losshistorytype',
                        'excludedfromarchive',
                        'archivestate',
                        'archiveschemainfo',
                        'archivefailuredetailsid',
                        'packagerisk',
                        'numpriorlosses',
                        'updatetime',
                        'primarylanguage',
                        'donotarchive',
                        'primarylocale',
                        'productcode',
                        'excludereason',
                        'groupnumberfromportal',
                        'createuserid',
                        'archivefailureid',
                        'crnnumber_icare',
                        'originaleffectivedate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'updateuserid',
                        'priortotalincurred',
                        'archivedate',
                        'priortotalincurred_cur',
                        'producercodeofserviceid',
                        'newproducercode_ext',
                        'newclaimschemeagent_icare',
                        'movedpolsrcacctpubid',
                        'agencycontactdetails_ext',
                        'agencycontactemail_ext',
                        'agencycontactnumber_ext',
                        'insurancebook_extid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
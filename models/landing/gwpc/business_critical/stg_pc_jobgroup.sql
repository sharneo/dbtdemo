{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_jobgroup.
                                                jobgroup_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "business_critical", "pc_jobgroup"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:GroupNumber_icare::NUMBER AS groupnumber_icare,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:GroupAnnWages AS NUMBER(18,2)) AS groupannwages,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:Name::TEXT AS VARCHAR(255)) AS name,
                data_payload:Account::NUMBER AS account,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:GroupStatus::NUMBER AS groupstatus,
                data_payload:CPACategory::NUMBER AS cpacategory,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:GroupType::NUMBER AS grouptype,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:GroupAnnBTP AS NUMBER(18,2)) AS groupannbtp,
                data_payload:GroupSecurity_cur::NUMBER AS groupsecurity_cur,
                data_payload:LPRProduct::NUMBER AS lprproduct,
                CAST(data_payload:GroupDepositPremium_amt AS NUMBER(18,2)) AS groupdepositpremium_amt,
                CAST(data_payload:SFactor AS NUMBER(7,4)) AS sfactor,
                CAST(data_payload:GroupSecurity_amt AS NUMBER(18,2)) AS groupsecurity_amt,
                data_payload:GroupStage::NUMBER AS groupstage,
                CAST(data_payload:GroupSecurityPerc AS NUMBER(5,2)) AS groupsecurityperc,
                data_payload:TermNumber::NUMBER AS termnumber,
                data_payload:LPRClaimsLimit::NUMBER AS lprclaimslimit,
                data_payload:GroupDepositPremium_cur::NUMBER AS groupdepositpremium_cur,
                data_payload:GroupCostOfClaims_cur::NUMBER AS groupcostofclaims_cur,
                CAST(data_payload:GroupCostOfClaims_amt AS NUMBER(18,2)) AS groupcostofclaims_amt,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:GrpBasePremForLPRPlus_cur::NUMBER AS grpbasepremforlprplus_cur,
                CAST(data_payload:GrpBasePremForLPRPlus_amt AS NUMBER(18,2)) AS grpbasepremforlprplus_amt,
                CAST(data_payload:GroupBTP_Ext AS NUMBER(18,2)) AS groupbtp_ext,
                CAST(data_payload:GroupTotalPremium AS NUMBER(18,2)) AS grouptotalpremium,
                data_payload:RecalculateAvailable::BOOLEAN AS recalculateavailable,
                data_payload:isIssueClicked::BOOLEAN AS isissueclicked,
                CAST(data_payload:PolicyChangeDescription::TEXT AS VARCHAR(1333)) AS policychangedescription,
                data_payload:IsRecalculateGrp::BOOLEAN AS isrecalculategrp,
                data_payload:PolicyChangeType::NUMBER AS policychangetype,
                data_payload:PolicyChangeReason::NUMBER AS policychangereason,
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
            FROM {{ source('gwpc', 'pc_jobgroup') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                $1:groupnumber_icare::NUMBER AS groupnumber_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:groupannwages AS NUMBER(18,2)) AS groupannwages,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:name::TEXT AS VARCHAR(255)) AS name,
                $1:account::NUMBER AS account,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:groupstatus::NUMBER AS groupstatus,
                $1:cpacategory::NUMBER AS cpacategory,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:grouptype::NUMBER AS grouptype,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                CAST($1:groupannbtp AS NUMBER(18,2)) AS groupannbtp,
                $1:groupsecurity_cur::NUMBER AS groupsecurity_cur,
                $1:lprproduct::NUMBER AS lprproduct,
                CAST($1:groupdepositpremium_amt AS NUMBER(18,2)) AS groupdepositpremium_amt,
                CAST($1:sfactor AS NUMBER(7,4)) AS sfactor,
                CAST($1:groupsecurity_amt AS NUMBER(18,2)) AS groupsecurity_amt,
                $1:groupstage::NUMBER AS groupstage,
                CAST($1:groupsecurityperc AS NUMBER(5,2)) AS groupsecurityperc,
                $1:termnumber::NUMBER AS termnumber,
                $1:lprclaimslimit::NUMBER AS lprclaimslimit,
                $1:groupdepositpremium_cur::NUMBER AS groupdepositpremium_cur,
                $1:groupcostofclaims_cur::NUMBER AS groupcostofclaims_cur,
                CAST($1:groupcostofclaims_amt AS NUMBER(18,2)) AS groupcostofclaims_amt,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:grpbasepremforlprplus_cur::NUMBER AS grpbasepremforlprplus_cur,
                CAST($1:grpbasepremforlprplus_amt AS NUMBER(18,2)) AS grpbasepremforlprplus_amt,
                CAST($1:groupbtp_ext AS NUMBER(18,2)) AS groupbtp_ext,
                CAST($1:grouptotalpremium AS NUMBER(18,2)) AS grouptotalpremium,
                $1:recalculateavailable::BOOLEAN AS recalculateavailable,
                $1:isissueclicked::BOOLEAN AS isissueclicked,
                CAST($1:policychangedescription::TEXT AS VARCHAR(1333)) AS policychangedescription,
                $1:isrecalculategrp::BOOLEAN AS isrecalculategrp,
                $1:policychangetype::NUMBER AS policychangetype,
                $1:policychangereason::NUMBER AS policychangereason,
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
            FROM {{ source('gwpc', 'pc_jobgroup') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS jobgroup_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'createuserid',
                        'groupnumber_icare',
                        'publicid',
                        'beanversion',
                        'groupannwages',
                        'retired',
                        'createtime',
                        'name',
                        'account',
                        'updateuserid',
                        'groupstatus',
                        'cpacategory',
                        'updatetime',
                        'grouptype',
                        'subtype',
                        'groupannbtp',
                        'groupsecurity_cur',
                        'lprproduct',
                        'groupdepositpremium_amt',
                        'sfactor',
                        'groupsecurity_amt',
                        'groupstage',
                        'groupsecurityperc',
                        'termnumber',
                        'lprclaimslimit',
                        'groupdepositpremium_cur',
                        'groupcostofclaims_cur',
                        'groupcostofclaims_amt',
                        'archivepartition',
                        'grpbasepremforlprplus_cur',
                        'grpbasepremforlprplus_amt',
                        'groupbtp_ext',
                        'grouptotalpremium',
                        'recalculateavailable',
                        'isissueclicked',
                        'policychangedescription',
                        'isrecalculategrp',
                        'policychangetype',
                        'policychangereason'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
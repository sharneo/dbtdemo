
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This Converts Parquet or AVRO Data Loaded in the Variant Column in the RAW DB into Flattend Views
                                                This also creates a HASH_KEY for Incremental Tables for the Curated Layer 
                                                Additional CDA Files are Null in the AVRO but not in CDA .
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    tags=["raw_gwpc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:LegacyDDL AS NUMBER(18,8)) AS legacyddl,
                CAST(data_payload:AuditedAppWages AS NUMBER(18,2)) AS auditedappwages,
                CAST(data_payload:Wages_icare AS NUMBER(18,2)) AS wages_icare,
                CAST(data_payload:TotalWages AS NUMBER(18,2)) AS totalwages,
                data_payload:Wages_icare_cur::NUMBER AS wages_icare_cur,
                data_payload:FixedID::NUMBER AS fixedid,
                data_payload:PAC::NUMBER AS pac,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:NoOfUnits::NUMBER AS noofunits,
                CAST(data_payload:AuditedAsbestosWages AS NUMBER(18,2)) AS auditedasbestoswages,
                data_payload:ID::NUMBER AS id,
                data_payload:NoOfInjuredEmployees::NUMBER AS noofinjuredemployees,
                data_payload:InitialExclusionsCreated::BOOLEAN AS initialexclusionscreated,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:NoOfEmployees::NUMBER AS noofemployees,
                CAST(data_payload:GrossValue AS NUMBER(18,2)) AS grossvalue,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:WICRate AS NUMBER(10,4)) AS wicrate,
                data_payload:location::NUMBER AS location,
                TO_TIMESTAMP_TZ(data_payload:ReferenceDateInternal::NUMBER/1000) AS referencedateinternal,
                data_payload:WCLine_icare::NUMBER AS wcline_icare,
                CAST(data_payload:BusinessDescription::TEXT AS VARCHAR(500)) AS businessdescription,
                data_payload:BranchID::NUMBER AS branchid,
                CAST(data_payload:AuditedTotalWages AS NUMBER(18,2)) AS auditedtotalwages,
                data_payload:InitialCoveragesCreated::BOOLEAN AS initialcoveragescreated,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:Total::NUMBER AS total,
                CAST(data_payload:LegacyWICRate AS NUMBER(18,8)) AS legacywicrate,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:NoOfApp::NUMBER AS noofapp,
                CAST(data_payload:AppWages AS NUMBER(18,2)) AS appwages,
                data_payload:WIC::NUMBER AS wic,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                CAST(data_payload:ClaimYears1 AS NUMBER(18,2)) AS claimyears1,
                data_payload:ClaimYears1_cur::NUMBER AS claimyears1_cur,
                CAST(data_payload:ClaimYears2 AS NUMBER(18,2)) AS claimyears2,
                data_payload:ClaimYears2_cur::NUMBER AS claimyears2_cur,
                CAST(data_payload:ClaimYears3 AS NUMBER(18,2)) AS claimyears3,
                data_payload:ClaimYears3_cur::NUMBER AS claimyears3_cur,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CostCenter_icare::NUMBER AS costcenter_icare,
                CAST(data_payload:AuditedGrossValue AS NUMBER(18,2)) AS auditedgrossvalue,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:ChangeType::NUMBER AS changetype,
                data_payload:DirectWageID::NUMBER AS directwageid,
                data_payload:InitialConditionsCreated::BOOLEAN AS initialconditionscreated,
                data_payload:BasedOnID::NUMBER AS basedonid,
                data_payload:AuditedUnits::NUMBER AS auditedunits,
                CAST(data_payload:BTPYear1 AS NUMBER(18,2)) AS btpyear1,
                data_payload:BTPYear1_cur::NUMBER AS btpyear1_cur,
                CAST(data_payload:BTPYear2 AS NUMBER(18,2)) AS btpyear2,
                data_payload:BTPYear2_cur::NUMBER AS btpyear2_cur,
                CAST(data_payload:BTPYear3 AS NUMBER(18,2)) AS btpyear3,
                data_payload:BTPYear3_cur::NUMBER AS btpyear3_cur,
                data_payload:PreferredCoverageCurrency::NUMBER AS preferredcoveragecurrency,
                CAST(data_payload:Description::TEXT AS VARCHAR(500)) AS description,
                CAST(data_payload:DDLContribution AS NUMBER(6,3)) AS ddlcontribution,
                data_payload:AuditedNoOfUnits::NUMBER AS auditednoofunits,
                data_payload:AuditedNoOfEmployees::NUMBER AS auditednoofemployees,
                CAST(data_payload:AuditedLabourComp AS NUMBER(18,2)) AS auditedlabourcomp,
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
            FROM {{ source('gwpc', 'pcx_directwage_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:legacyddl AS NUMBER(18,8)) AS legacyddl,
                CAST($1:auditedappwages AS NUMBER(18,2)) AS auditedappwages,
                CAST($1:wages_icare AS NUMBER(18,2)) AS wages_icare,
                CAST($1:totalwages AS NUMBER(18,2)) AS totalwages,
                $1:wages_icare_cur::NUMBER AS wages_icare_cur,
                $1:fixedid::NUMBER AS fixedid,
                $1:pac::NUMBER AS pac,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:noofunits::NUMBER AS noofunits,
                CAST($1:auditedasbestoswages AS NUMBER(18,2)) AS auditedasbestoswages,
                $1:id::NUMBER AS id,
                $1:noofinjuredemployees::NUMBER AS noofinjuredemployees,
                $1:initialexclusionscreated::BOOLEAN AS initialexclusionscreated,
                $1:createuserid::NUMBER AS createuserid,
                $1:beanversion::NUMBER AS beanversion,
                $1:noofemployees::NUMBER AS noofemployees,
                CAST($1:grossvalue AS NUMBER(18,2)) AS grossvalue,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:wicrate AS NUMBER(10,4)) AS wicrate,
                $1:location::NUMBER AS location,
                $1:referencedateinternal::TIMESTAMP_TZ AS referencedateinternal,
                $1:wcline_icare::NUMBER AS wcline_icare,
                CAST($1:businessdescription::TEXT AS VARCHAR(500)) AS businessdescription,
                $1:branchid::NUMBER AS branchid,
                CAST($1:auditedtotalwages AS NUMBER(18,2)) AS auditedtotalwages,
                $1:initialcoveragescreated::BOOLEAN AS initialcoveragescreated,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:total::NUMBER AS total,
                CAST($1:legacywicrate AS NUMBER(18,8)) AS legacywicrate,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:noofapp::NUMBER AS noofapp,
                CAST($1:appwages AS NUMBER(18,2)) AS appwages,
                $1:wic::NUMBER AS wic,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                CAST($1:claimyears1 AS NUMBER(18,2)) AS claimyears1,
                $1:claimyears1_cur::NUMBER AS claimyears1_cur,
                CAST($1:claimyears2 AS NUMBER(18,2)) AS claimyears2,
                $1:claimyears2_cur::NUMBER AS claimyears2_cur,
                CAST($1:claimyears3 AS NUMBER(18,2)) AS claimyears3,
                $1:claimyears3_cur::NUMBER AS claimyears3_cur,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:costcenter_icare::NUMBER AS costcenter_icare,
                CAST($1:auditedgrossvalue AS NUMBER(18,2)) AS auditedgrossvalue,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:changetype::NUMBER AS changetype,
                $1:directwageid::NUMBER AS directwageid,
                $1:initialconditionscreated::BOOLEAN AS initialconditionscreated,
                $1:basedonid::NUMBER AS basedonid,
                $1:auditedunits::NUMBER AS auditedunits,
                CAST($1:btpyear1 AS NUMBER(18,2)) AS btpyear1,
                $1:btpyear1_cur::NUMBER AS btpyear1_cur,
                CAST($1:btpyear2 AS NUMBER(18,2)) AS btpyear2,
                $1:btpyear2_cur::NUMBER AS btpyear2_cur,
                CAST($1:btpyear3 AS NUMBER(18,2)) AS btpyear3,
                $1:btpyear3_cur::NUMBER AS btpyear3_cur,
                $1:preferredcoveragecurrency::NUMBER AS preferredcoveragecurrency,
                CAST($1:description::TEXT AS VARCHAR(500)) AS description,
                CAST($1:ddlcontribution AS NUMBER(6,3)) AS ddlcontribution,
                $1:auditednoofunits::NUMBER AS auditednoofunits,
                $1:auditednoofemployees::NUMBER AS auditednoofemployees,
                CAST($1:auditedlabourcomp AS NUMBER(18,2)) AS auditedlabourcomp,
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
            FROM {{ source('gwpc', 'pcx_directwage_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
    Transformed CTE is To Create the HASH_KEY Based on the Right Combination
-#}   
cte_transformed AS (
    SELECT
        *,
        CASE
             WHEN file_type = 'AVRO' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'loadcommandid',
                        'legacyddl',
                        'auditedappwages',
                        'wages_icare',
                        'totalwages',
                        'wages_icare_cur',
                        'fixedid',
                        'pac',
                        'updatetime',
                        'noofunits',
                        'auditedasbestoswages',
                        'id',
                        'noofinjuredemployees',
                        'initialexclusionscreated',
                        'createuserid',
                        'beanversion',
                        'noofemployees',
                        'grossvalue',
                        'updateuserid',
                        'wicrate',
                        'location',
                        'referencedateinternal',
                        'wcline_icare',
                        'businessdescription',
                        'branchid',
                        'auditedtotalwages',
                        'initialcoveragescreated',
                        'publicid',
                        'total',
                        'legacywicrate',
                        'createtime',
                        'noofapp',
                        'appwages',
                        'wic',
                        'effectivedate',
                        'claimyears1',
                        'claimyears1_cur',
                        'claimyears2',
                        'claimyears2_cur',
                        'claimyears3',
                        'claimyears3_cur',
                        'expirationdate',
                        'costcenter_icare',
                        'auditedgrossvalue',
                        'archivepartition',
                        'changetype',
                        'directwageid',
                        'initialconditionscreated',
                        'basedonid',
                        'auditedunits',
                        'btpyear1',
                        'btpyear1_cur',
                        'btpyear2',
                        'btpyear2_cur',
                        'btpyear3',
                        'btpyear3_cur',
                        'preferredcoveragecurrency',
                        'description',
                        'ddlcontribution',
                        'auditednoofunits',
                        'auditednoofemployees',
                        'auditedlabourcomp'
                        ]) }}
            WHEN file_type = 'PARQUET' THEN
                {{ dbt_utils.generate_surrogate_key([
                                'id',
                        'gwcbi_seqval'
                        ]) }}
        END AS hash_key    
    FROM cte_source_data
)
SELECT * FROM cte_transformed
        
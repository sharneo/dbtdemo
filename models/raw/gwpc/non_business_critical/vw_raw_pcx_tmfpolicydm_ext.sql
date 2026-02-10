
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
                CAST(data_payload:AgencyContactEmail::TEXT AS VARCHAR(255)) AS agencycontactemail,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:LegacyPolicyNumber::TEXT AS VARCHAR(100)) AS legacypolicynumber,
                CAST(data_payload:AgencyContactNumber::TEXT AS VARCHAR(255)) AS agencycontactnumber,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:FranchiseDeductibleLimit::TEXT AS VARCHAR(255)) AS franchisedeductiblelimit,
                CAST(data_payload:ResultCode::TEXT AS VARCHAR(255)) AS resultcode,
                CAST(data_payload:CSPName::TEXT AS VARCHAR(255)) AS cspname,
                CAST(data_payload:CSPStartDate::TEXT AS VARCHAR(20)) AS cspstartdate,
                CAST(data_payload:Contribution::TEXT AS VARCHAR(255)) AS contribution,
                CAST(data_payload:EffectiveDate::TEXT AS VARCHAR(20)) AS effectivedate,
                data_payload:BatchId::NUMBER AS batchid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:ProductCode::TEXT AS VARCHAR(255)) AS productcode,
                CAST(data_payload:ExpirationDate::TEXT AS VARCHAR(20)) AS expirationdate,
                data_payload:Seq::NUMBER AS seq,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:CommencementDate::TEXT AS VARCHAR(20)) AS commencementdate,
                CAST(data_payload:CSPAgencyChosenDate::TEXT AS VARCHAR(20)) AS cspagencychosendate,
                CAST(data_payload:AgencyContactDetails::TEXT AS VARCHAR(255)) AS agencycontactdetails,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:ContributionAmount::TEXT AS VARCHAR(255)) AS contributionamount,
                data_payload:Retired::NUMBER AS retired,
                data_payload:BudgetNonBudget::BOOLEAN AS budgetnonbudget,
                CAST(data_payload:ResultDesc::TEXT AS VARCHAR(1000)) AS resultdesc,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PolicyCategory::TEXT AS VARCHAR(255)) AS policycategory,
                CAST(data_payload:PolicyPortfolioID::TEXT AS VARCHAR(255)) AS policyportfolioid,
                CAST(data_payload:Status::TEXT AS VARCHAR(255)) AS status,
                CAST(data_payload:FranchiseDeductible::TEXT AS VARCHAR(255)) AS franchisedeductible,
                CAST(data_payload:AccountNumber::TEXT AS VARCHAR(100)) AS accountnumber,
                data_payload:CSPAgencyChosen::BOOLEAN AS cspagencychosen,
                CAST(data_payload:PolicyNumber::TEXT AS VARCHAR(255)) AS policynumber,
                CAST(data_payload:PolicyName::TEXT AS VARCHAR(1333)) AS policyname,
                CAST(data_payload:DeclarationForm::TEXT AS VARCHAR(255)) AS declarationform,
                CAST(data_payload:WICCode::TEXT AS VARCHAR(255)) AS wiccode,
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
            FROM {{ source('gwpc', 'pcx_tmfpolicydm_ext') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:agencycontactemail::TEXT AS VARCHAR(255)) AS agencycontactemail,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:legacypolicynumber::TEXT AS VARCHAR(100)) AS legacypolicynumber,
                CAST($1:agencycontactnumber::TEXT AS VARCHAR(255)) AS agencycontactnumber,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:franchisedeductiblelimit::TEXT AS VARCHAR(255)) AS franchisedeductiblelimit,
                CAST($1:resultcode::TEXT AS VARCHAR(255)) AS resultcode,
                CAST($1:cspname::TEXT AS VARCHAR(255)) AS cspname,
                CAST($1:cspstartdate::TEXT AS VARCHAR(20)) AS cspstartdate,
                CAST($1:contribution::TEXT AS VARCHAR(255)) AS contribution,
                CAST($1:effectivedate::TEXT AS VARCHAR(20)) AS effectivedate,
                $1:batchid::NUMBER AS batchid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                CAST($1:productcode::TEXT AS VARCHAR(255)) AS productcode,
                CAST($1:expirationdate::TEXT AS VARCHAR(20)) AS expirationdate,
                $1:seq::NUMBER AS seq,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:commencementdate::TEXT AS VARCHAR(20)) AS commencementdate,
                CAST($1:cspagencychosendate::TEXT AS VARCHAR(20)) AS cspagencychosendate,
                CAST($1:agencycontactdetails::TEXT AS VARCHAR(255)) AS agencycontactdetails,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:contributionamount::TEXT AS VARCHAR(255)) AS contributionamount,
                $1:retired::NUMBER AS retired,
                $1:budgetnonbudget::BOOLEAN AS budgetnonbudget,
                CAST($1:resultdesc::TEXT AS VARCHAR(1000)) AS resultdesc,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:policycategory::TEXT AS VARCHAR(255)) AS policycategory,
                CAST($1:policyportfolioid::TEXT AS VARCHAR(255)) AS policyportfolioid,
                CAST($1:status::TEXT AS VARCHAR(255)) AS status,
                CAST($1:franchisedeductible::TEXT AS VARCHAR(255)) AS franchisedeductible,
                CAST($1:accountnumber::TEXT AS VARCHAR(100)) AS accountnumber,
                $1:cspagencychosen::BOOLEAN AS cspagencychosen,
                CAST($1:policynumber::TEXT AS VARCHAR(255)) AS policynumber,
                CAST($1:policyname::TEXT AS VARCHAR(1333)) AS policyname,
                CAST($1:declarationform::TEXT AS VARCHAR(255)) AS declarationform,
                CAST($1:wiccode::TEXT AS VARCHAR(255)) AS wiccode,
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
            FROM {{ source('gwpc', 'pcx_tmfpolicydm_ext') }}
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
                        'agencycontactemail',
                        'publicid',
                        'legacypolicynumber',
                        'agencycontactnumber',
                        'createtime',
                        'franchisedeductiblelimit',
                        'resultcode',
                        'cspname',
                        'cspstartdate',
                        'contribution',
                        'effectivedate',
                        'batchid',
                        'updatetime',
                        'id',
                        'productcode',
                        'expirationdate',
                        'seq',
                        'createuserid',
                        'commencementdate',
                        'cspagencychosendate',
                        'agencycontactdetails',
                        'beanversion',
                        'contributionamount',
                        'retired',
                        'budgetnonbudget',
                        'resultdesc',
                        'updateuserid',
                        'policycategory',
                        'policyportfolioid',
                        'status',
                        'franchisedeductible',
                        'accountnumber',
                        'cspagencychosen',
                        'policynumber',
                        'policyname',
                        'declarationform',
                        'wiccode'
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
        

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
                CAST(data_payload:PortalDisplayName::TEXT AS VARCHAR(255)) AS portaldisplayname,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:Offering::TEXT AS VARCHAR(15)) AS offering,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:Name::TEXT AS VARCHAR(80)) AS name,
                CAST(data_payload:Code::TEXT AS VARCHAR(15)) AS code,
                CAST(data_payload:ContactPhone::TEXT AS VARCHAR(20)) AS contactphone,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Role::NUMBER AS role,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Website::TEXT AS VARCHAR(255)) AS website,
                data_payload:SchemeAgentID::NUMBER AS schemeagentid,
                CAST(data_payload:ContactEmail::TEXT AS VARCHAR(255)) AS contactemail,
                TO_TIMESTAMP_TZ(data_payload:SpecialistCSPStartDate::NUMBER/1000) AS specialistcspstartdate,
                data_payload:SpecialistCSPIndex::NUMBER AS specialistcspindex,
                data_payload:GeneralistEmpChoice::BOOLEAN AS generalistempchoice,
                data_payload:SpecialistRoundRobin::BOOLEAN AS specialistroundrobin,
                data_payload:GeneralCSP::BOOLEAN AS generalcsp,
                data_payload:SpecialistEmpchoice::BOOLEAN AS specialistempchoice,
                TO_TIMESTAMP_TZ(data_payload:GeneralistCSPEndDate::NUMBER/1000) AS generalistcspenddate,
                data_payload:SpecialistCSP::BOOLEAN AS specialistcsp,
                data_payload:AssignedPoliciesCount::NUMBER AS assignedpoliciescount,
                TO_TIMESTAMP_TZ(data_payload:GeneralistCSPStartDate::NUMBER/1000) AS generalistcspstartdate,
                CAST(data_payload:Comments::TEXT AS VARCHAR(255)) AS comments,
                TO_TIMESTAMP_TZ(data_payload:SpecialistCSPEndDate::NUMBER/1000) AS specialistcspenddate,
                data_payload:GeneralCSPIndex::NUMBER AS generalcspindex,
                data_payload:GeneralistRoundRobin::BOOLEAN AS generalistroundrobin,
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
            FROM {{ source('gwpc', 'pcx_managingentity_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:portaldisplayname::TEXT AS VARCHAR(255)) AS portaldisplayname,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:offering::TEXT AS VARCHAR(15)) AS offering,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:name::TEXT AS VARCHAR(80)) AS name,
                CAST($1:code::TEXT AS VARCHAR(15)) AS code,
                CAST($1:contactphone::TEXT AS VARCHAR(20)) AS contactphone,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:role::NUMBER AS role,
                $1:id::NUMBER AS id,
                CAST($1:website::TEXT AS VARCHAR(255)) AS website,
                $1:schemeagentid::NUMBER AS schemeagentid,
                CAST($1:contactemail::TEXT AS VARCHAR(255)) AS contactemail,
                $1:specialistcspstartdate::TIMESTAMP_TZ AS specialistcspstartdate,
                $1:specialistcspindex::NUMBER AS specialistcspindex,
                $1:generalistempchoice::BOOLEAN AS generalistempchoice,
                $1:specialistroundrobin::BOOLEAN AS specialistroundrobin,
                $1:generalcsp::BOOLEAN AS generalcsp,
                $1:specialistempchoice::BOOLEAN AS specialistempchoice,
                $1:generalistcspenddate::TIMESTAMP_TZ AS generalistcspenddate,
                $1:specialistcsp::BOOLEAN AS specialistcsp,
                $1:assignedpoliciescount::NUMBER AS assignedpoliciescount,
                $1:generalistcspstartdate::TIMESTAMP_TZ AS generalistcspstartdate,
                CAST($1:comments::TEXT AS VARCHAR(255)) AS comments,
                $1:specialistcspenddate::TIMESTAMP_TZ AS specialistcspenddate,
                $1:generalcspindex::NUMBER AS generalcspindex,
                $1:generalistroundrobin::BOOLEAN AS generalistroundrobin,
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
            FROM {{ source('gwpc', 'pcx_managingentity_icare') }}
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
                        'portaldisplayname',
                        'createuserid',
                        'publicid',
                        'offering',
                        'beanversion',
                        'retired',
                        'createtime',
                        'name',
                        'code',
                        'contactphone',
                        'updateuserid',
                        'updatetime',
                        'role',
                        'id',
                        'website',
                        'schemeagentid',
                        'contactemail',
                        'specialistcspstartdate',
                        'specialistcspindex',
                        'generalistempchoice',
                        'specialistroundrobin',
                        'generalcsp',
                        'specialistempchoice',
                        'generalistcspenddate',
                        'specialistcsp',
                        'assignedpoliciescount',
                        'generalistcspstartdate',
                        'comments',
                        'specialistcspenddate',
                        'generalcspindex',
                        'generalistroundrobin'
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
        
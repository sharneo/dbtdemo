
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
    tags=["raw_gwcc","raw_layer"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:Details::TEXT AS VARCHAR(255)) AS details,
                data_payload:FringeBenefitTaxApplicable::BOOLEAN AS fringebenefittaxapplicable,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:FringeBenenfitValue AS NUMBER(18,2)) AS fringebenenfitvalue,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                TO_TIMESTAMP_TZ(data_payload:EndDate::NUMBER/1000) AS enddate,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:NonPecuniaryBenefitsType::NUMBER AS nonpecuniarybenefitstype,
                TO_TIMESTAMP_TZ(data_payload:StartDate::NUMBER/1000) AS startdate,
                CAST(data_payload:Value AS NUMBER(18,2)) AS value,
                data_payload:EmploymentDataID::NUMBER AS employmentdataid,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:Subtype::NUMBER AS subtype,
                data_payload:ID::NUMBER AS id,
                data_payload:NPBBasis::NUMBER AS npbbasis,
                data_payload:Weeks::NUMBER AS weeks,
                data_payload:Withdrawn::BOOLEAN AS withdrawn,
                TO_TIMESTAMP_TZ(data_payload:WithdrawnDate::NUMBER/1000) AS withdrawndate,
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
            FROM {{ source('gwcc', 'ccx_cts_nonpecuniarybenefits') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:details::TEXT AS VARCHAR(255)) AS details,
                $1:fringebenefittaxapplicable::BOOLEAN AS fringebenefittaxapplicable,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:fringebenenfitvalue AS NUMBER(18,2)) AS fringebenenfitvalue,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                $1:enddate::TIMESTAMP_TZ AS enddate,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:nonpecuniarybenefitstype::NUMBER AS nonpecuniarybenefitstype,
                $1:startdate::TIMESTAMP_TZ AS startdate,
                CAST($1:value AS NUMBER(18,2)) AS value,
                $1:employmentdataid::NUMBER AS employmentdataid,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:subtype::NUMBER AS subtype,
                $1:id::NUMBER AS id,
                $1:npbbasis::NUMBER AS npbbasis,
                $1:weeks::NUMBER AS weeks,
                $1:withdrawn::BOOLEAN AS withdrawn,
                $1:withdrawndate::TIMESTAMP_TZ AS withdrawndate,
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
            FROM {{ source('gwcc', 'ccx_cts_nonpecuniarybenefits') }}
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
                        'createuserid',
                        'details',
                        'fringebenefittaxapplicable',
                        'publicid',
                        'fringebenenfitvalue',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'retired',
                        'enddate',
                        'updateuserid',
                        'nonpecuniarybenefitstype',
                        'startdate',
                        'value',
                        'employmentdataid',
                        'updatetime',
                        'subtype',
                        'id',
                        'npbbasis',
                        'weeks',
                        'withdrawn',
                        'withdrawndate'
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
        
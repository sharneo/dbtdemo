
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
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:PCMCode::TEXT AS VARCHAR(255)) AS pcmcode,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:PACDescription::TEXT AS VARCHAR(255)) AS pacdescription,
                CAST(data_payload:WICDescription::TEXT AS VARCHAR(255)) AS wicdescription,
                CAST(data_payload:DetailedDescription::TEXT AS VARCHAR(16777216)) AS detaileddescription,
                CAST(data_payload:Code::TEXT AS VARCHAR(60)) AS code,
                CAST(data_payload:PerCapitaFlag::TEXT AS VARCHAR(255)) AS percapitaflag,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:WICRate AS NUMBER(8,4)) AS wicrate,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:BasisID::NUMBER AS basisid,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:DDLContribution AS NUMBER(6,4)) AS ddlcontribution,
                CAST(data_payload:CPIRate AS NUMBER(5,2)) AS cpirate,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                CAST(data_payload:PACCode::TEXT AS VARCHAR(60)) AS paccode,
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
            FROM {{ source('gwpc', 'pcx_directwagewic_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:pcmcode::TEXT AS VARCHAR(255)) AS pcmcode,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:pacdescription::TEXT AS VARCHAR(255)) AS pacdescription,
                CAST($1:wicdescription::TEXT AS VARCHAR(255)) AS wicdescription,
                CAST($1:detaileddescription::TEXT AS VARCHAR(16777216)) AS detaileddescription,
                CAST($1:code::TEXT AS VARCHAR(60)) AS code,
                CAST($1:percapitaflag::TEXT AS VARCHAR(255)) AS percapitaflag,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:wicrate AS NUMBER(8,4)) AS wicrate,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:basisid::NUMBER AS basisid,
                $1:id::NUMBER AS id,
                CAST($1:ddlcontribution AS NUMBER(6,4)) AS ddlcontribution,
                CAST($1:cpirate AS NUMBER(5,2)) AS cpirate,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                CAST($1:paccode::TEXT AS VARCHAR(60)) AS paccode,
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
            FROM {{ source('gwpc', 'pcx_directwagewic_icare') }}
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
                        'publicid',
                        'pcmcode',
                        'beanversion',
                        'createtime',
                        'retired',
                        'pacdescription',
                        'wicdescription',
                        'detaileddescription',
                        'code',
                        'percapitaflag',
                        'updateuserid',
                        'wicrate',
                        'effectivedate',
                        'updatetime',
                        'basisid',
                        'id',
                        'ddlcontribution',
                        'cpirate',
                        'expirationdate',
                        'paccode'
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
        
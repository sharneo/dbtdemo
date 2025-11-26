{#-

Project: Data Uplift Progran 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2025-11-18      0.0                             This Converts Parquet or AVRO Data in Variant Column in the RAW DB into Flattend Views

-#}   

{{ config(
    tags=["raw_gwpc"]
) }}

{%- set source_system_code = 'GWPC' -%}

WITH source_data AS 
(

            SELECT
                CAST(data_payload:TRUSTABNSTATUS::TEXT AS VARCHAR(255)) AS TRUSTABNSTATUS,
                data_payload:GROUPNUMBER_ICARE::NUMBER AS GROUPNUMBER,
                CAST(data_payload:BUSOPSDESC::TEXT AS VARCHAR(240)) AS BUSOPSDESC,
                CAST(data_payload:PUBLICID::TEXT AS VARCHAR(64)) AS PUBLICID,
                CAST(data_payload:GROUPNAME_ICARE::TEXT AS VARCHAR(255)) AS GROUPNAME,
                CAST(data_payload:ACN_ICARE::TEXT AS VARCHAR(60)) AS ACN,
                TO_TIMESTAMP_NTZ(data_payload:CREATETIME::NUMBER/1000) AS CREATETIME,
                data_payload:NEWACCOUNTREASON_ICARE::NUMBER AS NEWACCOUNTREASON,
                data_payload:LINKCONTACTS::BOOLEAN AS LINKCONTACTS,
                CAST(data_payload:ITCENTITLEMENT_ICARE::TEXT AS VARCHAR(60)) AS ITCENTITLEMENT,
                data_payload:ID::NUMBER  AS ID,
                CAST(NULL AS VARCHAR(120)) as lsn,
                CAST(NULL AS VARCHAR(2)) as row_operation,
                metadata_file_name,
                file_ingestion_timestamp
            FROM {{ source('gwpc', 'pc_account') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:trustabnstatus_icare::TEXT AS VARCHAR(255)) AS trustabnstatus,
                $1:groupnumber_icare::NUMBER AS groupnumber,
                CAST($1:busopsdesc::TEXT AS VARCHAR(240)) AS busopsdesc,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:groupname_icare::TEXT AS VARCHAR(255)) AS groupname,
                CAST($1:acn_icare::TEXT AS VARCHAR(60)) AS acn,
                $1:createtime::TIMESTAMP_NTZ AS createtime,
                $1:newaccountreason_icare::NUMBER AS newaccountreason,
                $1:linkcontacts::BOOLEAN AS linkcontacts,
                CAST($1:itcentitlement_icare::TEXT AS VARCHAR(60)) AS itcentitlement,
                $1:id::NUMBER AS id,
                CAST($1:gwcbi___lsn::STRING AS VARCHAR(120)) as lsn,
                CAST($1:gwcbi___operation::STRING AS VARCHAR(2)) as row_operation,
                metadata_file_name,
                file_ingestion_timestamp
            FROM {{ source('gwpc', 'pc_account') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
-#}   
transformed AS (
    SELECT
        *
    FROM source_data
)
SELECT * FROM transformed
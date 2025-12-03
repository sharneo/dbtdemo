{#-

Project: Data Uplift Progran 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2025-11-18      0.0                             This Converts Parquet or AVRO Data in Variant Column in the RAW DB into Flattend Views

-#}   

{{ config(
    tags=["raw_salesforce"]
) }}

{%- set source_system_code = 'SALESFORCE' -%}

WITH source_data AS 
(

            SELECT
                CAST('{{source_system_code}}' AS VARCHAR(10)) as source_system_code,
                CAST($1:casenumber::TEXT AS VARCHAR(15)) AS casenumber,
                CAST($1:complaint_outcome__c::TEXT AS VARCHAR(240)) AS complaint_outcome,
                CAST($1:email_address__c::TEXT AS VARCHAR(64)) AS email_address__c,
                CAST($1:fund__c::TEXT AS VARCHAR(255)) AS fund__c,
                metadata_file_name,
                file_ingestion_timestamp
            FROM {{ source('salesforce', 'case') }}
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
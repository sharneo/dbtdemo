{#-

Project: Data Uplift Progran 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2025-11-18      0.0                             This Converts Parquet or AVRO Data in Variant Column in the RAW DB into Flattend Views

-#}   

{{ config(
    tags=["raw_genesys"]
) }}

{%- set source_system_code = 'GENESYS' -%}

WITH source_data AS 
(

SELECT
    t.data_payload:policyId::string as policy_id,
    t.data_payload:organizationId::string AS organization_id,
    t.data_payload:conversationId::string AS conversation_id,
    tr.value:transcriptId::string AS transcript_id,
    tr.value:language::string AS language,
    ph.value:text::string AS phrase_text,
    ph.value:participantPurpose::string AS participant,
    TO_TIMESTAMP(ph.value:startTimeMs::number /  1000) AS start_time_ms,
    ph.value:duration.milliseconds::number AS phrase_duration_ms,
    CAST('{{source_system_code}}' AS VARCHAR(10)) as source_system_code
FROM {{ source('genesys', 'transcript') }} t,
LATERAL FLATTEN(input => t.data_payload:transcripts) tr,
LATERAL FLATTEN(input => tr.value:phrases) ph
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
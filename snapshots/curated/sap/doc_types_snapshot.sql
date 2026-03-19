{% snapshot doc_types_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for DOC_TYPES
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='doc_types_sk',
    alias ='doc_types',
    strategy='check',
    check_cols=['spras', 'ltext'],
    tags=['sap','snapshot','doc_types_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'blart'
    ]) }} AS doc_types_sk,
        *
    FROM {{ source('sap', 'doc_types') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
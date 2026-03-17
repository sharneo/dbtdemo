{% snapshot snap_comp_code %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for COMP_CODE
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='comp_code_sk',
    alias ='comp_code',
    strategy='check',
    check_cols=['butxt', 'ort01', 'land1', 'waers', 'ktopl', 'waabw', 'periv', 'kokfi', 'rcomp', 'stceg', 'xfdis', 'kkber', 'mwskv', 'mwska'],
    tags=['sap','snapshot','snp_comp_code']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'bukrs'
    ]) }} AS comp_code_sk,
        *
    FROM {{ source('sap', 'comp_code') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
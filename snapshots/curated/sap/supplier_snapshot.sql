{% snapshot supplier_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for SUPPLIER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='supplier_sk',
    alias ='supplier',
    strategy='check',
    check_cols=['land1', 'name1', 'name2', 'name3', 'name4', 'ort01', 'pstlz', 'regio', 'sortl', 'stras', 'ktokk', 'telf1', 'telf2', 'telfx', 'vbund', 'stceg'],
    tags=['sap','snapshot','supplier_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'lifnr'
    ]) }} AS supplier_sk,
        *
    FROM {{ source('sap', 'supplier') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
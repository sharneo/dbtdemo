{% snapshot snap_hierarchy_texts %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for HIERARCHY_TEXTS
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='hierarchy_texts_sk',
    alias ='hierarchy_texts',
    strategy='check',
    check_cols=['langu', 'descript'],
    tags=['sap','snapshot','snp_hierarchy_texts']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'setclass',
            'subclass',
            'setname'
    ]) }} AS hierarchy_texts_sk,
        *
    FROM {{ source('sap', 'hierarchy_texts') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
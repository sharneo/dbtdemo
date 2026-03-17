{% snapshot snap_hierarchy_header %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for HIERARCHY_HEADER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='hierarchy_header_sk',
    alias ='hierarchy_header',
    strategy='check',
    check_cols=['lineid', 'subsetcls', 'subsetscls', 'seqnr'],
    tags=['sap','snapshot','snp_hierarchy_header']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'setclass',
            'subclass',
            'setname',
            'subsetname'
    ]) }} AS hierarchy_header_sk,
        *
    FROM {{ source('sap', 'hierarchy_header') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
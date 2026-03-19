{% snapshot hierarchy_item_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for HIERARCHY_ITEM
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='hierarchy_item_sk',
    alias ='hierarchy_item',
    strategy='check',
    check_cols=['valsign', 'valoption', 'valfrom', 'valto', 'seqnr'],
    tags=['sap','snapshot','hierarchy_item_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'setclass',
            'subclass',
            'setname',
            'lineid'
    ]) }} AS hierarchy_item_sk,
        *
    FROM {{ source('sap', 'hierarchy_item') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
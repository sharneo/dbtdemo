{% snapshot profit_center_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for PROFIT_CENTER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='profit_center_sk',
    alias ='profit_center',
    strategy='check',
    check_cols=['datbi', 'kokrs', 'datab', 'ersda', 'usnam', 'verak', 'khinr', 'ktext', 'ltext'],
    tags=['sap','snapshot','profit_center_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'prctr'
    ]) }} AS profit_center_sk,
        *
    FROM {{ source('sap', 'profit_center') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
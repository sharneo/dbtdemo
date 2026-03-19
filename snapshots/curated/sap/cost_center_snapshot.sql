{% snapshot cost_center_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for COST_CENTER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='cost_center_sk',
    alias ='cost_center',
    strategy='check',
    check_cols=['datab', 'bkzkp', 'pkzkp', 'bukrs', 'gsber', 'kosar', 'verak', 'verak_user', 'waers', 'prctr', 'ersda', 'usnam', 'bkzks', 'bkzer', 'bkzob', 'pkzks', 'pkzer', 'vmeth', 'abtei', 'khinr', 'kompl', 'ktext', 'ltext', 'mctxt'],
    tags=['sap','snapshot','cost_center_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'kokrs',
            'kostl',
            'datbi'
    ]) }} AS cost_center_sk,
        *
    FROM {{ source('sap', 'cost_center') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
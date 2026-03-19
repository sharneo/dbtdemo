{% snapshot chart_of_acc_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for CHART_OF_ACC
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='chart_of_acc_sk',
    alias ='chart_of_acc',
    strategy='check',
    check_cols=['xbilk', 'sakan', 'bilkt', 'erdat', 'ernam', 'gvtyp', 'ktoks', 'xspea', 'xspeb', 'xspep', 'glaccount_type', 'txt20', 'txt50'],
    tags=['sap','snapshot','chart_of_acc_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'ktopl',
            'saknr'
    ]) }} AS chart_of_acc_sk,
        *
    FROM {{ source('sap', 'chart_of_acc') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
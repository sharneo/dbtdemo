{% snapshot snap_costcenter_manual_budget %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for COSTCENTER_MANUAL_BUDGET
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='costcenter_manual_budget_sk',
    alias ='costcenter_manual_budget',
    strategy='check',
    check_cols=['amount'],
    tags=['sap','snapshot','snp_costcenter_manual_budget']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'profitcentre',
            'costcentre',
            'company',
            'accountno',
            'fiscyear',
            'fiscperiod',
            'sourceofdata',
            'accyear',
            'flag',
            'category'
    ]) }} AS costcenter_manual_budget_sk,
        *
    FROM {{ source('sap', 'costcenter_manual_budget') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
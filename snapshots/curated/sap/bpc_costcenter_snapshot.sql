{% snapshot bpc_costcenter_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_COSTCENTER
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_costcenter_sk',
    alias ='bpc_costcenter',
    strategy='check',
    check_cols=['b631_s_co_area', 'b631_s_costcntr', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','bpc_costcenter_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_costcenter_sk,
        *
    FROM {{ source('sap', 'bpc_costcenter') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
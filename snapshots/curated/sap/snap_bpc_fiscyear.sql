{% snapshot snap_bpc_fiscyear %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_FISCYEAR
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_fiscyear_sk',
    alias ='bpc_fiscyear',
    strategy='check',
    check_cols=['fiscvarnt', 'fiscyear', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','snp_bpc_fiscyear']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_fiscyear_sk,
        *
    FROM {{ source('sap', 'bpc_fiscyear') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
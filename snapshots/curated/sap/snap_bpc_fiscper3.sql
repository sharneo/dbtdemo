{% snapshot snap_bpc_fiscper3 %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_FISCPER3
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_fiscper3_sk',
    alias ='bpc_fiscper3',
    strategy='check',
    check_cols=['fiscper3', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','snp_bpc_fiscper3']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_fiscper3_sk,
        *
    FROM {{ source('sap', 'bpc_fiscper3') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
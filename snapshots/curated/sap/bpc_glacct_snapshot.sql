{% snapshot bpc_glacct_snapshot%}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for BPC_GLACCT
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='bpc_glacct_sk',
    alias ='bpc_glacct',
    strategy='check',
    check_cols=['b631_s_chrtacct', 'b631_s_gl_acct', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','bpc_glacct_snapshot']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_glacct_sk,
        *
    FROM {{ source('sap', 'bpc_glacct') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
{% snapshot snap_ccenter_security %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             This creates a SCD Type 2 Record for the Data in the RAW Table . This will run for CCENTER_SECURITY
 
 #}   

{{ config(
    target_schema='sap',
    unique_key='ccenter_security_sk',
    alias ='ccenter_security',
    strategy='check',
    check_cols=['cc_name'],
    tags=['sap','snapshot','snp_ccenter_security']
) }}



WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'network_id',
            'cc_number'
    ]) }} AS ccenter_security_sk,
        *
    FROM {{ source('sap', 'ccenter_security') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
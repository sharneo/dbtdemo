{% snapshot snap_ccenter_security %}

{{ config(
    target_schema='sap',
    unique_key='ccenter_security_sk',
    alias ='ccenter_security',
    strategy='check',
    check_cols=['cc_name'],
    tags=['sap','snapshot','curated']
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
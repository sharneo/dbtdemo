{% snapshot snap_bpc_fiscyear %}

{{ config(
    target_schema='sap',
    unique_key='bpc_fiscyear_sk',
    alias ='bpc_fiscyear',
    strategy='check',
    check_cols=['fiscvarnt', 'fiscyear', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
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
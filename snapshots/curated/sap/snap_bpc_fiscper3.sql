{% snapshot snap_bpc_fiscper3 %}

{{ config(
    target_schema='sap',
    unique_key='bpc_fiscper3_sk',
    alias ='bpc_fiscper3',
    strategy='check',
    check_cols=['fiscper3', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
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
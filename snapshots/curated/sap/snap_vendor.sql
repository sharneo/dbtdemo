{% snapshot snap_vendor %}

{{ config(
    target_schema='sap',
    unique_key='vendor_sk',
    alias ='vendor',
    strategy='check',
    check_cols=['erdat', 'zuawa', 'akont', 'zwels', 'zterm', 'zsabe', 'fdgrv', 'intad'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'lifnr',
            'bukrs'
    ]) }} AS vendor_sk,
        *
    FROM {{ source('sap', 'vendor') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
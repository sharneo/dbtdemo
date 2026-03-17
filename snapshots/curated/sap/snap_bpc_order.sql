{% snapshot snap_bpc_order %}

{{ config(
    target_schema='sap',
    unique_key='bpc_order_sk',
    alias ='bpc_order',
    strategy='check',
    check_cols=['b631_s_coorder', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_order_sk,
        *
    FROM {{ source('sap', 'bpc_order') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
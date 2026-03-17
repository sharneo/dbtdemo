{% snapshot snap_bpc_profit_center %}

{{ config(
    target_schema='sap',
    unique_key='bpc_profit_center_sk',
    alias ='bpc_profit_center',
    strategy='check',
    check_cols=['b631_s_co_area', 'b631_s_proftctr', 'chckfl', 'datafl', 'incfl'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'sid'
    ]) }} AS bpc_profit_center_sk,
        *
    FROM {{ source('sap', 'bpc_profit_center') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
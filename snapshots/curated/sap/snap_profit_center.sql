{% snapshot snap_profit_center %}

{{ config(
    target_schema='sap',
    unique_key='profit_center_sk',
    alias ='profit_center',
    strategy='check',
    check_cols=['datbi', 'kokrs', 'datab', 'ersda', 'usnam', 'verak', 'khinr', 'ktext', 'ltext'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'prctr'
    ]) }} AS profit_center_sk,
        *
    FROM {{ source('sap', 'profit_center') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
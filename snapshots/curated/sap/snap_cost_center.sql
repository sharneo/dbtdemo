{% snapshot snap_cost_center %}

{{ config(
    target_schema='sap',
    unique_key='cost_center_sk',
    alias ='cost_center',
    strategy='check',
    check_cols=['datab', 'bkzkp', 'pkzkp', 'bukrs', 'gsber', 'kosar', 'verak', 'verak_user', 'waers', 'prctr', 'ersda', 'usnam', 'bkzks', 'bkzer', 'bkzob', 'pkzks', 'pkzer', 'vmeth', 'abtei', 'khinr', 'kompl', 'ktext', 'ltext', 'mctxt'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'kokrs',
            'kostl',
            'datbi'
    ]) }} AS cost_center_sk,
        *
    FROM {{ source('sap', 'cost_center') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
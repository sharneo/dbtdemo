{% snapshot snap_costcenter_manual_budget %}

{{ config(
    target_schema='sap',
    unique_key='costcenter_manual_budget_sk',
    alias ='costcenter_manual_budget',
    strategy='check',
    check_cols=['amount'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'profitcentre',
            'costcentre',
            'company',
            'accountno',
            'fiscyear',
            'fiscperiod',
            'sourceofdata',
            'accyear',
            'flag',
            'category'
    ]) }} AS costcenter_manual_budget_sk,
        *
    FROM {{ source('sap', 'costcenter_manual_budget') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
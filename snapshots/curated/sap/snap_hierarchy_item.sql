{% snapshot snap_hierarchy_item %}

{{ config(
    target_schema='sap',
    unique_key='hierarchy_item_sk',
    alias ='hierarchy_item',
    strategy='check',
    check_cols=['valsign', 'valoption', 'valfrom', 'valto', 'seqnr'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'setclass',
            'subclass',
            'setname',
            'lineid'
    ]) }} AS hierarchy_item_sk,
        *
    FROM {{ source('sap', 'hierarchy_item') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
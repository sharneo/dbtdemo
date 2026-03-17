{% snapshot snap_chart_of_acc %}

{{ config(
    target_schema='sap',
    unique_key='chart_of_acc_sk',
    alias ='chart_of_acc',
    strategy='check',
    check_cols=['xbilk', 'sakan', 'bilkt', 'erdat', 'ernam', 'gvtyp', 'ktoks', 'xspea', 'xspeb', 'xspep', 'glaccount_type', 'txt20', 'txt50'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'ktopl',
            'saknr'
    ]) }} AS chart_of_acc_sk,
        *
    FROM {{ source('sap', 'chart_of_acc') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
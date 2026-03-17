{% snapshot snap_mon_bal %}

{{ config(
    target_schema='sap',
    unique_key='mon_bal_sk',
    alias ='mon_bal',
    strategy='check',
    check_cols=['rpmax', 'cost_elem', 'rbukrs', 'rfarea', 'rbusa', 'kokrs', 'segment', 'sfarea', 'sbusa', 'psegment', 'tsl01', 'tsl02', 'tsl03', 'tsl04', 'tsl05', 'tsl06', 'tsl07', 'tsl08', 'tsl09', 'tsl10', 'tsl11', 'tsl12', 'tsl13', 'tsl14', 'tsl15', 'tsl16', 'hsl01', 'hsl02', 'hsl03', 'hsl04', 'hsl05', 'hsl06', 'hsl07', 'hsl08', 'hsl09', 'hsl10', 'hsl11', 'hsl12', 'hsl13', 'hsl14', 'hsl15', 'hsl16'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'ryear',
            'drcrk',
            'activ',
            'rmvct',
            'rtcur',
            'runit',
            'awtyp',
            'rldnr',
            'rrcty',
            'rvers',
            'racct',
            'rcntr',
            'prctr',
            'scntr',
            'pprctr',
            'rassc',
            'tslvt',
            'hslvt'
    ]) }} AS mon_bal_sk,
        *
    FROM {{ source('sap', 'mon_bal') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
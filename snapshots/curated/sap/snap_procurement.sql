{% snapshot snap_procurement %}

{{ config(
    target_schema='sap',
    unique_key='procurement_sk',
    alias ='procurement',
    strategy='check',
    check_cols=['aedat', 'txz01', 'bukrs', 'werks', 'matkl', 'ktmng', 'menge', 'meins', 'bprme', 'bpumz', 'bpumn', 'umrez', 'umren', 'netpr', 'peinh', 'netwr', 'brtwr', 'mwskz', 'pstyp', 'knttp', 'kzvbr', 'vrtkz', 'twrkz', 'wepos', 'weunb', 'repos', 'prdat', 'bstyp', 'effwr', 'xoblr', 'kzwi1', 'kzwi2', 'kzwi3', 'kzwi4', 'kzwi5', 'kzwi6', 'bonba', 'afnam', 'tzonrc', 'bsart', 'bsakz', 'loekz', 'statu', 'aedat1', 'ernam', 'pincr', 'lponr', 'lifnr', 'spras', 'zterm', 'zbd1t', 'zbd2t', 'zbd3t', 'zbd1p', 'zbd2p', 'ekorg', 'ekgrp', 'waers', 'wkurs', 'kufix', 'bedat', 'ihrez', 'ktwrt', 'knumv', 'kalsm', 'stafo', 'lifre', 'upinc', 'lands', 'stceg_l', 'stceg', 'absgr', 'procstat', 'rlwrt'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'ebeln',
            'ebelp'
    ]) }} AS procurement_sk,
        *
    FROM {{ source('sap', 'procurement') }}
    WHERE RECORDTYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
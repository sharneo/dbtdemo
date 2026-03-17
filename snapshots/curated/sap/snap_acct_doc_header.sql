{% snapshot snap_acct_doc_header %}

{{ config(
    target_schema='sap',
    unique_key='acct_doc_header_sk',
    alias ='acct_doc_header',
    strategy='check',
    check_cols=['gjahr', 'blart', 'bldat', 'budat', 'monat', 'cpudt', 'cputm', 'aedat', 'upddt', 'wwert', 'usnam', 'tcode', 'bvorg', 'xblnr', 'dbblg', 'dbblg_gjahr', 'dbblg_bukrs', 'stblg', 'stjah', 'bktxt', 'waers', 'kursf', 'kzwrs', 'kzkrs', 'bstat', 'xnetb', 'frath', 'xrueb', 'glvor', 'grpid', 'dokid', 'arcid', 'iblar', 'awtyp', 'awkey', 'fikrs', 'hwaer', 'hwae2', 'hwae3', 'kurs2', 'kurs3', 'basw2', 'basw3', 'umrd2', 'umrd3', 'xstov', 'stodt', 'xmwst', 'curt2', 'curt3', 'kuty2', 'kuty3', 'xsnet', 'ausbk', 'xusvr', 'duefl', 'awsys', 'txkrs', 'ctxkrs', 'lotkz', 'xwvof', 'stgrd', 'ppnam', 'ppdat', 'pptme', 'brnch', 'numpg', 'adisc', 'xref1_hd', 'xref2_hd', 'xreversal', 'reindat', 'rldnr', 'ldgrp', 'propmano', 'xblnr_alt', 'vatdate', 'doccat', 'xsplit', 'cash_alloc', 'follow_on', 'xreorg', 'subset', 'kurst', 'kursx', 'kur2x', 'kur3x', 'xmca', 'resubmission', 'logsystem_sender', 'bukrs_sender', 'belnr_sender', 'gjahr_sender', 'intsubid', 'aworg_rev', 'awref_rev', 'xreversing', 'xreversed', 'glbtgrp', 'co_vrgng', 'co_refbt', 'co_alebn', 'co_valdt', 'co_belnr_sender', 'kokrs_sender', 'field__dataaging', 'reprocessing_status_code', 'psoty', 'psoak', 'psoks', 'psosg', 'psofn', 'intform', 'intdate', 'psobt', 'psozl', 'psodt', 'psotm', 'fm_umart', 'ccins', 'ccnum', 'ssblk', 'batch', 'sname', 'sampled', 'exclude_flag', 'blind', 'offset_status', 'offset_refer_dat', 'penrc', 'knumv'],
    tags=['sap','snapshot','curated']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'bukrs',
            'belnr'
    ]) }} AS acct_doc_header_sk,
        *
    FROM {{ source('sap', 'acct_doc_header') }}
    WHERE RECORD_TYPE='D'
)

SELECT * FROM source_data

{% endsnapshot %}
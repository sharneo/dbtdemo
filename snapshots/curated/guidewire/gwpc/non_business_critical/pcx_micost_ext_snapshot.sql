{% snapshot pcx_micost_ext_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pcx_micost_ext.
                                                Source: ref('stg_raw_pcx_micost_ext')
                                                unique_key: micost_ext_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='micost_ext_sk',
    strategy='check',
    alias='pcx_micost_ext',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pcx_micost_ext']
) }}

SELECT
    micost_ext_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    fxrateconversionused,
    standardamountbilling,
    standardamountbilling_cur,
    fixedid,
    overridereason,
    overridetermamount,
    overridetermamount_cur,
    prorationmethod,
    overridable,
    updatetime,
    standardtermamountbilling,
    id,
    standardtermamountbilling_cur,
    createuserid,
    overrideamount,
    overrideamount_cur,
    beanversion,
    standardadjrate,
    updateuserid,
    roundingmode,
    actualadjrate,
    overrideadjrate,
    branchid,
    basis,
    actualbaserate,
    standardtermamount,
    standardtermamount_cur,
    publicid,
    createtime,
    ratebook,
    chargegroup,
    actualtermamount,
    actualtermamount_cur,
    effectivedate,
    actualamountbilling,
    standardamount,
    actualamountbilling_cur,
    standardamount_cur,
    overrideamountbilling,
    overrideamountbilling_cur,
    chargepattern,
    expirationdate,
    policyfxrate,
    actualamount,
    subjecttoreporting,
    overridesource,
    actualamount_cur,
    archivepartition,
    actualtermamountbilling,
    changetype,
    actualtermamountbilling_cur,
    overridetermamountbilling,
    overridetermamountbilling_cur,
    basedonid,
    numdaysinratedterm,
    standardbaserate,
    roundinglevel,
    rateamounttype,
    subtype,
    overridebaserate,
    miscellaneouslineid,
    'GWPC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_pcx_micost_ext') }}

{% endsnapshot %}
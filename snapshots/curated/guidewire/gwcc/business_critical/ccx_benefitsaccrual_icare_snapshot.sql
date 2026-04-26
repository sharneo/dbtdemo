{% snapshot ccx_benefitsaccrual_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_benefitsaccrual_icare.
                                                Source: ref('stg_raw_ccx_benefitsaccrual_icare')
                                                unique_key: benefitsaccrual_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='benefitsaccrual_icare_sk',
    strategy='check',
    alias='ccx_benefitsaccrual_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_benefitsaccrual_icare']
) }}

SELECT
    benefitsaccrual_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    noncomplianceamount,
    createuserid,
    exposureid,
    firstentitlementweeks,
    section38amount,
    publicid,
    secondentitlementweeks,
    totalweekspaid,
    beanversion,
    archivepartition,
    totalamountpaid,
    postsecondweek,
    createtime,
    retired,
    postsecondamount,
    updateuserid,
    secondentitlementamount,
    firstentitlementamount,
    updatetime,
    id,
    noncomplianceweek,
    section38week,
    section41week,
    section41amount,
    ewsec40weeks,
    ewsec36days,
    ewsec37days,
    ewsec36amount,
    ewsec37amount,
    ewsec38days,
    ewsec38amount,
    ewsec37weeks,
    ewtotaldays,
    ewtotalamount,
    ewtotalweeks,
    ewsec40days,
    ewsec40amount,
    ewsec38weeks,
    ewsec36weeks,
    'GWCC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_ccx_benefitsaccrual_icare') }}

{% endsnapshot %}
{% snapshot bc_basedist_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_basedist.
                                                Source: ref('stg_raw_bc_basedist')
                                                unique_key: basedist_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='basedist_sk',
    strategy='check',
    alias='bc_basedist',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_basedist']
) }}

SELECT
    basedist_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    createuserid,
    bankrefdetail_icare,
    publicid,
    writeoffamount,
    beanversion,
    frozenbyarchiving,
    writeoffamount_cur,
    retired,
    createtime,
    applieddate,
    updateuserid,
    invoicenumber_icare,
    distributeddate,
    currency,
    netdisttoinvoiceitems_cur,
    netdistributedtoinvoiceitems,
    updatetime,
    subtype,
    id,
    reversaldate,
    netinsuspense,
    netinsuspense_cur,
    'GWBC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_bc_basedist') }}

{% endsnapshot %}
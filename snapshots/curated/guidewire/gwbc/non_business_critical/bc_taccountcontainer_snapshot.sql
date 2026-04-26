{% snapshot bc_taccountcontainer_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_taccountcontainer.
                                                Source: ref('stg_raw_bc_taccountcontainer')
                                                unique_key: taccountcontainer_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='taccountcontainer_sk',
    strategy='check',
    alias='bc_taccountcontainer',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_taccountcontainer']
) }}

SELECT
    taccountcontainer_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    publicid,
    createtime,
    unearneddenorm,
    unearneddenorm_cur,
    currency,
    expensedenorm,
    updatetime,
    expensedenorm_cur,
    revenuedenorm,
    revenuedenorm_cur,
    id,
    createuserid,
    negativewriteoffdenorm,
    billeddenorm,
    negativewriteoffdenorm_cur,
    billeddenorm_cur,
    writeoffexpensedenorm,
    writeoffexpensedenorm_cur,
    archivepartition,
    beanversion,
    updateuserid,
    duedenorm,
    duedenorm_cur,
    reservedenorm,
    subtype,
    reservedenorm_cur,
    unbilleddenorm,
    unbilleddenorm_cur,
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
FROM {{ ref('stg_raw_bc_taccountcontainer') }}

{% endsnapshot %}
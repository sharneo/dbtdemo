{% snapshot bc_transaction_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_transaction.
                                                Source: ref('stg_raw_bc_transaction')
                                                unique_key: transaction_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='transaction_sk',
    strategy='check',
    alias='bc_transaction',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_transaction']
) }}

SELECT
    transaction_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    reversed,
    commissionamount,
    publicid,
    commissionamount_cur,
    reason,
    createtime,
    transactiondate,
    commissionamountchanged,
    currency,
    commissionamountchanged_cur,
    reversalreason,
    writeoffchannel,
    updatetime,
    amount,
    amount_cur,
    id,
    createuserid,
    transactionnumberdenorm,
    transactionnumber,
    beanversion,
    archivepartition,
    retired,
    updateuserid,
    subtype,
    basis,
    basis_cur,
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
FROM {{ ref('stg_raw_bc_transaction') }}

{% endsnapshot %}
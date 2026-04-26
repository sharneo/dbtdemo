{% snapshot cc_transaction_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_transaction.
                                                Source: ref('stg_raw_cc_transaction')
                                                unique_key: transaction_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='transaction_sk',
    strategy='check',
    alias='cc_transaction',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_transaction']
) }}

SELECT
    transaction_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    paymenttype,
    loadcommandid,
    matter,
    closeclaim,
    publicid,
    submitdate,
    costtype,
    createtime,
    recoverytype_icare,
    reservingcurrency,
    costcategory,
    reservelineid,
    currency,
    recoverycategory,
    updatetime,
    claimid,
    claimtoreportingexchangerate,
    recoverycodingid,
    id,
    transactionsetid,
    doesnoterodereserves,
    exposureid,
    createuserid,
    checkid,
    lifecyclestate,
    beanversion,
    archivepartition,
    retired,
    payerdenormid,
    updateuserid,
    invoicenumber_icare,
    status,
    comments,
    transtoreservingexchangerate,
    subtype,
    oboclaimcontactid,
    transtoclaimexchangerate,
    payeetype_icare,
    claimcontactid,
    closeexposure,
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
FROM {{ ref('stg_raw_cc_transaction') }}

{% endsnapshot %}
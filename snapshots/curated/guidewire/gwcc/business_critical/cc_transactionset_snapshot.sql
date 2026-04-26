{% snapshot cc_transactionset_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_transactionset.
                                                Source: ref('stg_raw_cc_transactionset')
                                                unique_key: transactionset_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='transactionset_sk',
    strategy='check',
    alias='cc_transactionset',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_transactionset']
) }}

SELECT
    transactionset_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    createuserid,
    publicid,
    beanversion,
    archivepartition,
    createtime,
    voidtransactionreason_icare,
    retired,
    documentlinkableid,
    transferfromclaimnumber_icare,
    recurrenceid,
    updateuserid,
    approvaldate,
    adjustmentpayment_icare,
    approvalstatus,
    updatetime,
    manualtransactionreason_icare,
    claimid,
    requestinguserid,
    subtype,
    transfertoclaimnumber_icare,
    id,
    paymenttype_icare,
    managingentityname_icare,
    invoicetype_ext,
    excessrefundpayment_ext,
    issavedcheck_ext,
    weeklybenefitrationale_ext,
    sportingtransactionreason_ext,
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
FROM {{ ref('stg_raw_cc_transactionset') }}

{% endsnapshot %}
{% snapshot cc_deductible_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_deductible.
                                                Source: ref('stg_raw_cc_deductible')
                                                unique_key: deductible_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='deductible_sk',
    strategy='check',
    alias='cc_deductible',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_deductible']
) }}

SELECT
    deductible_sk,
    hash_key,
    createuserid,
    coverageid,
    publicid,
    beanversion,
    archivepartition,
    createtime,
    retired,
    paid,
    editreason,
    updateuserid,
    currency,
    invoicenumber_icare,
    accountnumber_icare,
    overridden,
    updatetime,
    waived,
    claimid,
    amount,
    invoiceamountsenttobc,
    id,
    crnnumber_ext,
    invoicedate_ext,
    paymentduedate_ext,
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
FROM {{ ref('stg_raw_cc_deductible') }}

{% endsnapshot %}
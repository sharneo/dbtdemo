{% snapshot bc_invoice_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_invoice.
                                                Source: ref('stg_raw_bc_invoice')
                                                unique_key: invoice_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='invoice_sk',
    strategy='check',
    alias='bc_invoice',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_invoice']
) }}

SELECT
    invoice_sk,
    hash_key,
    loadcommandid,
    netamountpaid,
    netamountpaid_cur,
    accountid,
    currency,
    updatetime,
    id,
    amountdue,
    colunappliedamount,
    unappliedamount,
    amountdue_cur,
    colunappliedamount_cur,
    unappliedamount_cur,
    createuserid,
    colremainingbalance,
    remainingbalance,
    colremainingbalance_cur,
    remainingbalance_cur,
    beanversion,
    retired,
    updateuserid,
    netamount,
    invoicenumber,
    netamount_cur,
    adhoc,
    publicid,
    allinvoiceitemsexactlypaid,
    eventdate,
    invoicestreamid,
    createtime,
    numresends,
    amount,
    amount_cur,
    paymentduedate,
    frozenbyarchiving,
    primarydirectbillearned,
    primarydirectbillearned_cur,
    netamountwrittenoff,
    netamountwrittenoff_cur,
    paymentreminderevaluator_icare,
    status,
    coloutstandingamount,
    outstandingamount,
    invoicenumberdenorm,
    coloutstandingamount_cur,
    outstandingamount_cur,
    subtype,
    description,
    iscollapsedinvoice_icare,
    recoveryinvoicetype_icare,
    eftrefnumber_icare,
    userinputref_icare,
    initialrnquarterly,
    includeforreferral_ext,
    rnswinvoicestatus_ext,
    rnswreferralackdate_ext,
    rnswreferraldate_ext,
    rnswreferralstatus_ext,
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
FROM {{ ref('stg_raw_bc_invoice') }}

{% endsnapshot %}
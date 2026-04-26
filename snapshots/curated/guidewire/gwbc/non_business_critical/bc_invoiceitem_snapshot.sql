{% snapshot bc_invoiceitem_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_invoiceitem.
                                                Source: ref('stg_raw_bc_invoiceitem')
                                                unique_key: invoiceitem_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='invoiceitem_sk',
    strategy='check',
    alias='bc_invoiceitem',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_invoiceitem']
) }}

SELECT
    invoiceitem_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    installmentnumber,
    primarycommissionamount,
    primarycommissionamount_cur,
    invoiceid,
    lineitemnumber,
    currency,
    grossamountwrittenoff,
    grossamountwrittenoff_cur,
    updatetime,
    id,
    createuserid,
    paidamount,
    paidamount_cur,
    invoicedateoverride,
    beanversion,
    retired,
    haschargebilledtransaction,
    custompaymentgroup,
    updateuserid,
    promisedandpaidamount,
    promisedandpaidamount_cur,
    canbepromisedmorebyagencybill,
    paymentexceptionlock,
    grosssettled,
    paymentexceptiondate,
    haschargeduetransaction,
    type,
    promiseexceptionlock,
    primarywrittenoffcmsn_cur,
    promiseexceptiondate,
    reversed,
    exceptioncomments,
    publicid,
    eventdate,
    createtime,
    policyperiodid,
    chargeid,
    hasbeenpaymentexception,
    promisedcommission,
    promisedcommission_cur,
    canbepaidmorebyagencybill,
    amount,
    amount_cur,
    paymentexceptionlockdate,
    primarywrittenoffcommission,
    archivepartition,
    primarydirectbillearned,
    primarydirectbillearned_cur,
    promiseexceptionlockdate,
    primaryagencybillretained,
    primaryagencybillretained_cur,
    comments,
    description,
    rcwaivedamount_icare_cur,
    waivedamount,
    rccreditnoteamount_icare,
    rccreditnoteamount_icare_cur,
    rcpaymentarrangementref_icare,
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
FROM {{ ref('stg_raw_bc_invoiceitem') }}

{% endsnapshot %}
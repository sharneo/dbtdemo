{% snapshot bc_paymentinstrument_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_paymentinstrument.
                                                Source: ref('stg_raw_bc_paymentinstrument')
                                                unique_key: paymentinstrument_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='paymentinstrument_sk',
    strategy='check',
    alias='bc_paymentinstrument',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_paymentinstrument']
) }}

SELECT
    paymentinstrument_sk,
    hash_key,
    loadcommandid,
    createuserid,
    immutable,
    publicid,
    chequenumber_icare,
    paymentmethod,
    beanversion,
    accountid,
    createtime,
    retired,
    isactive_icare,
    producerid,
    updateuserid,
    detail,
    bankaccountnumber_icare,
    bsbnumber_icare,
    updatetime,
    token,
    id,
    description,
    accountholdername_icare,
    cardholdername_icare,
    creditcardissuer,
    cardnumber_icare,
    bankaccounttype_icare,
    expirymonth_icare,
    expiryyear_icare,
    tokennumber_icare,
    camtbankaccnumber_icare,
    rnswpayment_ext,
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
FROM {{ ref('stg_raw_bc_paymentinstrument') }}

{% endsnapshot %}
{% snapshot bc_disbursement_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_disbursement.
                                                Source: ref('stg_raw_bc_disbursement')
                                                unique_key: disbursement_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='disbursement_sk',
    strategy='check',
    alias='bc_disbursement',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_disbursement']
) }}

SELECT
    disbursement_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    agencycyclepaymentid,
    loadcommandid,
    internalcomment,
    chequepresenteddate_icare,
    publicid,
    reason,
    bankbookingdate_icare,
    accountid,
    paymentinstrumentid,
    createtime,
    currency,
    collateralid,
    updatetime,
    amount,
    voidreason,
    refnumberdenorm,
    amount_cur,
    requestinguserid,
    id,
    returntosender_icare,
    refnumber,
    paytodenorm,
    createuserid,
    duedate,
    payto,
    bankrefdetail_icare,
    unappliedfundid,
    chequenumber_icare,
    closedate,
    suspensepaymentid,
    beanversion,
    retired,
    disbursementnumber,
    reportinggroupid,
    producerid,
    updateuserid,
    approvaldate,
    status,
    mailto,
    address,
    approvalstatus,
    memo,
    subtype,
    mailto_icare,
    iscor_icare,
    userinputref_icare,
    awaitingeftdetails_ext,
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
FROM {{ ref('stg_raw_bc_disbursement') }}

{% endsnapshot %}
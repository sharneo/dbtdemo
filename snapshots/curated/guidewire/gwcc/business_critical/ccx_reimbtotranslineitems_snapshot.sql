{% snapshot ccx_reimbtotranslineitems_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_reimbtotranslineitems.
                                                Source: ref('stg_raw_ccx_reimbtotranslineitems')
                                                unique_key: reimbtotranslineitems_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='reimbtotranslineitems_sk',
    strategy='check',
    alias='ccx_reimbtotranslineitems',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_reimbtotranslineitems']
) }}

SELECT
    reimbtotranslineitems_sk,
    hash_key,
    loadcommandid,
    overpaymentreimbursement_icare,
    createuserid,
    publicid,
    transactionlineitem,
    invoiceamount,
    writeoffamountallocate,
    beanversion,
    archivepartition,
    retired,
    createtime,
    updateuserid,
    reimbursementamount,
    creditamount,
    updatetime,
    waived,
    id,
    paymentamountallocate,
    recoverywbrate,
    recoverydeductions,
    recoveryhoursworked,
    recoverypayg,
    recoverypiawe,
    recoveryearnings,
    dateto_ext,
    countofweeks,
    recoveryawe_ext,
    recoverypercentageofweek_ext,
    recoveryweeklyactualrate_ext,
    datefrom_ext,
    adjustmentflag_ext,
    previouspaidamount_ext,
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
FROM {{ ref('stg_raw_ccx_reimbtotranslineitems') }}

{% endsnapshot %}
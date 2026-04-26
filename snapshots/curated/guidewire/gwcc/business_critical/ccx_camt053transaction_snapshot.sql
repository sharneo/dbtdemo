{% snapshot ccx_camt053transaction_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_camt053transaction.
                                                Source: ref('stg_raw_ccx_camt053transaction')
                                                unique_key: camt053transaction_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='camt053transaction_sk',
    strategy='check',
    alias='ccx_camt053transaction',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_camt053transaction']
) }}

SELECT
    camt053transaction_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    reversalindicator,
    paymentinformationid,
    debtorname,
    publicid,
    processingstatus,
    claimnumber,
    bookingdate,
    createtime,
    statementid,
    westpacuidnontransformed,
    transactionproprietarycode,
    transactiondomaincode,
    transactionstatus,
    gwccpaymentid,
    checknumber,
    debtoraccountid,
    paymentmethodcode,
    reconciliationstatus,
    additionalinfo,
    updatetime,
    transactionproprietaryissuer,
    id,
    creditdebitindicator,
    valuedate,
    endtoendid,
    createuserid,
    matchedpayment,
    merchantid,
    resolutiontype,
    beanversion,
    retired,
    instructionid,
    statementdatetime,
    archivedcheckpublicid,
    reasontounknown,
    recallid,
    creditoraccountid,
    westpacuid,
    updateuserid,
    camt053_id,
    transactionsubfamilycode,
    transactiondomainfamilycode,
    controlrecordmessageid,
    transactionamount,
    memo,
    paymentsource,
    triggernotification,
    provideruniquerefid,
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
FROM {{ ref('stg_raw_ccx_camt053transaction') }}

{% endsnapshot %}
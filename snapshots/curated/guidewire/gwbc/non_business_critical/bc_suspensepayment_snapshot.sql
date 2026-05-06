{% snapshot bc_suspensepayment_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_suspensepayment.
                                                Source: ref('stg_raw_bc_suspensepayment')
                                                unique_key: suspensepayment_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='suspensepayment_sk',
    strategy='check',
    alias='bc_suspensepayment',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_suspensepayment']
) }}

SELECT
    suspensepayment_sk,
    hash_key,
    policyperiodappliedtoid,
    producerappliedtoid,
    publicid,
    accountappliedtoid,
    paymentinstrumentid,
    createtime,
    offeroption,
    currency,
    updatetime,
    refnumber_icare,
    amount,
    refnumberdenorm,
    amount_cur,
    hiddentaccountcontainerid,
    producernamedenorm,
    id,
    refnumber,
    producername,
    createuserid,
    bankrefdetail_icare,
    offernumber,
    beanversion,
    retired,
    reportinggroupid,
    updateuserid,
    status,
    originalbankrefdetail_icare,
    accountnumberdenorm,
    paymentdate,
    invoicenumber,
    accountnumber,
    paymentmoneyreceivedid,
    policynumberdenorm,
    appliedbyuserid,
    policynumber,
    description,
    reversedbyuserid,
    offeringtype_icare,
    createdfromdbmr_icare,
    lobaccounttypecode,
    userinputref_icare,
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
FROM {{ ref('stg_raw_bc_suspensepayment') }}

{% endsnapshot %}
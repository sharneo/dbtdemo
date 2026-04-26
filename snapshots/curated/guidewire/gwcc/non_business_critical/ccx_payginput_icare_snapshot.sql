{% snapshot ccx_payginput_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_payginput_icare.
                                                Source: ref('stg_raw_ccx_payginput_icare')
                                                unique_key: payginput_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='payginput_icare_sk',
    strategy='check',
    alias='ccx_payginput_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_payginput_icare']
) }}

SELECT
    payginput_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    publicid,
    entitlementamount,
    createtime,
    tfnreported,
    dependentspouse,
    accumulatedhelpdebt,
    updatetime,
    id,
    accumulatedfsdebt,
    createuserid,
    checkid,
    archivepartition,
    beanversion,
    medicarelevy,
    domesticresident,
    updateuserid,
    taxfreethreshold,
    currententitlementamt,
    currentyear,
    payeeage,
    nbrdepchldrnformedcrlevy,
    entlmntamntaftrddctns,
    taxoffset,
    paycycle,
    wagebenefitstartdate,
    medicareexemption,
    paygscale,
    wagebenefitenddate,
    seniorpnsnrstatus,
    transactiondate_icare,
    effectivefromvariation_icare,
    upwardvariationpercent_icare,
    upwardvariationamt_icare,
    downwardvariationpercent_icare,
    effectivetovariation_icare,
    downwardvariationamt_icare,
    withholdingvariation_icare,
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
FROM {{ ref('stg_raw_ccx_payginput_icare') }}

{% endsnapshot %}
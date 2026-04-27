{% snapshot pcx_claim_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pcx_claim_icare.
                                                Source: ref('stg_raw_pcx_claim_icare')
                                                unique_key: claim_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='claim_icare_sk',
    strategy='check',
    alias='pcx_claim_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pcx_claim_icare']
) }}

SELECT
    claim_icare_sk,
    hash_key,
    loadcommandid,
    losshistoryentry,
    publicid,
    claimnumber,
    overwrittenclaimscost,
    overwrittenclaimscost_cur,
    createtime,
    policyid,
    policyperiodid,
    totalpaid,
    lossdate,
    totalpaid_cur,
    rtwi,
    rtwi_cur,
    updatetime,
    id,
    createuserid,
    beanversion,
    archivepartition,
    cprclaimscost,
    cprclaimscost_cur,
    directwageid,
    claimstatus,
    rtwipercentage,
    updateuserid,
    claimantname,
    recoveriesorestimates_cur,
    recoveriesorestimates_amt,
    policytermid,
    isczeroclaim,
    isexternaldata,
    addedtopolicyperiodterm,
    estimates_cur,
    estimates_amt,
    isclaimcenterclaim,
    policynumber,
    'GWPC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_pcx_claim_icare') }}

{% endsnapshot %}
{% snapshot ccx_commonlaw_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_commonlaw_icare.
                                                Source: ref('stg_raw_ccx_commonlaw_icare')
                                                unique_key: commonlaw_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='commonlaw_icare_sk',
    strategy='check',
    alias='ccx_commonlaw_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_commonlaw_icare']
) }}

SELECT
    commonlaw_icare_sk,
    hash_key,
    loadcommandid,
    ctmresolutiontype,
    section74noticedate_icare,
    publicid,
    createtime,
    dateparticrece_icare,
    nocdatereceived_icare,
    dateparticrequ_icare,
    outcome_icare,
    initialdatereceived_icare,
    typeofdispute_icare,
    settlementdate_icare,
    initialnoticeofintention_icare,
    updatetime,
    claimdisputed_icare,
    noticeofintention_icare,
    mediationcertificate_icare,
    id,
    namereference_icare,
    reasonreopened_icare,
    prefilingdefencedate_icare,
    defectivenoticesentdate_icare,
    litigationstarted,
    mediationdate_icare,
    createuserid,
    noticeofclaimnotreceived_icare,
    recoverypotential_icare,
    settlement_icare,
    dateofinstruction_icare,
    validationlevel_icare,
    prefilingdatereceived_icare,
    statementofclaimdaterec_icare,
    beanversion,
    noticeofclaim_icare,
    retired,
    updateuserid,
    settlementcost_icare,
    initialnoticenotrec_icare,
    ourlegalcosts_icare,
    mattertype_icare,
    subtype,
    trialdate_icare,
    resultofmediation_icare,
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
FROM {{ ref('stg_raw_ccx_commonlaw_icare') }}

{% endsnapshot %}
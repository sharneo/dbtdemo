{% snapshot ccx_wpiassessrecord_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_wpiassessrecord_icare.
                                                Source: ref('stg_raw_ccx_wpiassessrecord_icare')
                                                unique_key: wpiassessrecord_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='wpiassessrecord_icare_sk',
    strategy='check',
    alias='ccx_wpiassessrecord_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_wpiassessrecord_icare']
) }}

SELECT
    wpiassessrecord_icare_sk,
    hash_key,
    loadcommandid,
    publicid,
    wpiresult_icare,
    wpiassessment_icareid,
    createtime,
    documentlinkableid,
    claimedbhlperventage_icare,
    updatetime,
    wpiassessmentstate_icare,
    id,
    settlementtype_icare,
    offeredbhlpercentage_icare,
    createuserid,
    beanversion,
    archivepartition,
    letterofofferdate_icare,
    retired,
    medicare_icare,
    offeredwpipercentage_icare,
    interest,
    relevantparticularsdate_icare,
    actiontype_icare,
    offeraccepteddate_icare,
    approvaldate_icare,
    complyingagreementdate_icare,
    updateuserid,
    claimedpercentage_icare,
    approved_icare,
    hasbackinjury_icare,
    maxmedimprovreached_icare,
    settlementamount_icare,
    s66receiveddate_icare,
    bhlresult_icare,
    offeraccepted_icare,
    assessedwpifors66,
    finallumpsumamount,
    dateoffered,
    claimeds67amount,
    offeredamount,
    lumpsumamountoffered,
    s67settlementamount,
    legacycreatetime,
    legacyupdatetime,
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
FROM {{ ref('stg_raw_ccx_wpiassessrecord_icare') }}

{% endsnapshot %}
{% snapshot ccx_wcdinternalreview_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_wcdinternalreview_icare.
                                                Source: ref('stg_raw_ccx_wcdinternalreview_icare')
                                                unique_key: wcdinternalreview_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='wcdinternalreview_icare_sk',
    strategy='check',
    alias='ccx_wcdinternalreview_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_wcdinternalreview_icare']
) }}

SELECT
    wcdinternalreview_icare_sk,
    hash_key,
    loadcommandid,
    reviewdetailsid,
    assessedasindefinitelyunable,
    outcomereasoning,
    solicitorid,
    updatetime,
    newwcdrequired,
    id,
    fairnoticerequired,
    reasoning,
    createuserid,
    working15hourperweektext,
    beanversion,
    retired,
    decisionissuedate,
    updateuserid,
    furtherinfoimpactsdecision,
    stayapplicable,
    dateapplicationreceived,
    section38eligibility,
    publicid,
    internalreviewcompleteddate,
    earningperweek,
    createtime,
    reviewduedate,
    wpipercentage,
    applicationlodgedby,
    acknowledgementletterdate,
    assessedasunabletext,
    earningse,
    reviewerid,
    medentitlement,
    section38eligible,
    earningscapacity,
    archivepartition,
    eligibilityeffectivedate,
    furtherinfosubmitted,
    section54stillactive,
    weeklypaymentimpact,
    assessedhours,
    draftdecisioncompleted,
    responsedate,
    earningperweektext,
    reviewrequestedwithin30days,
    followupflag,
    working15hourperweek,
    casenumber,
    decisioneffectivedate,
    reviewoutcome,
    stayexpirydate,
    sections80stillactive,
    piawe,
    withins80noticeperiod,
    internalreviewstatus,
    decisionenddate_ext,
    cwwrsuitableemployment,
    discretionaryentitlement,
    abilitytoearn,
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
FROM {{ ref('stg_raw_ccx_wcdinternalreview_icare') }}

{% endsnapshot %}
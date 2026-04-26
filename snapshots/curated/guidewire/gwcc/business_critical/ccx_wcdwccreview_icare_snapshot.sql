{% snapshot ccx_wcdwccreview_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_wcdwccreview_icare.
                                                Source: ref('stg_raw_ccx_wcdwccreview_icare')
                                                unique_key: wcdwccreview_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='wcdwccreview_icare_sk',
    strategy='check',
    alias='ccx_wcdwccreview_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_wcdwccreview_icare']
) }}

SELECT
    wcdwccreview_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    reviewdetailsid,
    reviewer,
    assessedasindefinitelyunable,
    outcomereasoning,
    submissionsentdate,
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
    wcccompleteddate,
    dateapplicationreceived,
    section38eligibility,
    publicid,
    internalreviewcompleteddate,
    earningperweek,
    sections80stillactive,
    createtime,
    reviewduedate,
    wpipercentage,
    applicationlodgedby,
    acknowledgementletterdate,
    assessedasunabletext,
    piawe,
    earningse,
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
    withins80noticeperiod,
    reviewoutcome,
    stayexpirydate,
    wccreviewstatus,
    wccreviewpiaweid,
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
FROM {{ ref('stg_raw_ccx_wcdwccreview_icare') }}

{% endsnapshot %}
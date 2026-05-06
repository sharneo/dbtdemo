{% snapshot ccx_workcapdecision_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_workcapdecision_icare.
                                                Source: ref('stg_raw_ccx_workcapdecision_icare')
                                                unique_key: workcapdecision_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='workcapdecision_icare_sk',
    strategy='check',
    alias='ccx_workcapdecision_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_workcapdecision_icare']
) }}

SELECT
    workcapdecision_icare_sk,
    hash_key,
    loadcommandid,
    evaluationprobableoutcome,
    assessedasindefinitelyunable,
    paymentreductionreminderflag,
    furtherinforeasoning,
    effectivedatefairnotice,
    reviewtype,
    currentweeklyhours,
    updatetime,
    reviewperiod,
    id,
    fairnoticerequired,
    assessmentreminderflag,
    createuserid,
    working15hourperweektext,
    beanversion,
    retired,
    hoursperday,
    updateuserid,
    effectivedatedecision,
    furtherinfoimpactsdecision,
    decisionreasoning,
    issuedatefairnotice,
    publicid,
    currentworkcapacity,
    earningperweek,
    createtime,
    documentlinkableid,
    wpipercentage,
    referencenumber,
    assessedasunabletext,
    claimid,
    daysperweek,
    earningse,
    medentitlement,
    section38eligible,
    currentweeklyearnings,
    issuedatedecision,
    earningscapacity,
    issuedatefollowflag,
    issuedatereminderflag,
    relatedto,
    draftfairnoticeready,
    archivepartition,
    eligibilityeffectivedate,
    furtherinfosubmitted,
    weeklypaymentimpact,
    assessedhours,
    draftdecisioncompleted,
    status,
    earningperweektext,
    working15hourperweek,
    currentworkstatus,
    effectivedatefairnoticeflag,
    wcdpiawe,
    assessmentreminderweekscount,
    scheduleassessmentreminderflag,
    s38eligibilityissuedate,
    rejectedsuitableemployment,
    cwwrsuitableemployment,
    acceptedofferemployment,
    discretionaryentitlement,
    abilitytoearn,
    legacycreatetime,
    legacyupdatetime,
    decisionenddate_ext,
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
FROM {{ ref('stg_raw_ccx_workcapdecision_icare') }}

{% endsnapshot %}
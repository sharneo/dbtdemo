{% snapshot ccx_piaweindexremreport_ext_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_piaweindexremreport_ext.
                                                Source: ref('stg_raw_ccx_piaweindexremreport_ext')
                                                unique_key: piaweindexremreport_ext_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='piaweindexremreport_ext_sk',
    strategy='check',
    alias='ccx_piaweindexremreport_ext',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_piaweindexremreport_ext']
) }}

SELECT
    piaweindexremreport_ext_sk,
    hash_key,
    loadcommandid,
    latestwq3indexedperiod,
    s38eligibilityeffectivedate,
    earliestwbpaidfromdate,
    weeklybenefitscessationdate,
    claimnumber,
    caseownername,
    piawetype,
    cocfitness,
    updatetime,
    id,
    createuserid,
    irweeklypaymentimpact,
    mename,
    retirementdate,
    beanversion,
    dateofloss,
    retired,
    wcddetails,
    claimant,
    resultofinjury,
    didtheclaimindexinwq3,
    workstatusstartdate,
    noncomplianceeffectivedate,
    piaweamount,
    cocconsultationdate,
    claimstatus,
    updateuserid,
    workstatuscode,
    reasonnotindexdofi,
    adjustment_required,
    cocenddate,
    hasmedicalother,
    numberofperiodsindexed,
    liabilitystatus,
    publicid,
    createtime,
    picreviewweeklypaymentimpact,
    retirementcessationdateweekly,
    compliancedate,
    wpipercentage,
    wageloss,
    weekspaid,
    hasweeklybenefitsindemnity,
    wcdtype,
    weeklybenefitenddate,
    picreviewstatus,
    effectivedate,
    picreviewdecisioneffectivedate,
    irstatus,
    reasonnowbpaid,
    reportdate,
    noncompliancetype,
    reasonnomissingpoints,
    dateoffirstincapacity,
    settlementpaymentissuedate,
    caseownerteamname,
    weeklypaymentimpact,
    reasonnotwq3ac03decrease,
    statutorymaximumrateapplies,
    irdecisioneffectivedate,
    insuredname,
    datepaidto,
    s38eligibility,
    cocstartdate,
    decisioneffectivedate,
    noncompliancecessationdate,
    icdcodeanddescription,
    wcdrefno,
    earliestindexationpointwq3,
    s38refno,
    doesclaimflowtowq1orwq2,
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
FROM {{ ref('stg_raw_ccx_piaweindexremreport_ext') }}

{% endsnapshot %}
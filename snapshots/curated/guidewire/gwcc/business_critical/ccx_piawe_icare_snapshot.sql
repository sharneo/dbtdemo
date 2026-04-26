{% snapshot ccx_piawe_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_piawe_icare.
                                                Source: ref('stg_raw_ccx_piawe_icare')
                                                unique_key: piawe_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='piawe_icare_sk',
    strategy='check',
    alias='ccx_piawe_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'ccx_piawe_icare']
) }}

SELECT
    piawe_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    uncappedpiawepre52,
    totaloeleaveweeks,
    deflatedpiawepre52,
    publicid,
    ordinaryearnings_icare,
    totalshiftotallowanceweekrp,
    createtime,
    piawecomment,
    partialpiawelinkid,
    totalovertimeleaveamount,
    piawetype_icare,
    rpenddate,
    totaloeweekrp,
    piawelater52_icare,
    schedule3item,
    effectivedate_icare,
    totalshiftleaveamount,
    rpstartdate,
    overtimeallowance_icare,
    piawedeactivated_icare,
    piawereason_icare,
    updatetime,
    leavewagerecordsrecd,
    totalovertimeweek,
    deflatedpiawepost52,
    id,
    schedule3applicable,
    relevantperiod,
    totaloeweekoverrp,
    draft,
    createuserid,
    exposureid,
    beanversion,
    archivepartition,
    retired,
    totalleaveweeksrp,
    updateuserid,
    employmentdataid,
    totalshiftweek,
    piawefirst52_icare,
    comments,
    totaloeleaveamount,
    uncappedpiawepost52,
    statminapplied,
    hoursworked,
    noteid,
    relevantperiodlegreforms2018,
    agreementreceiveddate,
    agreementapproved,
    agreementwithdrawdate,
    documentlinkableid,
    autocreated,
    agreementapproveddate,
    needpiaweworksheet,
    initialpiawe,
    workcaprefid,
    isdocumentusedforpiawe_ext,
    reason_ext,
    isindexationdecronreopen,
    legacycreatetime,
    legacyupdatetime,
    piaweindexationid,
    isinjuredworkerbelow21,
    isinjuredworkerapprentice,
    wcdpiaweamount,
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
FROM {{ ref('stg_raw_ccx_piawe_icare') }}

{% endsnapshot %}
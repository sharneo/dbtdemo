{% snapshot cc_workcomp_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_workcomp.
                                                Source: ref('stg_raw_cc_workcomp')
                                                unique_key: workcomp_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='workcomp_sk',
    strategy='check',
    alias='cc_workcomp',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_workcomp']
) }}

SELECT
    workcomp_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    dateofemployeerepresentation,
    publicid,
    medicalreport,
    createtime,
    equipmentused,
    reasonableexcuse_icare,
    activityperformed,
    accidentpremises,
    compensable,
    updatetime,
    discontinuedfringebenefits,
    id,
    othertriagequestions_icareid,
    fulldenialeffectivedate,
    createuserid,
    timelossreport,
    jurisdictionclaimnumber,
    insuredreportnumber,
    emptriagequestions_icareid,
    beanversion,
    archivepartition,
    triagequestionssummary_icare,
    deathreport,
    retired,
    injtriagequestions_icareid,
    medrecreleaseauth,
    partialdenialreason,
    illnessrelatedtoexposure,
    updateuserid,
    waitingperiodapplied,
    classcodebylocation,
    employerliability,
    initialtreatment,
    doctriagequestions_icareid,
    accidentlocationtype_icare,
    relationshiptotheinjured_icare,
    overallriskrating_ext,
    cmtriagequestions_extid,
    propertydamageclaim_ext,
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
FROM {{ ref('stg_raw_cc_workcomp') }}

{% endsnapshot %}
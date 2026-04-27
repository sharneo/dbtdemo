{% snapshot pcx_directwage_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pcx_directwage_icare.
                                                Source: ref('stg_raw_pcx_directwage_icare')
                                                unique_key: directwage_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='directwage_icare_sk',
    strategy='check',
    alias='pcx_directwage_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pcx_directwage_icare']
) }}

SELECT
    directwage_icare_sk,
    hash_key,
    loadcommandid,
    legacyddl,
    auditedappwages,
    wages_icare,
    totalwages,
    wages_icare_cur,
    fixedid,
    pac,
    updatetime,
    noofunits,
    auditedasbestoswages,
    id,
    noofinjuredemployees,
    initialexclusionscreated,
    createuserid,
    beanversion,
    noofemployees,
    grossvalue,
    updateuserid,
    wicrate,
    location,
    referencedateinternal,
    wcline_icare,
    businessdescription,
    branchid,
    auditedtotalwages,
    initialcoveragescreated,
    publicid,
    total,
    legacywicrate,
    createtime,
    noofapp,
    appwages,
    wic,
    effectivedate,
    claimyears1,
    claimyears1_cur,
    claimyears2,
    claimyears2_cur,
    claimyears3,
    claimyears3_cur,
    expirationdate,
    costcenter_icare,
    auditedgrossvalue,
    archivepartition,
    changetype,
    directwageid,
    initialconditionscreated,
    basedonid,
    auditedunits,
    btpyear1,
    btpyear1_cur,
    btpyear2,
    btpyear2_cur,
    btpyear3,
    btpyear3_cur,
    preferredcoveragecurrency,
    description,
    ddlcontribution,
    auditednoofunits,
    auditednoofemployees,
    auditedlabourcomp,
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
FROM {{ ref('stg_raw_pcx_directwage_icare') }}

{% endsnapshot %}
{% snapshot pcx_losshistforrating_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pcx_losshistforrating_icare.
                                                Source: ref('stg_raw_pcx_losshistforrating_icare')
                                                unique_key: losshistforrating_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='losshistforrating_icare_sk',
    strategy='check',
    alias='pcx_losshistforrating_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pcx_losshistforrating_icare']
) }}

SELECT
    losshistforrating_icare_sk,
    hash_key,
    locationname,
    loadcommandid,
    effectivedatedfieldsid,
    publicid,
    wages1,
    wages2,
    wages1_cur,
    wages3,
    wages2_cur,
    createtime,
    wages3_cur,
    fixedid,
    islossadded,
    effectivedate,
    updatetime,
    claimyears1,
    claimyears1_cur,
    claimyears2,
    claimyears2_cur,
    claimyears3,
    claimyears3_cur,
    id,
    losshistoryentryid,
    expirationdate,
    createuserid,
    costcenterno,
    archivepartition,
    beanversion,
    changetype,
    directwageid,
    basedonid,
    updateuserid,
    btpyear1,
    btpyear1_cur,
    btpyear2,
    btpyear3,
    btpyear2_cur,
    btpyear3_cur,
    subtype,
    numofpercapunit1,
    numofpercapunit2,
    branchid,
    numofpercapunit3,
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
FROM {{ ref('stg_raw_pcx_losshistforrating_icare') }}

{% endsnapshot %}
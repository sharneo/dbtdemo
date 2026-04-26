{% snapshot pc_policycontactrole_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_policycontactrole.
                                                Source: ref('stg_raw_pc_policycontactrole')
                                                unique_key: policycontactrole_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='policycontactrole_sk',
    strategy='check',
    alias='pc_policycontactrole',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pc_policycontactrole']
) }}

SELECT
    policycontactrole_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    licensenumberinternal,
    excludedinternal,
    companynamekanjiinternaldenorm,
    fixedid,
    state,
    ownershippct,
    commercialpropertyline,
    updatetime,
    dateofbirthinternal,
    generalliabilityline,
    lastnameinternaldenorm,
    id,
    createuserid,
    maritalstatusinternal,
    policyline,
    beanversion,
    licensestateinternal,
    updateuserid,
    firstnameinternaldenorm,
    quickquotenumber,
    workerscompline,
    relationship,
    included,
    branchid,
    classcodeid,
    companynameinternaldenorm,
    donotordermvr,
    particleinternal,
    publicid,
    companynameinternal,
    applicablegooddriverdiscount,
    inlandmarineline,
    accountcontactrole,
    createtime,
    companynamekanjiinternal,
    numberofviolations,
    lastnamekanjiinternaldenorm,
    lastnamekanjiinternal,
    businessownersline,
    effectivedate,
    expirationdate,
    contactdenorm,
    relationshiptitleinternal,
    lastnameinternal,
    businessautoline,
    personalautoline,
    numberofaccidents,
    seqnumber,
    archivepartition,
    changetype,
    basedonid,
    firstnamekanjiinternaldenorm,
    firstnameinternal,
    firstnamekanjiinternal,
    subtype,
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
FROM {{ ref('stg_raw_pc_policycontactrole') }}

{% endsnapshot %}
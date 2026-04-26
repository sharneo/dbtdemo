{% snapshot pc_activitypattern_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_activitypattern.
                                                Source: ref('stg_raw_pc_activitypattern')
                                                unique_key: activitypattern_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='activitypattern_sk',
    strategy='check',
    alias='pc_activitypattern',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_activitypattern']
) }}

SELECT
    activitypattern_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    escbuscallocpath,
    publicid,
    createtime,
    activityclass,
    command,
    targetincludedays,
    documenttemplate,
    updatetime,
    emailtemplate,
    escalationbuscaltag,
    mandatory,
    escalationhours,
    id,
    targetbuscaltag,
    automatedonly,
    recurring,
    targethours,
    createuserid,
    priority,
    targetbuscallocpath,
    beanversion,
    targetdays,
    retired,
    subject,
    escalationdays,
    code,
    updateuserid,
    shortsubject,
    escalationstartpt,
    patternlevel,
    type,
    escalationincldays,
    description,
    category,
    targetstartpoint,
    defaultassignmenttype,
    defaultassignedgroup,
    defaultassignedqueue,
    refertornsw_ext,
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
FROM {{ ref('stg_raw_pc_activitypattern') }}

{% endsnapshot %}
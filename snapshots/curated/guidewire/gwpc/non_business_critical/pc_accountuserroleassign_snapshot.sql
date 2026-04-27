{% snapshot pc_accountuserroleassign_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_accountuserroleassign.
                                                Source: ref('stg_raw_pc_accountuserroleassign')
                                                unique_key: accountuserroleassign_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='accountuserroleassign_sk',
    strategy='check',
    alias='pc_accountuserroleassign',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pc_accountuserroleassign']
) }}

SELECT
    accountuserroleassign_sk,
    hash_key,
    createuserid,
    previousgroupid,
    publicid,
    active,
    closedate,
    beanversion,
    accountid,
    createtime,
    retired,
    assignedbyuserid,
    assignedgroupid,
    updateuserid,
    comments,
    assigneduserid,
    previousqueueid,
    updatetime,
    role,
    id,
    assignmentdate,
    previoususerid,
    assignedqueueid,
    assignmentstatus,
    archivepartition,
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
FROM {{ ref('stg_raw_pc_accountuserroleassign') }}

{% endsnapshot %}
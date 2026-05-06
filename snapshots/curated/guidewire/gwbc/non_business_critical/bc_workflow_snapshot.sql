{% snapshot bc_workflow_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_workflow.
                                                Source: ref('stg_raw_bc_workflow')
                                                unique_key: workflow_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='workflow_sk',
    strategy='check',
    alias='bc_workflow',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_workflow']
) }}

SELECT
    workflow_sk,
    hash_key,
    previousgroupid,
    enteredstep,
    previousstep,
    publicid,
    currentstep,
    topleveldelinquencyprocessid,
    messagehistoryid,
    processversion,
    createtime,
    handler,
    assignedbyuserid,
    activestate,
    assignedgroupid,
    logentrycounter,
    state,
    previousqueueid,
    updatetime,
    id,
    currentbranch,
    previoususerid,
    assignedqueueid,
    testtime,
    messageid,
    createuserid,
    closedate,
    archivepartition,
    beanversion,
    agencycycleprocessid,
    retired,
    stepexectime,
    timeouttime,
    currentaction,
    updateuserid,
    assigneduserid,
    triggerinvoked,
    delinquencyprocessid,
    forcetimeoutbranch,
    subtype,
    assignmentdate,
    assignmentstatus,
    camtodsid,
    directdebitid,
    refundid,
    account,
    'GWBC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_bc_workflow') }}

{% endsnapshot %}
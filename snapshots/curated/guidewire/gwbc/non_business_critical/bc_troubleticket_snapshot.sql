{% snapshot bc_troubleticket_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_troubleticket.
                                                Source: ref('stg_raw_bc_troubleticket')
                                                unique_key: troubleticket_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='troubleticket_sk',
    strategy='check',
    alias='bc_troubleticket',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_troubleticket']
) }}

SELECT
    troubleticket_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    escalationdate,
    previousgroupid,
    publicid,
    tickettype,
    troubleticketnumberdenorm,
    createtime,
    assignedbyuserid,
    troubleticketnumber,
    detaileddescription,
    assignedgroupid,
    titledenorm,
    previousqueueid,
    updatetime,
    title,
    id,
    previoususerid,
    closeuserid,
    assignedqueueid,
    createuserid,
    priority,
    closedate,
    beanversion,
    retired,
    updateuserid,
    escalated,
    assigneduserid,
    assignmentdate,
    targetdate,
    assignmentstatus,
    reviewdelinquencyprocess_ext,
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
FROM {{ ref('stg_raw_bc_troubleticket') }}

{% endsnapshot %}
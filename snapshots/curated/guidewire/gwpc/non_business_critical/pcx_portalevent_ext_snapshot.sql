{% snapshot pcx_portalevent_ext_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pcx_portalevent_ext.
                                                Source: ref('stg_raw_pcx_portalevent_ext')
                                                unique_key: portalevent_ext_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='portalevent_ext_sk',
    strategy='check',
    alias='pcx_portalevent_ext',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pcx_portalevent_ext']
) }}

SELECT
    portalevent_ext_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    createuserid,
    publicid,
    ackid,
    projectname,
    jobnumber,
    operation,
    beanversion,
    createtime,
    retired,
    updateuserid,
    status,
    agencycrmid,
    apperrorcode,
    errorcode,
    updatetime,
    consumer,
    subtype,
    id,
    policynumber,
    errormessage,
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
FROM {{ ref('stg_raw_pcx_portalevent_ext') }}

{% endsnapshot %}
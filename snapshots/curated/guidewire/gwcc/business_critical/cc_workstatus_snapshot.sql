{% snapshot cc_workstatus_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_workstatus.
                                                Source: ref('stg_raw_cc_workstatus')
                                                unique_key: workstatus_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='workstatus_sk',
    strategy='check',
    alias='cc_workstatus',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'business_critical', 'cc_workstatus']
) }}

SELECT
    workstatus_sk,
    hash_key,
    statusdate,
    loadcommandid,
    createuserid,
    publicid,
    wageloss_icare,
    beanversion,
    archivepartition,
    createtime,
    retired,
    numdaysworked,
    updateuserid,
    employmentdataid,
    status,
    comments,
    lastworkeddate,
    paidfullforlastworked,
    updatetime,
    statusenddate,
    wageamountpostinjury,
    numhoursworked,
    id,
    refid,
    legacycreatetime_ext,
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
FROM {{ ref('stg_raw_cc_workstatus') }}

{% endsnapshot %}
{% snapshot pcx_agencycostcentre_ext_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pcx_agencycostcentre_ext.
                                                Source: ref('stg_raw_pcx_agencycostcentre_ext')
                                                unique_key: agencycostcentre_ext_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='agencycostcentre_ext_sk',
    strategy='check',
    alias='pcx_agencycostcentre_ext',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pcx_agencycostcentre_ext']
) }}

SELECT
    agencycostcentre_ext_sk,
    hash_key,
    level9name,
    loadcommandid,
    publicid,
    accountid,
    createtime,
    costcentrecode,
    updatetime,
    id,
    level2code,
    level3code,
    level4code,
    level5code,
    level6code,
    level7code,
    level8code,
    level9code,
    createuserid,
    beanversion,
    archivepartition,
    updateuserid,
    costcentrename,
    level2name,
    level3name,
    level4name,
    level5name,
    level6name,
    level7name,
    level8name,
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
FROM {{ ref('stg_raw_pcx_agencycostcentre_ext') }}

{% endsnapshot %}
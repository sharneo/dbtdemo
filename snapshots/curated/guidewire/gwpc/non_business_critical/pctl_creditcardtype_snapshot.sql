{% snapshot pctl_creditcardtype_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pctl_creditcardtype.
                                                Source: ref('stg_raw_pctl_creditcardtype')
                                                unique_key: creditcardtype_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='creditcardtype_sk',
    strategy='check',
    alias='pctl_creditcardtype',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'non_business_critical', 'pctl_creditcardtype']
) }}

SELECT
    creditcardtype_sk,
    hash_key,
    s_en_us_edg,
    l_en_us,
    priority,
    typecode,
    s_en_us,
    retired,
    l_en_us_edg,
    name,
    id,
    description,
    l_en_au,
    s_en_au,
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
FROM {{ ref('stg_raw_pctl_creditcardtype') }}

{% endsnapshot %}
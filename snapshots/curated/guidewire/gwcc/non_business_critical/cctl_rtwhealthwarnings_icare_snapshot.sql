{% snapshot cctl_rtwhealthwarnings_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cctl_rtwhealthwarnings_icare.
                                                Source: ref('stg_raw_cctl_rtwhealthwarnings_icare')
                                                unique_key: rtwhealthwarnings_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='rtwhealthwarnings_icare_sk',
    strategy='check',
    alias='cctl_rtwhealthwarnings_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'cctl_rtwhealthwarnings_icare']
) }}

SELECT
    rtwhealthwarnings_icare_sk,
    hash_key,
    l_en_us,
    priority,
    s_en_us_edg_ph,
    typecode,
    s_en_us,
    retired,
    l_en_us_edg,
    name,
    l_en_us_edg_ph,
    s_en_us_edg,
    l_en_au,
    id,
    description,
    s_en_au,
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
FROM {{ ref('stg_raw_cctl_rtwhealthwarnings_icare') }}

{% endsnapshot %}
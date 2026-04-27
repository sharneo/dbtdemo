{% snapshot cc_servicereqstatementline_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for cc_servicereqstatementline.
                                                Source: ref('stg_raw_cc_servicereqstatementline')
                                                unique_key: servicereqstatementline_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='servicereqstatementline_sk',
    strategy='check',
    alias='cc_servicereqstatementline',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'cc_servicereqstatementline']
) }}

SELECT
    servicereqstatementline_sk,
    hash_key,
    practitionername_icare,
    createuserid,
    publicid,
    dateto_icare,
    beanversion,
    archivepartition,
    appointmentdate_icare,
    createtime,
    failedtoattend_icare,
    servicecategory_icare,
    updateuserid,
    datefrom_icare,
    servicerequeststatement,
    updatetime,
    description_icare,
    amount,
    sessions_icare,
    id,
    description,
    category,
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
FROM {{ ref('stg_raw_cc_servicereqstatementline') }}

{% endsnapshot %}
{% snapshot pc_producercode_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_producercode.
                                                Source: ref('stg_raw_pc_producercode')
                                                unique_key: producercode_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='producercode_sk',
    strategy='check',
    alias='pc_producercode',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pc_producercode']
) }}

SELECT
    producercode_sk,
    hash_key,
    createuserid,
    publicid,
    producerstatus,
    preferredunderwriterid,
    beanversion,
    createtime,
    retired,
    codedenorm,
    code,
    appointmentdate,
    organizationid,
    updateuserid,
    addressid,
    terminationdate,
    updatetime,
    descriptiondenorm,
    id,
    description,
    branchid,
    emailaddress_icare,
    comunicatnprfrnce_icare,
    totalbtp_icare,
    policycount_icare,
    addresspublicid,
    stopcorrespondence_ext,
    stopbccorrespondence_ext,
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
FROM {{ ref('stg_raw_pc_producercode') }}

{% endsnapshot %}
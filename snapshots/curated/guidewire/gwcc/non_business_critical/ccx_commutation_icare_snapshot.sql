{% snapshot ccx_commutation_icare_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for ccx_commutation_icare.
                                                Source: ref('stg_raw_ccx_commutation_icare')
                                                unique_key: commutation_icare_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwcc',
    unique_key='commutation_icare_sk',
    strategy='check',
    alias='ccx_commutation_icare',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'claim_centre', 'non_business_critical', 'ccx_commutation_icare']
) }}

SELECT
    commutation_icare_sk,
    hash_key,
    COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at,
    loadcommandid,
    commutationdecision_icare,
    calculation_icare,
    publicid,
    decisiondate_icare,
    eligibility_icare,
    createtime,
    maximumofferamount_icare,
    claim_icareid,
    initialoffer_icare,
    updatetime,
    siracertificationnumber_icare,
    id,
    wccmatternumber_icare,
    actiondate_icare,
    createuserid,
    archivepartition,
    beanversion,
    retired,
    updateuserid,
    rationale_icare,
    attorney_icareid,
    wccregistrationdate_icare,
    siracertificationdate_icare,
    commutationtype_icare,
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
FROM {{ ref('stg_raw_ccx_commutation_icare') }}

{% endsnapshot %}
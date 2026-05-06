{% snapshot pc_policylocation_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_policylocation.
                                                Source: ref('stg_raw_pc_policylocation')
                                                unique_key: policylocation_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='policylocation_sk',
    strategy='check',
    alias='pc_policylocation',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pc_policylocation']
) }}

SELECT
    policylocation_sk,
    hash_key,
    publicid,
    citykanjiinternaldenorm,
    addressline1internal,
    countyinternal,
    addressline2internal,
    addressline3internal,
    createtime,
    citykanjiinternal,
    isratedlocation_icare,
    addressline2kanjiinternal,
    stateinternal,
    fixedid,
    countryinternal,
    effectivedate,
    updatetime,
    id,
    expirationdate,
    employeecountinternal,
    isvalidated_icare,
    validuntilinternal,
    taxlocation,
    locationcontactid_icare,
    createuserid,
    accountlocation,
    cityinternaldenorm,
    industrycodeid,
    archivepartition,
    beanversion,
    cityinternal,
    changetype,
    addresstypeinternal,
    addressline1kanjiinternal,
    cedexbureauinternal,
    basedonid,
    updateuserid,
    locationnum,
    postalcodeinternaldenorm,
    cedexinternal,
    buildingautonumberseq,
    postalcodeinternal,
    descriptioninternal,
    branchid,
    fireprotectclass,
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
FROM {{ ref('stg_raw_pc_policylocation') }}

{% endsnapshot %}
{% snapshot pc_address_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for pc_address.
                                                Source: ref('stg_raw_pc_address')
                                                unique_key: address_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwpc',
    unique_key='address_sk',
    strategy='check',
    alias='pc_address',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'policy_centre', 'business_critical', 'pc_address']
) }}

SELECT
    address_sk,
    hash_key,
    locationname,
    loadcommandid,
    publicid,
    batchgeocode,
    active,
    createtime,
    addressline1,
    addressline2,
    county,
    addressline3,
    citykanji,
    spatialpoint,
    addressline2kanji,
    phoneextension,
    state,
    addressbookuid,
    updatetime,
    country,
    id,
    isvalidated_icare,
    employeecount,
    locationcode,
    createuserid,
    validuntil,
    phonecountry,
    beanversion,
    citydenorm,
    retired,
    city,
    lastupdatetime,
    account,
    phone,
    addresstype,
    addressline1kanji,
    updateuserid,
    cedexbureau,
    geocodestatus,
    locationnum,
    cedex,
    postalcodedenorm,
    postalcode,
    referenced,
    subtype,
    linkedaddress,
    description,
    temporarylastupdatetime,
    nonspecific,
    obfuscatedinternal,
    archivepartition,
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
FROM {{ ref('stg_raw_pc_address') }}

{% endsnapshot %}
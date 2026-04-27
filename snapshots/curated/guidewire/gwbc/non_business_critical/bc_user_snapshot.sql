{% snapshot bc_user_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026-01-01      0.0                             SCD Type 2 snapshot for bc_user.
                                                Source: ref('stg_raw_bc_user')
                                                unique_key: user_sk (surrogate on PK 'id')
                                                check_cols: ['hash_key'] (surrogate on all business cols excl PK)
                                                hard_deletes: new_record (inserts dbt_is_deleted=True row)
                                                No code change required when PARQUET CDC goes live.
-#}

{{ config(
    target_schema='gwbc',
    unique_key='user_sk',
    strategy='check',
    alias='bc_user',
    check_cols=['hash_key'],
    hard_deletes='new_record',
    tags=['snapshot', 'billing_centre', 'non_business_critical', 'bc_user']
) }}

SELECT
    user_sk,
    hash_key,
    loadcommandid,
    publicid,
    usersettingsid,
    createtime,
    sessiontimeoutsecs,
    organizationid,
    vacationstatus,
    department,
    externaluser,
    updatetime,
    language,
    experiencelevel,
    locale,
    id,
    authorityprofileid,
    createuserid,
    beanversion,
    defaultphonecountry,
    retired,
    validationlevel,
    updateuserid,
    credentialid,
    systemusertype,
    defaultcountry,
    timezone,
    contactid,
    jobtitle,
    oktaid_icare,
    obfuscatedinternal,
    'GWBC' AS source_system,
    gwcbi_connector_ts_ms,
    gwcbi_lsn,
    gwcbi_operation,
    gwcbi_payload_ts_ms,
    gwcbi_seqval,
    gwcbi_seqval_hex,
    gwcbi_tx_id,
    metadata_file_name,
    file_ingestion_timestamp
FROM {{ ref('stg_raw_bc_user') }}

{% endsnapshot %}
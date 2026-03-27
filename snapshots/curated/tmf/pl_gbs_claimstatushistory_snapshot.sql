{% snapshot pl_gbs_claimstatushistory_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table pl_gbs_claimstatushistory_snapshot . RFTM 
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='pl_gbs_claimstatushistory_sk',
    strategy='check',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'claim_status_code', 'claim_status_code_dsc', 'date_changed', 'reopen_reason_code'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','pl_gbs_claimstatushistory']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'claims_manager',
            'agency',
            'line_of_business',
            'extract_date',
            'record_number',
            'policy_number',
            'claim_number',
            'claim_status_code',
            'claim_status_code_dsc',
            'date_changed',
            'reopen_reason_code'
    ]) }} AS pl_gbs_claimstatushistory_sk,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        claims_manager,
        agency,
        line_of_business,
        extract_date,
        record_number,
        policy_number,
        claim_number,
        claim_status_code,
        claim_status_code_dsc,
        date_changed,
        reopen_reason_code,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'pl_gbs_claimstatushistory') }}
)

SELECT * FROM source_data

{% endsnapshot %}
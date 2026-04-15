{% snapshot wc_aal_rehabilitation_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table wc_aal_rehabilitation_snapshot . 
                                                dbt snapshot is SCD Type 2 .
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_aal_rehabilitation_sk',
    strategy='check',
    alias='wc_aal_rehabilitation',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'rehab_referral_seq_number', 'date_referred_to_provider', 'rehab_provider_code', 'rehab_provider_code_dsc', 'date_rehab_completed', 'date_rehab_started', 'rehab_coordinator', 'rehab_outcome', 'rehab_status', 'rehab_case', 'service_provision_type', 'service_provision_sub_type', 'service_provision_null_date', 'work_trial_host_employer_abn'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_aal_rehabilitation']
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
            'rehab_referral_seq_number',
            'date_referred_to_provider',
            'rehab_provider_code',
            'rehab_provider_code_dsc',
            'date_rehab_completed',
            'date_rehab_started',
            'rehab_coordinator',
            'rehab_outcome',
            'rehab_status',
            'rehab_case',
            'service_provision_type',
            'service_provision_sub_type',
            'service_provision_null_date',
            'work_trial_host_employer_abn'
    ]) }} AS wc_aal_rehabilitation_sk,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        claims_manager,
        agency,
        line_of_business,
        extract_date,
        record_number,
        policy_number,
        claim_number,
        rehab_referral_seq_number,
        date_referred_to_provider,
        rehab_provider_code,
        rehab_provider_code_dsc,
        date_rehab_completed,
        date_rehab_started,
        rehab_coordinator,
        rehab_outcome,
        rehab_status,
        rehab_case,
        service_provision_type,
        service_provision_sub_type,
        service_provision_null_date,
        work_trial_host_employer_abn,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'wc_aal_rehabilitation') }}
)

SELECT * FROM source_data

{% endsnapshot %}
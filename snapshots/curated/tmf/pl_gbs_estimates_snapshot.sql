{% snapshot pl_gbs_estimates_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table pl_gbs_estimates_snapshot . RFTM 
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='pl_gbs_estimates_sk',
    strategy='check',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'estimate_date', 'payment_type', 'payment_transaction_code', 'estimate_amount', 'estimate_gst', 'estimate_future_weeks_off'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','pl_gbs_estimates']
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
            'estimate_date',
            'payment_type',
            'payment_transaction_code',
            'estimate_amount',
            'estimate_gst',
            'estimate_future_weeks_off'
    ]) }} AS pl_gbs_estimates_sk,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        claims_manager,
        agency,
        line_of_business,
        extract_date,
        record_number,
        policy_number,
        claim_number,
        estimate_date,
        payment_type,
        payment_transaction_code,
        estimate_amount,
        estimate_gst,
        estimate_future_weeks_off,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'pl_gbs_estimates') }}
)

SELECT * FROM source_data

{% endsnapshot %}
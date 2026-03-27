{% snapshot wc_tmfgwcc_estimates_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table wc_tmfgwcc_estimates_snapshot . 
                                                dbt snapshot is SCD Type 2 .
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_tmfgwcc_estimates_sk',
    strategy='check',
    alias='wc_tmfgwcc_estimates',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'estimate_date', 'payment_type', 'payment_transaction_code', 'estimate_amount', 'estimate_gst', 'estimate_future_weeks_off', 'original_estimate'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_tmfgwcc_estimates']
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
            'estimate_future_weeks_off',
            'original_estimate'
    ]) }} AS wc_tmfgwcc_estimates_sk,
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
        original_estimate,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'wc_tmfgwcc_estimates') }}
)

SELECT * FROM source_data

{% endsnapshot %}
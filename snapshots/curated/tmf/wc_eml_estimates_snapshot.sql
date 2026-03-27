{% snapshot wc_eml_estimates_snapshot %}

{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_eml_estimates_sk',
    strategy='check',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'estimate_date', 'payment_type', 'payment_transaction_code', 'estimate_amount', 'estimate_gst', 'estimate_future_weeks_off', 'original_estimate'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_eml_estimates']
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
    ]) }} AS wc_eml_estimates_sk,
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
    FROM {{ source('ifnsw_claim_interface', 'wc_eml_estimates') }}
)

SELECT * FROM source_data

{% endsnapshot %}
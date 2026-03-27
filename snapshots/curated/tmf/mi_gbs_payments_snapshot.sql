{% snapshot mi_gbs_payments_snapshot %}

{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='mi_gbs_payments_sk',
    strategy='check',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'payment_sequence_number', 'amount_paid', 'amount_paid_sign', 'amount_gst', 'amount_gst_sign', 'payment_transaction_date', 'payment_type', 'payment_transaction_code', 'payment_transaction_code_dsc', 'cheque_number', 'payee', 'payee_number', 'payee_type', 'amount_excess', 'date_payment_approved', 'itc', 'gl_month', 'dam', 'date_cheque_issued'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','mi_gbs_payments']
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
            'payment_sequence_number',
            'amount_paid',
            'amount_paid_sign',
            'amount_gst',
            'amount_gst_sign',
            'payment_transaction_date',
            'payment_type',
            'payment_transaction_code',
            'payment_transaction_code_dsc',
            'cheque_number',
            'payee',
            'payee_number',
            'payee_type',
            'amount_excess',
            'date_payment_approved',
            'itc',
            'gl_month',
            'dam',
            'date_cheque_issued'
    ]) }} AS mi_gbs_payments_sk,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        claims_manager,
        agency,
        line_of_business,
        extract_date,
        record_number,
        policy_number,
        claim_number,
        payment_sequence_number,
        amount_paid,
        amount_paid_sign,
        amount_gst,
        amount_gst_sign,
        payment_transaction_date,
        payment_type,
        payment_transaction_code,
        payment_transaction_code_dsc,
        cheque_number,
        payee,
        payee_number,
        payee_type,
        amount_excess,
        date_payment_approved,
        itc,
        gl_month,
        dam,
        date_cheque_issued,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'mi_gbs_payments') }}
)

SELECT * FROM source_data

{% endsnapshot %}
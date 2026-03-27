{% snapshot wc_qbe_payments_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table wc_qbe_payments_snapshot . 
                                                dbt snapshot is SCD Type 2 .
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_qbe_payments_sk',
    strategy='check',
    alias='wc_qbe_payments',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'payment_sequence_number', 'amount_paid', 'amount_paid_sign', 'amount_gst', 'amount_gst_sign', 'payment_transaction_date', 'payment_type', 'payment_transaction_code', 'payment_transaction_code_dsc', 'cheque_number', 'payee', 'payee_number', 'payee_type', 'amount_excess', 'date_payment_approved', 'itc', 'adjustment_transaction_flag', 'date_cheque_issued', 'payment_end_date', 'payment_start_date', 'weeks_paid_other_incapacity', 'hours_paid_total_incapacity', 'reimbursement_schedule_code', 'cont_wkly_bnft_exception_code', 'cont_wkly_bnft_exception_date', 'service_provider_id', 'payment_classification_number', 'gl_month', 'dam', 'date_of_service', 'payee_id', 'hours_paid_partial_incapacity', 'payment_unique_id', 'wage_reimb_request_recd_date', 'determined_weekly_benefit_amt', 'hours_lost', 'earnings', 'deductibles', 'legal_provider_id'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_qbe_payments']
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
            'adjustment_transaction_flag',
            'date_cheque_issued',
            'payment_end_date',
            'payment_start_date',
            'weeks_paid_other_incapacity',
            'hours_paid_total_incapacity',
            'reimbursement_schedule_code',
            'cont_wkly_bnft_exception_code',
            'cont_wkly_bnft_exception_date',
            'service_provider_id',
            'payment_classification_number',
            'gl_month',
            'dam',
            'date_of_service',
            'payee_id',
            'hours_paid_partial_incapacity',
            'payment_unique_id',
            'wage_reimb_request_recd_date',
            'determined_weekly_benefit_amt',
            'hours_lost',
            'earnings',
            'deductibles',
            'legal_provider_id'
    ]) }} AS wc_qbe_payments_sk,
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
        adjustment_transaction_flag,
        date_cheque_issued,
        payment_end_date,
        payment_start_date,
        weeks_paid_other_incapacity,
        hours_paid_total_incapacity,
        reimbursement_schedule_code,
        cont_wkly_bnft_exception_code,
        cont_wkly_bnft_exception_date,
        service_provider_id,
        payment_classification_number,
        gl_month,
        dam,
        date_of_service,
        payee_id,
        hours_paid_partial_incapacity,
        payment_unique_id,
        wage_reimb_request_recd_date,
        determined_weekly_benefit_amt,
        hours_lost,
        earnings,
        deductibles,
        legal_provider_id,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'wc_qbe_payments') }}
)

SELECT * FROM source_data

{% endsnapshot %}
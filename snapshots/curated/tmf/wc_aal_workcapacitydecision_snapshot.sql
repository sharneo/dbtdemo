{% snapshot wc_aal_workcapacitydecision_snapshot %}

{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_aal_workcapacitydecision_sk',
    strategy='check',
    check_cols=['claims_manager', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'original_decision_date', 'work_capacity_decn_type', 'work_capacity_revw_stage', 'work_capacity_date_type', 'work_capacity_trans_date', 'work_capacity_outcome'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_aal_workcapacitydecision']
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'claims_manager',
            'line_of_business',
            'extract_date',
            'record_number',
            'policy_number',
            'claim_number',
            'original_decision_date',
            'work_capacity_decn_type',
            'work_capacity_revw_stage',
            'work_capacity_date_type',
            'work_capacity_trans_date',
            'work_capacity_outcome'
    ]) }} AS wc_aal_workcapacitydecision_sk,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        claims_manager,
        line_of_business,
        extract_date,
        record_number,
        policy_number,
        claim_number,
        original_decision_date,
        work_capacity_decn_type,
        work_capacity_revw_stage,
        work_capacity_date_type,
        work_capacity_trans_date,
        work_capacity_outcome,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'wc_aal_workcapacitydecision') }}
)

SELECT * FROM source_data

{% endsnapshot %}
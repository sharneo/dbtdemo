{% snapshot wc_eml_dependents_snapshot %}

{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_eml_dependents_sk',
    strategy='check',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'dependents_children', 'dependents_other', 'dependent_name', 'relationship', 'date_of_birth', 'working_spouse', 'student', 'residing_at_home', 'child', 'dependent_id', 'dependent_status'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_eml_dependents']
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
            'dependents_children',
            'dependents_other',
            'dependent_name',
            'relationship',
            'date_of_birth',
            'working_spouse',
            'student',
            'residing_at_home',
            'child',
            'dependent_id',
            'dependent_status'
    ]) }} AS wc_eml_dependents_sk,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        claims_manager,
        agency,
        line_of_business,
        extract_date,
        record_number,
        policy_number,
        claim_number,
        dependents_children,
        dependents_other,
        dependent_name,
        relationship,
        date_of_birth,
        working_spouse,
        student,
        residing_at_home,
        child,
        dependent_id,
        dependent_status,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'wc_eml_dependents') }}
)

SELECT * FROM source_data

{% endsnapshot %}
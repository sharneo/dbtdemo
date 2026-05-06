{% snapshot wc_aal_dependents_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table wc_aal_dependents_snapshot . 
                                                dbt snapshot is SCD Type 2 .
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_aal_dependents_sk',
    strategy='check',
    alias='wc_aal_dependents',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'dependents_children', 'dependents_other', 'dependent_name', 'relationship', 'date_of_birth', 'working_spouse', 'student', 'residing_at_home', 'child', 'dependent_id', 'dependent_status'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_aal_dependents']
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
    ]) }} AS wc_aal_dependents_sk,
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
    FROM {{ source('ifnsw_claim_interface', 'wc_aal_dependents') }}
)

SELECT * FROM source_data

{% endsnapshot %}
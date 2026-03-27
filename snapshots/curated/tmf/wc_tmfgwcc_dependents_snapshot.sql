{% snapshot wc_tmfgwcc_dependents_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table wc_tmfgwcc_dependents_snapshot . RFTM 
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_tmfgwcc_dependents_sk',
    strategy='check',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'dependents_children', 'dependents_other', 'dependent_name', 'relationship', 'date_of_birth', 'working_spouse', 'student', 'residing_at_home', 'child', 'dependent_id', 'dependent_status'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_tmfgwcc_dependents']
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
    ]) }} AS wc_tmfgwcc_dependents_sk,
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
    FROM {{ source('ifnsw_claim_interface', 'wc_tmfgwcc_dependents') }}
)

SELECT * FROM source_data

{% endsnapshot %}
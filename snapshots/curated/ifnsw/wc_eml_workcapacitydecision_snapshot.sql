{% snapshot wc_eml_workcapacitydecision_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table wc_eml_workcapacitydecision_snapshot . 
                                                dbt snapshot is SCD Type 2 .
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='wc_eml_workcapacitydecision_sk',
    strategy='check',
    alias='wc_eml_workcapacitydecision',
    check_cols=['claims_manager', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'original_decision_date', 'work_capacity_decn_type', 'work_capacity_revw_stage', 'work_capacity_date_type', 'work_capacity_trans_date', 'work_capacity_outcome'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','wc_eml_workcapacitydecision']
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
    ]) }} AS wc_eml_workcapacitydecision_sk,
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
    FROM {{ source('ifnsw_claim_interface', 'wc_eml_workcapacitydecision') }}
)

SELECT * FROM source_data

{% endsnapshot %}
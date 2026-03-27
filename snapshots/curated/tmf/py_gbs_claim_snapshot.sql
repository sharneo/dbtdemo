{% snapshot py_gbs_claim_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a  View for the Snapshot for the Table py_gbs_claim_snapshot . RFTM 
                                                
-#}


{{ config(
    target_schema='ifnsw_claim_interface',
    unique_key='py_gbs_claim_sk',
    strategy='check',
    check_cols=['claims_manager', 'agency', 'line_of_business', 'extract_date', 'record_number', 'policy_number', 'claim_number', 'accident_cause', 'assessor', 'claim_lodged', 'claim_status', 'claim_type', 'clmnt_surname', 'clmnt_first_name', 'claims_officer', 'cost_centre', 'date_reported', 'date_entered', 'date_finalised', 'date_occurred', 'date_received', 'incident_code', 'incident_code_dsc', 'incident_number', 'litigation_claim_status', 'location_street', 'location_suburb', 'location_postcode', 'property_asset_value', 'recovery_claim_status', 'time_occurred', 'damage_type', 'catastrophe_code', 'catastrophe_dsc', 'result_code', 'country_code', 'event_code', 'previous_policy_number', 'incident_narrative', 'salvage_action', 'area_health_service', 'claim_sensitivity_code', 'claim_sensitivity_desc'],
    tags=['snapshot_tmf','snapshot_curated','snapshot','py_gbs_claim']
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
            'accident_cause',
            'assessor',
            'claim_lodged',
            'claim_status',
            'claim_type',
            'clmnt_surname',
            'clmnt_first_name',
            'claims_officer',
            'cost_centre',
            'date_reported',
            'date_entered',
            'date_finalised',
            'date_occurred',
            'date_received',
            'incident_code',
            'incident_code_dsc',
            'incident_number',
            'litigation_claim_status',
            'location_street',
            'location_suburb',
            'location_postcode',
            'property_asset_value',
            'recovery_claim_status',
            'time_occurred',
            'damage_type',
            'catastrophe_code',
            'catastrophe_dsc',
            'result_code',
            'country_code',
            'event_code',
            'previous_policy_number',
            'incident_narrative',
            'salvage_action',
            'area_health_service',
            'claim_sensitivity_code',
            'claim_sensitivity_desc'
    ]) }} AS py_gbs_claim_sk,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        claims_manager,
        agency,
        line_of_business,
        extract_date,
        record_number,
        policy_number,
        claim_number,
        accident_cause,
        assessor,
        claim_lodged,
        claim_status,
        claim_type,
        clmnt_surname,
        clmnt_first_name,
        claims_officer,
        cost_centre,
        date_reported,
        date_entered,
        date_finalised,
        date_occurred,
        date_received,
        incident_code,
        incident_code_dsc,
        incident_number,
        litigation_claim_status,
        location_street,
        location_suburb,
        location_postcode,
        property_asset_value,
        recovery_claim_status,
        time_occurred,
        damage_type,
        catastrophe_code,
        catastrophe_dsc,
        result_code,
        country_code,
        event_code,
        previous_policy_number,
        incident_narrative,
        salvage_action,
        area_health_service,
        claim_sensitivity_code,
        claim_sensitivity_desc,
        metadata_file_name,
        metadata_file_last_modified,
        metadata_scan_time,
        metadata_row_number,
        file_ingestion_timestamp
    FROM {{ source('ifnsw_claim_interface', 'py_gbs_claim') }}
)

SELECT * FROM source_data

{% endsnapshot %}
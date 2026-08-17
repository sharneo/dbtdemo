{#-
Project: Data Uplift Program
Date            Version         Author          Description of Change           
2026-01-01      0.5                             Curated current-state view for pc_account.
                                                Filters: current SCD2 via {{ snapshot_valid_to_current() }}
                                                         excludes Guidewire Cloud Program delete Flag i.e. CDA Deletion is 1 and snapshot hard deletes
-#}

{{ config(
    materialized='view',
    tags=["current_view"]
) }}

with source as (

    select * from {{ source('gwab', 'insurer_control') }}

),

filtered as (

    select
        source.*,
        'Y' as current_record
    from source
    WHERE current_submission_flag = 'Y'      

)

select * from filtered

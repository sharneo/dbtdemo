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

    select * from {{ source('gwab', 'sira_submission_period') }}

),

filtered as (

    select
        *
    from source
)

select * from filtered

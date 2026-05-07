{#-
Project: Data Uplift Program
Date            Version         Author          Description of Change           
2026-01-01      0.5                             Curated current-state view for abtl_dpplatformtype_ext.
                                                Filters: current SCD2 via {{ snapshot_valid_to_current() }}
                                                         excludes Guidewire Cloud Program delete Flag i.e. CDA Deletion is 1 and snapshot hard deletes
-#}

{{ config(
    materialized='view',
    tags=["current_view"]
) }}

with source as (

    select * from {{ ref('abtl_dpplatformtype_ext_snapshot') }}

),

filtered as (

    select
        source.*,
        'Y' as current_record
    from source
    where dbt_valid_to = {{ snapshot_valid_to_current() }}
      and coalesce(gwcbi_operation, 0) <> 1
      and lower(dbt_is_deleted) = 'false'

)

select * from filtered
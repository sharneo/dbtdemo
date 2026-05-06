{{
  config(
    materialized='incremental'
  )
}}

with

activity_base as (
    select
          claim_nbr
        , src_activity_id
        , activity_public_id
        , activity_subject_desc
        , activity_target_dttm
        , activity_target_dt
        , activity_escalation_dttm
        , activity_assignment_dttm
        , activity_close_dttm
        , activity_close_dt
        , activity_last_view_dttm
        , src_create_dttm
        , src_create_dt
        , priority_desc
        , activity_type_desc
        , activity_status_desc
        , activity_pattern_code
        , close_user_sk
        , assigned_user_sk
        , assigned_team_sk
        , approval_issue
        , src_txn_set_id
        , approval_rationale
        , file_ingestion_timestamp
        , extract_date
    from {{ ref('msc_qlk_aspire_activity') }}
),

final as (
    select * from activity_base
)

select * from final

{% if is_incremental() %}
  where file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}

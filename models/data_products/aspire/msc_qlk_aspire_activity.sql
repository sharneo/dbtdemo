{{
  config(
    materialized='incremental',
    unique_key='src_activity_id',
  )
}}

with

cc_activity as (
    select
          id
        , claimid
        , publicid
        , subject
        , targetdate
        , escalationdate
        , assignmentdate
        , closedate
        , lastvieweddate
        , createtime
        , type
        , status
        , priority
        , closeuserid
        , assigneduserid
        , activitypatternid
        , assignedgroupid
        , approvalissue
        , transactionsetid
        , approvalrationale
        , file_ingestion_timestamp
    from {{ ref('v_cc_activity_current') }}
    where retired = 0
),

cc_claim as (
    select
          id
        , claimnumber
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cctl_activitytype as (
    select
          id
        , name
    from {{ ref('v_cctl_activitytype_current') }}
),

cctl_activitystatus as (
    select
          id
        , name
    from {{ ref('v_cctl_activitystatus_current') }}
),

cctl_priority as (
    select
          id
        , name
    from {{ ref('v_cctl_priority_current') }}
),

cc_user as (
    select
          id
        , publicid
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

cc_activitypattern as (
    select
          id
        , code
    from {{ ref('v_cc_activitypattern_current') }}
),

cc_group as (
    select
          id
        , publicid
    from {{ ref('v_cc_group_current') }}
    where retired = 0
),

final as (
    select
          clm.claimnumber as claim_nbr
        , act.id as src_activity_id
        , act.publicid as activity_public_id
        , act.subject as activity_subject_desc
        , act.targetdate as activity_target_dttm
        , cast(act.targetdate as date) as activity_target_dt
        , act.escalationdate as activity_escalation_dttm
        , act.assignmentdate as activity_assignment_dttm
        , act.closedate as activity_close_dttm
        , cast(act.closedate as date) as activity_close_dt
        , act.lastvieweddate as activity_last_view_dttm
        , act.createtime as src_create_dttm
        , cast(act.createtime as date) as src_create_dt
        , act_pri.name as priority_desc
        , act_type.name as activity_type_desc
        , act_stus.name as activity_status_desc
        , act_patt.code as activity_pattern_code
        , md5(concat('GWCC', cls_usr.publicid)) as close_user_sk
        , md5(concat('GWCC', assg_usr.publicid)) as assigned_user_sk
        , md5(concat('GWCC', assgn_team.publicid)) as assigned_team_sk
        , act.approvalissue as approval_issue
        , act.transactionsetid as src_txn_set_id
        , act.approvalrationale as approval_rationale
        , act.file_ingestion_timestamp
        , current_date() as extract_date

    from cc_activity act

    join cc_claim clm
        on act.claimid = clm.id

    left join cctl_activitytype act_type
        on act.type = act_type.id

    left join cctl_activitystatus act_stus
        on act.status = act_stus.id

    left join cctl_priority act_pri
        on act.priority = act_pri.id

    left join cc_user cls_usr
        on act.closeuserid = cls_usr.id

    left join cc_user assg_usr
        on act.assigneduserid = assg_usr.id

    left join cc_activitypattern act_patt
        on act.activitypatternid = act_patt.id

    left join cc_group assgn_team
        on act.assignedgroupid = assgn_team.id
)

select * from final

{% if is_incremental() %}
  where file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}

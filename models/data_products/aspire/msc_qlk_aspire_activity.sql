{{
  config(
    materialized='incremental',
    unique_key='src_activity_id',
    incremental_strategy='merge'
  )
}}

{#
  Source: 02_ACTIVITY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A02
  TBL_NM: MSC_QLK_ASPIRE_ACTIVITY
#}

with cc_activity as (
    select
        id,
        claimid,
        publicid,
        subject,
        targetdate,
        escalationaldate,
        assignmentdate,
        closedate,
        lastvieweddate,
        createtime,
        priority,
        type,
        status,
        activitypatternid,
        approvalissue,
        transactionsetid,
        approvalrationale,
        assigneduserid,
        assignedgroupid,
        closeuserid,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_activity_current') }}
    where retired = 0
),

cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cctl_activitytype as (
    select
        id,
        name
    from {{ ref('v_cctl_activitytype_current') }}
),

cctl_activitystatus as (
    select
        id,
        name
    from {{ ref('v_cctl_activitystatus_current') }}
),

cctl_priority as (
    select
        id,
        name
    from {{ ref('v_cctl_priority_current') }}
),

cc_user_close as (
    select
        id,
        publicid
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

cc_user_assigned as (
    select
        id,
        publicid
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

cc_activitypattern as (
    select
        id,
        code
    from {{ ref('v_cc_activitypattern_current') }}
),

cc_group as (
    select
        id,
        publicid
    from {{ ref('v_cc_group_current') }}
    where retired = 0
)

select
    clm.claimnumber as claim_nbr,
    act.id as src_activity_id,
    act.publicid as activity_public_id,
    act.subject as activity_subject_desc,
    act.targetdate as activity_target_dttm,
    cast(act.targetdate as date) as activity_target_dt,
    act.escalationaldate as activity_escalation_dttm,
    act.assignmentdate as activity_assignment_dttm,
    act.closedate as activity_close_dttm,
    cast(act.closedate as date) as activity_close_dt,
    act.lastvieweddate as activity_last_view_dttm,
    act.createtime as src_create_dttm,
    cast(act.createtime as date) as src_create_dt,
    actpri.name as priority_desc,
    acttype.name as activity_type_desc,
    actstus.name as activity_status_desc,
    actpatt.code as activity_pattern_code,
    md5(concat('GWCC', clsusr.publicid)) as close_user_sk,
    md5(concat('GWCC', assgusr.publicid)) as assigned_user_sk,
    md5(concat('GWCC', assgnteam.publicid)) as assigned_team_sk,
    act.approvalissue as approval_issue,
    act.transactionsetid as src_txn_set_id,
    act.approvalrationale as approval_rationale,
    current_date() as extract_date,
    act.file_ingestion_timestamp

from cc_activity act

inner join cc_claim clm
    on act.claimid = clm.id

left join cctl_activitytype acttype
    on act.type = acttype.id

left join cctl_activitystatus actstus
    on act.status = actstus.id

left join cctl_priority actpri
    on act.priority = actpri.id

left join cc_user_close clsusr
    on act.closeuserid = clsusr.id

left join cc_user_assigned assgusr
    on act.assigneduserid = assgusr.id

left join cc_activitypattern actpatt
    on act.activitypatternid = actpatt.id

left join cc_group assgnteam
    on act.assignedgroupid = assgnteam.id

{% if is_incremental() %}
where act.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% else %}
where 1=1
{% endif %}

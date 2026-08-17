{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Done from MSC_QLK_ASPIRE_ACTIVITY
                                                Source: 02_ACTIVITY.sas
 
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_activity_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
       tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

with cc_activity as (
    select
        id,
        claimid,
        publicid,
        subject,
        targetdate,
        escalationdate,
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
        file_ingestion_timestamp,
        source_system
    from {{ ref('v_cc_activity_current') }}
    where retired = 0
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
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
),

cte_join as 
(
select
    source_system,
    clm.claimnumber as claim_nbr,
    act.id as src_activity_id,
    act.publicid as activity_public_id,
    act.subject as activity_subject_desc,
    CAST(act.targetdate as TIMESTAMP_NTZ) as  activity_target_dttm,
    cast(act.targetdate as date) as activity_target_dt,
    CAST(act.escalationdate as TIMESTAMP_NTZ) as activity_escalation_dttm,
    CAST(act.assignmentdate as TIMESTAMP_NTZ) as activity_assignment_dttm,
    CAST(act.closedate as TIMESTAMP_NTZ) as activity_close_dttm,
    CAST(act.closedate as date) as activity_close_dt,
    CAST(act.lastvieweddate as TIMESTAMP_NTZ) as activity_last_view_dttm,
    CAST(act.createtime as  TIMESTAMP_NTZ) as src_create_dttm,
    cast(act.createtime as date) as src_create_dt,
    actpri.name as priority_desc,
    acttype.name as activity_type_desc,
    actstus.name as activity_status_desc,
    actpatt.code as activity_pattern_code,
    cast({{ dbt_utils.generate_surrogate_key([
        'act.source_system',
        'clsusr.publicid'
    ]) }} as varchar(150)) as close_user_sk,
    cast({{ dbt_utils.generate_surrogate_key([
        'act.source_system',
        'assgusr.publicid'
    ]) }} as varchar(150)) as assigned_user_sk,
    cast({{ dbt_utils.generate_surrogate_key([
        'act.source_system',
        'assgnteam.publicid'
    ]) }} as varchar(150)) as assigned_team_sk,
    act.approvalissue as approval_issue,
    act.transactionsetid as src_txn_set_id,
    act.approvalrationale as approval_rationale,
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
)
    SELECT 
        claim_nbr,
        src_activity_id,
        activity_public_id,
        activity_subject_desc,
        activity_target_dttm,
        activity_target_dt,
        activity_escalation_dttm,
        activity_assignment_dttm,
        activity_close_dttm,
        activity_close_dt,
        activity_last_view_dttm,
        src_create_dttm,
        src_create_dt,
        priority_desc,
        activity_type_desc,
        activity_status_desc,
        activity_pattern_code,
        close_user_sk,
        assigned_user_sk,
        assigned_team_sk,
        approval_issue,
        src_txn_set_id,
        approval_rationale,
        file_ingestion_timestamp        
    from 
        cte_join
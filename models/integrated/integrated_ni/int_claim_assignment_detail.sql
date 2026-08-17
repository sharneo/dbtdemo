{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for claim assignment detail.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_rule_detail_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 40_CLAIM_ASSIGNMENT_DETAIL.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A40
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_ASSIGNMENT_DETAIL
-#}

with ccx_assignmentdetails_icare as (
    select
        id,
        publicid,
        claimassignmentrules,
        allocationsegment,
        managingentity,
        team,
        assigneduserid,
        allocationrule,
        status,
        startdate,
        enddate,
        createtime,
        updatetime,
        file_ingestion_timestamp,
        source_system
    from {{ ref('v_ccx_assignmentdetails_icare_current') }}
    where retired = 0
    {% if is_incremental() %}
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

ccx_managingentity_icare as (
    select
        id,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
    where retired = 0
),

cctl_allocationstatustype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_allocationstatustype_icare_current') }}
    where retired = 0
),

cctl_allocationsegementtype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_allocationsegementtype_icare_current') }}
    where retired = 0
),

cctl_allocationruletype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_allocationruletype_icare_current') }}
    where retired = 0
),

cc_group as (
    select
        id,
        name
    from {{ ref('v_cc_group_current') }}
    where retired = 0
),

cc_groupuser as (
    select
        id,
        userid
    from {{ ref('v_cc_groupuser_current') }}
),

cc_user as (
    select
        id,
        contactid
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),

cc_contact as (
    select
        id,
        firstname,
        middlename,
        lastname
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

cte_join AS 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'dtl.source_system',
        'dtl.publicid'
    ]) }} as varchar(150)) as rule_detail_sk,
    dtl.publicid as rule_detail_id,
    dtl.id as src_rule_detail_id,
    dtl.claimassignmentrules as src_assignment_rule_id,
    seg.typecode as allocation_segment_cd,
    seg.name as allocation_segment_desc,
    mgent.code as managing_entity_cd,
    grp.name as managing_entity_group,
    trim(concat_ws(' ', ctt.firstname, ctt.middlename, ctt.lastname)) as managing_entity_user,
    allocrul.typecode as allocation_rule_cd,
    allocrul.name as allocation_rule_desc,
    allocsta.typecode as rule_detail_status_cd,
    allocsta.name as rule_detail_status_desc,
    CAST(dtl.startdate as TIMESTAMP_NTZ) as  allocation_start_dttm,
    CAST(dtl.enddate as TIMESTAMP_NTZ) as allocation_end_dttm,
    CAST(dtl.createtime as TIMESTAMP_NTZ) as  src_create_dttm,
    CAST(dtl.updatetime as TIMESTAMP_NTZ) as src_update_dttm,
    dtl.file_ingestion_timestamp
from ccx_assignmentdetails_icare dtl
left join ccx_managingentity_icare mgent
    on mgent.id = dtl.managingentity
left join cctl_allocationstatustype_icare allocsta
    on allocsta.id = dtl.status
left join cctl_allocationsegementtype_icare seg
    on seg.id = dtl.allocationsegment
left join cctl_allocationruletype_icare allocrul
    on allocrul.id = dtl.allocationrule
left join cc_group grp
    on grp.id = dtl.team
left join cc_groupuser grpusr
    on grpusr.id = dtl.assigneduserid
left join cc_user usr
    on usr.id = grpusr.userid
left join cc_contact ctt
    on ctt.id = usr.contactid
)
SELECT 
        rule_detail_sk,
        rule_detail_id,
        src_rule_detail_id,
        src_assignment_rule_id,
        allocation_segment_cd,
        allocation_segment_desc,
        managing_entity_cd,
        managing_entity_group,
        managing_entity_user,
        allocation_rule_cd,
        allocation_rule_desc,
        rule_detail_status_cd,
        rule_detail_status_desc,
        allocation_start_dttm,
        allocation_end_dttm,
        src_create_dttm,
        src_update_dttm,
        file_ingestion_timestamp
FROM    
        cte_join 
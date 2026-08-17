{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for subrogation.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_subrogation_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 43_SUBROGATION.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A43
  TBL_NM: MSC_QLK_ASPIRE_SUBROGATION
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_subrogationsummary as (
    select
        id,
        publicid,
        claimid,
        subroreferralcomment,
        subroreferraldate,
        createtime,
        updatetime
    from {{ ref('v_cc_subrogationsummary_current') }}
    where retired = 0
),

cc_subrogation as (
    select
        id,
        subrogationsummaryid,
        assigneduserid,
        assignedgroupid,
        status,
        outcome,
        assignmentstatus,
        assignmentdate
    from {{ ref('v_cc_subrogation_current') }}
    where retired = 0
),

cc_user as (
    select
        id,
        contactid
    from {{ ref('v_cc_user_current') }}
),

cc_contact as (
    select
        id,
        COALESCE(firstname,'') as firstname,
        COALESCE(middlename,'') as middlename, 
        COALESCE(lastname,'') as lastname 
    from {{ ref('v_cc_contact_current') }}
    where retired = 0
),

cctl_subrogationstatus as (
    select
        id,
        typecode,
        l_en_au
    from {{ ref('v_cctl_subrogationstatus_current') }}
    where retired = 0
),

cctl_subroclosedoutcome as (
    select
        id,
        typecode,
        description
    from {{ ref('v_cctl_subroclosedoutcome_current') }}
    where retired = 0
),

cctl_assignmentstatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_assignmentstatus_current') }}
    where retired = 0
),

cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'summ.publicid'
    ]) }} as varchar(150)) as subrogation_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    summ.id as src_subrogation_id,
    sbr.assigneduserid as subrogation_owner_id,
    concat(ownr.firstname, ' ', ownr.middlename, ' ', ownr.lastname) as subrogation_owner_name,
    sbr.assignedgroupid as subrogation_group_id,
    sts.typecode as subrogation_status_cd,
    sts.l_en_au as subrogation_status_desc,
    otc.typecode as subrogation_outcome_cd,
    otc.description as subrogation_outcome_desc,
    asssts.typecode as subrogation_assignment_status_cd,
    asssts.name as subrogation_assignment_status_desc,
    summ.subroreferralcomment as subrogation_referral_comment,
    CAST(summ.subroreferraldate as TIMESTAMP_NTZ) as subrogation_referral_dttm,
    CAST(sbr.assignmentdate as TIMESTAMP_NTZ) as subrogation_assignment_dttm,
    CAST(summ.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    CAST(summ.updatetime as TIMESTAMP_NTZ) as src_update_dttm,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_subrogationsummary summ

inner join cc_claim clm
    on clm.id = summ.claimid

inner join cc_subrogation sbr
    on sbr.subrogationsummaryid = summ.id

left join cctl_subrogationstatus sts
    on sts.id = sbr.status

left join cctl_subroclosedoutcome otc
    on otc.id = sbr.outcome

left join cctl_assignmentstatus asssts
    on asssts.id = sbr.assignmentstatus

left join cc_user usr
    on usr.id = sbr.assigneduserid

left join cc_contact ownr
    on ownr.id = usr.contactid
)
select 
    claim_sk,
    subrogation_sk,
    claim_nbr,
    src_claim_id,
    src_subrogation_id,
    subrogation_owner_id,
    subrogation_owner_name,
    subrogation_group_id,
    subrogation_status_cd,
    subrogation_status_desc,
    subrogation_outcome_cd,
    subrogation_outcome_desc,
    subrogation_assignment_status_cd,
    subrogation_assignment_status_desc,
    subrogation_referral_comment,
    subrogation_referral_dttm,
    subrogation_assignment_dttm,
    src_create_dttm,
    src_update_dttm,
    file_ingestion_timestamp
from    
    cte_join
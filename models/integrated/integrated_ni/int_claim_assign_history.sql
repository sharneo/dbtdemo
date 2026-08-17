{#-

Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-01-01      0.0                             Aspire - original table materialization
2026-07-13      1.0                             Converted to incremental with merge strategy

-#}

{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 57_CLAIM_ASSIGN_HISTORY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A57
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_ASSIGN_HISTORY
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
        assigneduserid,
        assignedgroupid,
        state,
        reporteddate,
        isclaimmigrated_icare,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
      and state > 1
    {% if is_incremental() %}
        and file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_history as (
    select
        id,
        claimid,
        type,
        exposureid,
        dispute_icareid,
        matterid,
        workcapacitydecision_icareid,
        eventtimestamp,
        description
    from {{ ref('v_cc_history_current') }}
    where type = 5
      and exposureid is null
      and dispute_icareid is null
      and matterid is null
      and workcapacitydecision_icareid is null
),

base_cc_user as (
    select
        id,
        contactid
    from {{ ref('v_cc_user_current') }}
),

base_cc_contact as (
    select
        id,
        firstname,
        lastname
    from {{ ref('v_cc_contact_current') }}
),

base_cc_group as (
    select
        id,
        name
    from {{ ref('v_cc_group_current') }}
),

base_ccx_managingentity_icare as (
    select
        id,
        publicid,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_migrated_clm_with_hist as (
    select distinct
        clm.id as claimid,
        clm.reporteddate,
        hist.matterid
    from base_cc_claim as clm
    inner join base_cc_history as hist
        on hist.claimid = clm.id
    where clm.isclaimmigrated_icare = 1
),

cte_no_assign_hist as (
    select
        clm.id as claimid,
        5 as type,
        clm.reporteddate as eventtimestamp,
        concat('Assigned to user ',
            case when ctt.id is null then 'Pending Assignment' else concat(ctt.firstname, ' ', ctt.lastname) end,
            ' in group ', grp.name) as description,
        hist.matterid
    from base_cc_claim as clm
    left join base_cc_history as hist
        on hist.claimid = clm.id
    left join base_cc_user as usr
        on usr.id = clm.assigneduserid
    left join base_cc_contact as ctt
        on ctt.id = usr.contactid
    left join base_cc_group as grp
        on grp.id = clm.assignedgroupid
    where hist.id is null
),

cte_history as (
    select
        a.*,
        charindex(' in group ', a.description) as pos_of_group,
        len(a.description) as desc_length,
        cast(a.eventtimestamp as date) as eventdt,
        lead(cast(a.eventtimestamp as date)) over (partition by a.claimid order by a.eventtimestamp) as nexteventdt
    from (
        select claimid, 5 as type, reporteddate as eventtimestamp,
               'Assigned to user UNKNOWN in group UNKNOWN' as description, matterid
        from cte_migrated_clm_with_hist

        union

        select * from cte_no_assign_hist

        union

        select claimid, type, eventtimestamp, description, matterid
        from base_cc_history
        where left(description, 16) = 'Assigned to user'
    ) as a
),

final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.managingentity_icare as managing_entity_id,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'mge.publicid'
        ]) }} as varchar(150)) as managing_entity_sk,
        coalesce(mge.code, 'NI_ICARE') as managing_entity_cd,
        hist.claimid as src_claim_id,
        clm.claimnumber as claim_nbr,
        CAST(hist.eventtimestamp AS TIMESTAMP_NTZ)  as assign_dttm,
        hist.eventdt as assign_dt,
        hist.nexteventdt as next_assign_dt,
        hist.description as event_desc,
        hist.matterid as matter_id,
        substr(hist.description, 18, hist.pos_of_group - 17) as assign_user_name,
        substr(hist.description, hist.pos_of_group + 10, hist.desc_length - hist.pos_of_group + 9) as assign_team,
        case when hist.eventdt = hist.nexteventdt then 0 else 1 end as include_ind,
        case
            when hist.eventdt = hist.nexteventdt then 0
            else row_number() over (
                partition by hist.claimid
                order by case when hist.eventdt = hist.nexteventdt then 1 else 0 end, hist.eventtimestamp
            )
        end as earliest_history_rank,
        case
            when hist.eventdt = hist.nexteventdt then 0
            else row_number() over (
                partition by hist.claimid
                order by case when hist.eventdt = hist.nexteventdt then 1 else 0 end, hist.eventtimestamp desc
            )
        end as latest_history_rank,
        clm.file_ingestion_timestamp
    from cte_history as hist
    inner join base_cc_claim as clm
        on clm.id = hist.claimid
    left join base_ccx_managingentity_icare as mge
        on mge.id = clm.managingentity_icare
)

select
    claim_sk,
    src_system_cd,
    managing_entity_id,
    managing_entity_sk,
    managing_entity_cd,
    src_claim_id,
    claim_nbr,
    assign_dttm,
    assign_dt,
    next_assign_dt,
    event_desc,
    matter_id,
    assign_user_name,
    assign_team,
    include_ind,
    earliest_history_rank,
    latest_history_rank,
    file_ingestion_timestamp
from final
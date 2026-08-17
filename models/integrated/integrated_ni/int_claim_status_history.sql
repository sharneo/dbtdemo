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
    unique_key='claim_status_history_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 58_CLAIM_STATUS_HISTORY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A58
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_STATUS_HISTORY
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
        state,
        reporteddate,
        closedate,
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
        eventtimestamp,
        description,
        userid
    from {{ ref('v_cc_history_current') }}
    where (type = 16 and description = 'New claim saved')
        or (type = 11 and left(description, 12) = 'Claim closed')
        or (type = 20 and left(description, 12) = 'Claim reopen')
),

base_ccx_managingentity_icare as (
    select
        id,
        publicid,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
    where retired = 0
),

base_cctl_claimreopenedreason as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_claimreopenedreason_current') }}
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

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_migrated_clms as (
    select
        id as claimid,
        16 as type,
        cast(reporteddate as timestamp_ntz) as eventtimestamp,
        'New claim saved' as description,
        cast(closedate as timestamp_ntz) as claim_close_dttm,
        null as userid,
        1 as migrated_clm_ind,
        1 as earliest_history_row
    from base_cc_claim
    where isclaimmigrated_icare = 1
),

cte_history as (
    select
        hst.claimid,
        clm.claimnumber,
        clm.managingentity_icare,
        hst.type as event,
        lead(hst.type) over (partition by hst.claimid order by hst.eventtimestamp) as nextevent,
        case when hst.type = 16 then clm.reporteddate else hst.eventtimestamp end as eventdttm,
        case
            when hst.migrated_clm_ind = 1 and hst.claim_close_dttm is not null and hst.type = 16
                and lead(hst.type) over (partition by hst.claimid order by hst.eventtimestamp) is null
            then hst.claim_close_dttm
            else lead(hst.eventtimestamp) over (partition by hst.claimid order by hst.eventtimestamp)
        end as nexteventdttm,
        hst.description,
        hst.userid,
        hst.migrated_clm_ind,
        clm.source_system,
        clm.file_ingestion_timestamp
    from (
        select
            hist.claimid, hist.type, cast(hist.eventtimestamp as timestamp_ntz) as eventtimestamp, hist.description,
            cast(null as timestamp_ntz) as claim_close_dttm, hist.userid,
            case when mig.claimid is not null then 1 else 0 end as migrated_clm_ind,
            row_number() over (partition by hist.claimid order by hist.eventtimestamp) as earliest_history_row
        from base_cc_history as hist
        left join cte_migrated_clms as mig
            on mig.claimid = hist.claimid

        union all

        select claimid, type, eventtimestamp, description, claim_close_dttm, userid, migrated_clm_ind, earliest_history_row
        from cte_migrated_clms
    ) as hst
    inner join base_cc_claim as clm
        on clm.id = hst.claimid
    where not (hst.migrated_clm_ind = 1 and hst.type = 20 and hst.earliest_history_row = 1)
),

final as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'hist.source_system',
            'hist.claimnumber',
            'hist.eventdttm'
        ]) }} as varchar(150)) as claim_status_history_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'hist.source_system',
            'hist.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        hist.source_system as src_system_cd,
        hist.managingentity_icare as managing_entity_id,
        cast({{ dbt_utils.generate_surrogate_key([
            'hist.source_system',
            'mge.publicid'
        ]) }} as varchar(150)) as managing_entity_sk,
        coalesce(mge.code, 'NI_ICARE') as managing_entity_cd,
        hist.claimid as src_claim_id,
        hist.claimnumber as claim_nbr,
        hist.eventdttm as open_dttm,
        cast(hist.eventdttm as date) as open_dt,
        hist.nexteventdttm as close_dttm,
        cast(hist.nexteventdttm as date) as close_dt,
        hist.description as event_desc,
        opnrsn.typecode as reopen_reason_cd,
        opnrsn.name as reopen_reason_desc,
        case
            when cast(hist.eventdttm as date) = cast(hist.nexteventdttm as date) then 0
            when opnrsn.typecode in ('2', '3', '4') then 0
            else 1
        end as s59a_valid_open_ind,
        concat(ctt.firstname, ' ', ctt.lastname) as changed_by_user_name,
        row_number() over (partition by hist.claimid order by hist.eventdttm) as earliest_history_rank,
        row_number() over (partition by hist.claimid order by hist.eventdttm desc) as latest_history_rank,
        hist.file_ingestion_timestamp
    from cte_history as hist
    left join base_ccx_managingentity_icare as mge
        on mge.id = hist.managingentity_icare
    left join base_cctl_claimreopenedreason as opnrsn
        on hist.event = 20
        and opnrsn.id is not null
    left join base_cc_user as usr
        on usr.id = hist.userid
    left join base_cc_contact as ctt
        on ctt.id = usr.contactid
)

select
    claim_status_history_sk,
    claim_sk,
    src_system_cd,
    managing_entity_id,
    managing_entity_sk,
    managing_entity_cd,
    src_claim_id,
    claim_nbr,
    CAST(open_dttm AS TIMESTAMP_NTZ) as open_dttm,
    open_dt,
    CAST(close_dttm AS TIMESTAMP_NTZ) as close_dttm,
    close_dt,
    event_desc,
    reopen_reason_cd,
    reopen_reason_desc,
    s59a_valid_open_ind,
    changed_by_user_name,
    earliest_history_rank,
    latest_history_rank,
    file_ingestion_timestamp
from final
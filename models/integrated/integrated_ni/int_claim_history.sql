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
    unique_key='claim_history_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 54_CLAIM_HISTORY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A54
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_HISTORY
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_history as (
    select
        id,
        claimid,
        type,
        customtype,
        description,
        eventtimestamp
    from {{ ref('v_cc_history_current') }}
),

base_cctl_historytype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_historytype_current') }}
    where retired = 0
      and typecode in ('policyselected', 'custom')
),

base_cctl_customhistorytype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_customhistorytype_current') }}
    where typecode = 'transferred_icare'
),

base_ccx_managingentity_icare as (
    select
        id,
        publicid,
        code
    from {{ ref('v_ccx_managingentity_icare_current') }}
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_policy_event as (
    select
        clm.managingentity_icare as managing_entity_id,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        clm.source_system,
        hist.id as src_claim_hist_id,
        histtype.typecode as claim_hist_typecode,
        histtype.name as claim_hist_type,
        hist.description as claim_hist_desc,
        CAST(hist.eventtimestamp AS TIMESTAMP_NTZ) as claim_hist_dttm,
        case
            when histtype.typecode = 'policyselected' and hist.description like 'The policy was changed from%to%'
            then 1
            else 0
        end as policy_verified_event_ind,
        case
            when substr(hist.description, 33, 12) = 'SS_DOC_MGMNT'
                or substr(hist.description, 41, 12) = 'SS_DOC_MGMNT'
            then 1
            else 0
        end as claim_tfr_from_ssdocmgmt_ind,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    left join base_cc_history as hist
        on hist.claimid = clm.id
    inner join base_cctl_historytype as histtype
        on histtype.id = hist.type
    left join base_cctl_customhistorytype as ctyp
        on ctyp.id = hist.customtype
    where histtype.typecode = 'policyselected'
        or substr(hist.description, 33, 12) = 'SS_DOC_MGMNT'
        or substr(hist.description, 41, 12) = 'SS_DOC_MGMNT'
),

cte_join as (
    select
        CAST({{ dbt_utils.generate_surrogate_key(['pe.source_system', 'pe.claim_nbr','pe.src_claim_hist_id']) }} as varchar(150)) as claim_history_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'pe.source_system',
            'pe.claim_nbr'
        ]) }} as varchar(150)) as claim_sk,
        pe.source_system as src_system_cd,
        pe.managing_entity_id,
        cast({{ dbt_utils.generate_surrogate_key([
            'pe.source_system',
            'ent.publicid'
        ]) }} as varchar(150)) as managing_entity_sk,
        ent.code as managing_entity_cd,
        pe.claim_nbr,
        pe.src_claim_id,
        pe.src_claim_hist_id,
        pe.claim_hist_type,
        pe.claim_hist_desc,
        pe.claim_hist_dttm,
        pe.policy_verified_event_ind,
        pe.claim_tfr_from_ssdocmgmt_ind,
        case
            when pe.claim_hist_type = 'Policy selected or refreshed' and pe.policy_verified_event_ind = 1
            then row_number() over (
                partition by pe.claim_nbr, pe.claim_hist_type
                order by pe.policy_verified_event_ind desc, pe.claim_hist_dttm asc, pe.src_claim_hist_id asc
            )
            else 0
        end as policy_verified_event_rank_asc,
        case
            when pe.policy_verified_event_ind = 1
                and row_number() over (
                    partition by pe.claim_nbr, pe.claim_hist_type
                    order by pe.policy_verified_event_ind desc, pe.claim_hist_dttm asc, pe.src_claim_hist_id asc
                ) = 1
            then 'Y'
            else 'N'
        end as first_policy_verified_event_ind,
        case
            when pe.claim_hist_type = 'Custom' and pe.claim_tfr_from_ssdocmgmt_ind = 1
            then row_number() over (
                partition by pe.claim_nbr, pe.claim_hist_type
                order by pe.claim_tfr_from_ssdocmgmt_ind desc, pe.claim_hist_dttm asc, pe.src_claim_hist_id asc
            )
            else 0
        end as claim_tfr_from_ssdocmgmt_rank_asc,
        case
            when pe.claim_tfr_from_ssdocmgmt_ind = 1
                and row_number() over (
                    partition by pe.claim_nbr, pe.claim_hist_type
                    order by pe.claim_tfr_from_ssdocmgmt_ind desc, pe.claim_hist_dttm asc, pe.src_claim_hist_id asc
                ) = 1
            then 'Y'
            else 'N'
        end as first_claim_tfr_from_ssdocmgmt_ind,
        pe.file_ingestion_timestamp
    from cte_policy_event as pe
    left join base_ccx_managingentity_icare as ent
        on ent.id = pe.managing_entity_id
    where pe.policy_verified_event_ind = 1
        or pe.claim_tfr_from_ssdocmgmt_ind = 1
)

select
    claim_history_sk,
    claim_sk,
    src_system_cd,
    managing_entity_id,
    managing_entity_sk,
    managing_entity_cd,
    claim_nbr,
    src_claim_id,
    src_claim_hist_id,
    claim_hist_type,
    claim_hist_desc,
    claim_hist_dttm,
    policy_verified_event_ind,
    claim_tfr_from_ssdocmgmt_ind,
    policy_verified_event_rank_asc,
    first_policy_verified_event_ind,
    claim_tfr_from_ssdocmgmt_rank_asc,
    first_claim_tfr_from_ssdocmgmt_ind,
    file_ingestion_timestamp
from cte_join

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
    unique_key='claim_litigation_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: L02_CLAIM_LITIGATION_DETAILS.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_L02
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_LITIG_DET
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_matter as (
    select
        id,
        claimid,
        casenumber,
        significantlitigation_icare,
        courttype,
        keyissues_icare,
        newlitstranote_icare,
        litigationstrategy_icare,
        createtime
    from {{ ref('v_cc_matter_current') }}
    where retired = 0
),

base_cctl_mattercourttype as (
    select
        id,
        description
    from {{ ref('v_cctl_mattercourttype_current') }}
    where retired = 0
),

base_cctl_signifilitig_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_signifilitig_icare_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_main as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber',
            'm.casenumber'
        ]) }} as varchar(150)) as claim_litigation_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        m.casenumber as matternumber,
        m.significantlitigation_icare as court_type_cd,
        mct.description as court_type,
        sli.description as significant_litigation,
        m.keyissues_icare as key_issues,
        m.newlitstranote_icare as litigation_strategy,
        m.litigationstrategy_icare as litigation_notes,
        case
            when row_number() over (partition by m.casenumber order by m.createtime desc) = 1 then 'Y'
            else 'N'
        end as latest_record_ind,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    inner join base_cc_matter as m
        on clm.id = m.claimid
    left join base_cctl_mattercourttype as mct
        on mct.id = m.courttype
    left join base_cctl_signifilitig_icare as sli
        on sli.id = m.significantlitigation_icare
)

select
    claim_litigation_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    matternumber,
    court_type_cd,
    court_type,
    significant_litigation,
    key_issues,
    litigation_strategy,
    litigation_notes,
    latest_record_ind,
    file_ingestion_timestamp
from cte_main
where latest_record_ind = 'Y'

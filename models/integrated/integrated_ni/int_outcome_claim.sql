{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-04-20      1.0                             Converted to incremental with merge strategy

-#}

{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#-
  Source: D05_DISP_OUTCOME_CLAIM.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_D05
  TBL_NM: MSC_QLK_ASPIRE_OUTCOME_CLAIM
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        source_system,
        retired
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_matter as (
    select
        id,
        claimid,
        name,
        casenumber,
        resolution,
        finallegalcost,
        finalsettlecost,
        finalsettledate,
        commonlawid,
        createtime,
        retired
    from {{ ref('v_cc_matter_current') }}
    where retired = 0
),

base_cctl_resolutiontype as (
    select
        id,
        name,
        retired
    from {{ ref('v_cctl_resolutiontype_current') }}
    where retired = 0
),

base_ccx_commonlaw_icare as (
    select
        id,
        ourlegalcosts_icare,
        settlementcost_icare,
        settlementdate_icare,
        retired
    from {{ ref('v_ccx_commonlaw_icare_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_ranked as (
    select
        cast({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'clm.claimnumber']) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        m.name as matter_name,
        m.casenumber as matter_number,
        res.name as outcome,
        case when m.finallegalcost is not null then '$' || to_varchar(m.finallegalcost, '999,999,999,999.00')
             else '$' || to_varchar(outc.ourlegalcosts_icare, '999,999,999,999.00')
        end as final_legal_cost,
        case when m.finalsettlecost is not null then '$' || to_varchar(m.finalsettlecost, '999,999,999,999.00')
             else '$' || to_varchar(outc.settlementcost_icare, '999,999,999,999.00')
        end as final_settlement_cost,
        case when m.finalsettledate is not null then cast(m.finalsettledate as date)
             else cast(outc.settlementdate_icare as date)
        end as final_settlement_date,
        case when row_number() over (partition by m.casenumber order by m.createtime desc) = 1 then 'Y'
             else 'N'
        end as latest_record_ind
    from base_cc_claim as clm
    inner join base_cc_matter as m
        on clm.id = m.claimid
    left join base_cctl_resolutiontype as res
        on m.resolution = res.id
    left join base_ccx_commonlaw_icare as outc
        on outc.id = m.commonlawid
),

cte_join as (
    select *
    from cte_ranked
    where latest_record_ind = 'Y'
)

select
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    matter_name,
    matter_number,
    outcome,
    final_legal_cost,
    final_settlement_cost,
    final_settlement_date,
    latest_record_ind
from cte_join
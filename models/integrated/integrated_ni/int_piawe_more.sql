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
    unique_key='piawe_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 61_PIAWE_MORE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A61
  TBL_NM: MSC_QLK_ASPIRE_PIAWE_ADD
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

base_ccx_piawe_icare as (
    select
        id,
        exposureid,
        effectivedate_icare,
        piawetype_icare,
        piawefirst52_icare,
        piawelater52_icare,
        piawedeactivated_icare,
        draft,
        createtime,
        updatetime
    from {{ ref('v_ccx_piawe_icare_current') }}
    where retired = 0
      and draft = 0
      and piawedeactivated_icare = 0
),

base_cc_exposure as (
    select
        id,
        claimid
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
),

base_cctl_piawetype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_piawetype_icare_current') }}
),

base_ccx_benefitsaccrual_icare as (
    select
        id,
        exposureid,
        totalweekspaid
    from {{ ref('v_ccx_benefitsaccrual_icare_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_join as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber',
            'pia.id'
        ]) }} as varchar(150)) as piawe_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        pia.id as src_piawe_id,
        pia.effectivedate_icare as piawe_effective_dt,
        coalesce(
            lead(pia.effectivedate_icare) over (
                partition by clm.claimnumber
                order by pia.effectivedate_icare, pia.createtime, pia.updatetime, pia.id desc
            ),
            current_timestamp()
        ) as piawe_expiry_dt,
        dimpiawe.typecode as piawe_type,
        dimpiawe.name as piawe_type_desc,
        pia.createtime as src_create_dttm,
        cast(pia.createtime as date) as src_create_dt,
        pia.updatetime as src_update_dttm,
        cast(pia.updatetime as date) as src_update_dt,
        case
            when dimpiawe.typecode = 'manual_icare' and coalesce(bacc.totalweekspaid, 0) > 52
            then pia.piawelater52_icare
            else pia.piawefirst52_icare
        end as piawe_amount,
        row_number() over (
            partition by clm.claimnumber
            order by pia.effectivedate_icare desc, pia.createtime desc, pia.updatetime desc, pia.id
        ) as latest_piawe_rank,
        row_number() over (
            partition by clm.claimnumber
            order by pia.createtime desc, pia.effectivedate_icare desc, pia.updatetime desc, pia.id
        ) as latest_create_rank,
        clm.file_ingestion_timestamp
    from base_ccx_piawe_icare as pia
    inner join base_cc_exposure as xpr
        on xpr.id = pia.exposureid
    inner join base_cc_claim as clm
        on clm.id = xpr.claimid
    left join base_cctl_piawetype_icare as dimpiawe
        on dimpiawe.id = pia.piawetype_icare
    left join base_ccx_benefitsaccrual_icare as bacc
        on bacc.exposureid = xpr.id
)

select
    piawe_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    src_piawe_id,
    piawe_effective_dt,
    piawe_expiry_dt,
    piawe_type,
    piawe_type_desc,
    src_create_dttm,
    src_create_dt,
    src_update_dttm,
    src_update_dt,
    piawe_amount,
    latest_piawe_rank,
    latest_create_rank,
    file_ingestion_timestamp
from cte_join

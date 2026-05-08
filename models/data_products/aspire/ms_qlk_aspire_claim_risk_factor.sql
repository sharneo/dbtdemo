{{
  config(
    materialized='incremental',
    unique_key='src_claim_id',
    incremental_strategy='merge'
  )
}}

{#
  Source: 12_CLAIM_RISK_FACTOR.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A12
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_RISK_FACTOR
#}

with ccx_triageriskfactor_icare as (
    select
        claimid,
        publicid,
        id,
        newfactor,
        retired,
        createtime,
        updatetime,
        riskfactor,
        file_ingestion_timestamp
    from {{ ref('v_ccx_triageriskfactor_icare_current') }}
),

cctl_riskfactors_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_riskfactors_icare_current') }}
)

select
    risk.claimid as src_claim_id,
    risk.publicid as risk_factor_id,
    risk.id as src_risk_factor_id,
    case
        when risk.newfactor = 0 then 'N'
        when risk.newfactor = 1 then 'Y'
    end as new_risk_factor_ind,
    case
        when risk.retired = 0 then 'N'
        else 'Y'
    end as retired_ind,
    risk.createtime as src_create_dttm,
    cast(risk.createtime as date) as src_create_dt,
    risk.updatetime as src_eff_dttm,
    cast(risk.updatetime as date) as src_eff_dt,
    dimrisk.typecode as risk_factor_cd,
    dimrisk.name as risk_factor_desc,
    risk.file_ingestion_timestamp

from ccx_triageriskfactor_icare risk

left join cctl_riskfactors_icare dimrisk
    on risk.riskfactor = dimrisk.id

{% if is_incremental() %}
where risk.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% else %}
where 1=1
{% endif %}

{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for claim risk factor.
                                                claim_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_risk_factor_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 12_CLAIM_RISK_FACTOR.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A12
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_RISK_FACTOR
-#}

with ccx_triageriskfactor_icare as (
    select
        id,
        claimid,
        publicid,
        newfactor,
        retired,
        createtime,
        updatetime,
        riskfactor,
        file_ingestion_timestamp
    from {{ ref('v_ccx_triageriskfactor_icare_current') }}
    {% if is_incremental() %}
        WHERE file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cctl_riskfactors_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_riskfactors_icare_current') }}
),
cte_join as 
(
select
    risk.claimid as src_claim_id,
    risk.publicid as risk_factor_id,
    risk.id as src_risk_factor_id,
    case risk.newfactor
        when 0 then 'N'
        when 1 then 'Y'
    end as new_risk_factor_ind,
    case risk.retired
        when 0 then 'N'
        else 'Y'
    end as retired_ind,
    CAST(risk.createtime as TIMESTAMP_NTZ) as  src_create_dttm,
    cast(risk.createtime as date) as src_create_dt,
    CAST(risk.updatetime as TIMESTAMP_NTZ) as src_eff_dttm,
    cast(risk.updatetime as date) as src_eff_dt,
    dimrisk.typecode as risk_factor_cd,
    dimrisk.name as risk_factor_desc,
    current_date() as extract_date,
    risk.file_ingestion_timestamp

from ccx_triageriskfactor_icare risk

left join cctl_riskfactors_icare dimrisk
    on risk.riskfactor = dimrisk.id
)
select  
    src_claim_id,
    risk_factor_id,
    src_risk_factor_id,
    new_risk_factor_ind,
    retired_ind,
    src_create_dttm,
    src_create_dt,
    src_eff_dttm,
    src_eff_dt,
    risk_factor_cd,
    risk_factor_desc,
    file_ingestion_timestamp
from cte_join
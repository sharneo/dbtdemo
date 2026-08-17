{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-06-02      1.0                             Converted to incremental with merge strategy

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
  Source: 62_CLAIM_SECURITY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A62
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_SECURITY
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
        claimsagent_icare,
        lodgingagent_icare,
        policyid,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where 1=1
    {% if is_incremental() %}
    and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

ccx_managingentity_icare as (
    select
        id,
        code,
        name
    from {{ ref('v_ccx_managingentity_icare_current') }}
),

cctl_claimagent_icare as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimagent_icare_current') }}
),

cc_policy as (
    select
        id,
        policynumber,
        policytype
    from {{ ref('v_cc_policy_current') }}
),

cctl_policytype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_policytype_current') }}
),

cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    ccxmng.code as managing_entity_cd,
    ccxmng.name as managingentity_name,
    COALESCE(clmagt1.typecode, clmagt2.typecode) as insurernumber,
    pol.policynumber as policynumber,
    pol.policytype as policytype_id,
    poltyp.typecode as policytype_cd,
    clm.file_ingestion_timestamp

from cc_claim clm
left join ccx_managingentity_icare ccxmng
    on ccxmng.id = clm.managingentity_icare
left join cctl_claimagent_icare clmagt1
    on clmagt1.id = clm.claimsagent_icare
left join cctl_claimagent_icare clmagt2
    on clmagt2.id = clm.lodgingagent_icare
left join cc_policy pol
    on pol.id = clm.policyid
left join cctl_policytype poltyp
    on poltyp.id = pol.policytype
)
select 
    claim_sk,
    source_system,
    src_claim_id,
    claim_nbr,
    managing_entity_cd,
    managingentity_name,
    insurernumber,
    policynumber,
    policytype_id,
    policytype_cd,
    file_ingestion_timestamp
from cte_join
qualify row_number() over (partition by claim_nbr order by file_ingestion_timestamp desc) = 1

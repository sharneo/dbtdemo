{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental Model for claim cost centre.

-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_claim_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 36_CLAIM_COST_CENTRE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A36
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_COST_CENTRE
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

ccx_claimcostcentreicare as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_ccx_claimcostcentreicare_current') }}
),

ccx_costcentre_icare as (
    select
        id,
        number,
        name,
        othername
    from {{ ref('v_ccx_costcentre_icare_current') }}
    where retired = 0
),
cte_join AS
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as src_system_cd,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    c2.number as cost_centre_number,
    c2.name as cost_centre_name,
    c2.othername as cost_centre_othername,
    clm.file_ingestion_timestamp
from cc_claim clm
inner join ccx_claimcostcentreicare c1
    on c1.ownerid = clm.id
left join ccx_costcentre_icare c2
    on c2.id = c1.foreignentityid
)
select
    claim_sk,
    src_system_cd,
    src_claim_id,
    claim_nbr,
    cost_centre_number,
    cost_centre_name,
    cost_centre_othername,
    file_ingestion_timestamp
from
    cte_join
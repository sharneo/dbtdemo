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
    unique_key='rehab_case_mgmt_service_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 23_REHAB_CASE_MANAGEMENT_SERVICE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A23
  TBL_NM: MSC_QLK_ASPIRE_REHAB_CASE_MANAGEMENT_SERVICE
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

base_ccx_theplanservices_icare as (
    select
        id,
        claim,
        servicetype,
        servicestatus
    from {{ ref('v_ccx_theplanservices_icare_current') }}
),

base_cctl_casemgmtservice_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_casemgmtservice_icare_current') }}
),

base_cctl_casemgmtstatus_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_casemgmtstatus_icare_current') }}
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_join as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber',
            'thepln.id'
        ]) }} as varchar(150)) as rehab_case_mgmt_service_sk,
        cast({{ dbt_utils.generate_surrogate_key([
            'clm.source_system',
            'clm.claimnumber'
        ]) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.id as src_claim_id,
        clm.claimnumber as claim_nbr,
        dimcase.name as case_management_service_desc,
        dimcasestus.name as case_management_status_desc,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    inner join base_ccx_theplanservices_icare as thepln
        on clm.id = thepln.claim
    inner join base_cctl_casemgmtservice_icare as dimcase
        on thepln.servicetype = dimcase.id
    inner join base_cctl_casemgmtstatus_icare as dimcasestus
        on thepln.servicestatus = dimcasestus.id
)

select
    rehab_case_mgmt_service_sk,
    claim_sk,
    src_system_cd,
    src_claim_id,
    claim_nbr,
    case_management_service_desc,
    case_management_status_desc,
    file_ingestion_timestamp
from cte_join

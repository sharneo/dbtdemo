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
    unique_key='claim_event_hist_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: L04_CLAIM_EVENT_HIST.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_L04
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_EVENT_HIST
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
        casenumber
    from {{ ref('v_cc_matter_current') }}
    where retired = 0
),

base_ccx_matterevent_icare as (
    select
        id,
        matterid,
        eventtype,
        eventdate,
        eventresult
    from {{ ref('v_ccx_matterevent_icare_current') }}
    where retired = 0
),

base_cctl_eventtype_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_eventtype_icare_current') }}
    where retired = 0
),

base_cctl_mattereventresult_icare as (
    select
        id,
        description
    from {{ ref('v_cctl_mattereventresult_icare_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_join as (
    select
         CAST({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'clm.claimnumber','mei.id']) }} as varchar(150)) as claim_event_hist_sk,
         CAST({{ dbt_utils.generate_surrogate_key(['clm.source_system', 'clm.claimnumber']) }} as varchar(150)) as claim_sk,
        clm.source_system as src_system_cd,
        clm.claimnumber as claim_nbr,
        clm.id as src_claim_id,
        m.casenumber as matternumber,
        mei.eventtype as event_type_cd,
        eti.description as event_type_desc,
        cast(mei.eventdate as date) as event_date,
        mei.eventresult as event_result_cd,
        mer.description as event_result_desc,
        clm.file_ingestion_timestamp
    from base_cc_claim as clm
    inner join base_cc_matter as m
        on clm.id = m.claimid
    left join base_ccx_matterevent_icare as mei
        on mei.matterid = m.id
    left join base_cctl_eventtype_icare as eti
        on eti.id = mei.eventtype
    left join base_cctl_mattereventresult_icare as mer
        on mer.id = mei.eventresult
)

select
    claim_event_hist_sk,
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    matternumber,
    event_type_cd,
    event_type_desc,
    event_date,
    event_result_cd,
    event_result_desc,
    file_ingestion_timestamp
from cte_join

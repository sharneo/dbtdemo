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
  Source: 55_CLAIM_REOPEN_HISTORY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A55
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_REOPEN_HISTORY
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        managingentity_icare,
        retired,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        AND  file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_history as (
    select
        id,
        claimid,
        type,
        description,
        eventtimestamp,
        exposureid
    from {{ ref('v_cc_history_current') }}
),

cctl_historytype as (
    select
        id,
        typecode,
        name,
        retired
    from {{ ref('v_cctl_historytype_current') }}
    where retired = 0
),

ccx_managingentity_icare as (
    select
        id,
        publicid,
        code,
        source_system
    from {{ ref('v_ccx_managingentity_icare_current') }}
),

cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.managingentity_icare as managing_entity_id,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'ent.publicid'
    ]) }} as varchar(150)) as managing_entity_sk,
    ent.code as managing_entity_cd,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    hist.id as src_claim_hist_id,
    histtype.name as claim_hist_type,
    hist.description as claim_hist_desc,
    cast(hist.eventtimestamp as date) as reopened_dt,
    CAST(hist.eventtimestamp  AS TIMESTAMP_NTZ) as  reopened_dttm,
    month(hist.eventtimestamp) as reopened_mth,
    year(hist.eventtimestamp) as reopened_yr,
    date_part(week, hist.eventtimestamp) as reopened_wk_number,
    case when row_number() over (partition by clm.claimnumber, year(hist.eventtimestamp) order by hist.eventtimestamp desc) = 1 then 1 else 0 end as reopened_unique_yr_ind,
    case when row_number() over (partition by clm.claimnumber, month(hist.eventtimestamp), year(hist.eventtimestamp) order by hist.eventtimestamp desc) = 1 then 1 else 0 end as reopened_unique_monthyr_ind,
    case when row_number() over (partition by clm.claimnumber, date_part(week, hist.eventtimestamp), year(hist.eventtimestamp) order by hist.eventtimestamp desc) = 1 then 1 else 0 end as reopened_unique_weekyr_ind,
    clm.file_ingestion_timestamp

from cc_claim clm

left join cc_history hist
    on clm.id = hist.claimid

left join cctl_historytype histtype
    on histtype.id = hist.type

left join ccx_managingentity_icare ent
    on ent.id = clm.managingentity_icare

where histtype.typecode = 'reopened'
and hist.exposureid is null
)

SELECT 
    claim_sk,
    source_system,
    managing_entity_id,
    managing_entity_sk,
    managing_entity_cd,
    claim_nbr,
    src_claim_id,
    src_claim_hist_id,
    claim_hist_type,
    claim_hist_desc,
    reopened_dt,
    reopened_dttm,
    reopened_mth,
    reopened_yr,
    reopened_wk_number,
    reopened_unique_yr_ind,
    reopened_unique_monthyr_ind,
    reopened_unique_weekyr_ind,
    file_ingestion_timestamp
from
    cte_join

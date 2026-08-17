{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for claim screen action.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_claim_screen_action_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 34_CLAIM_SCREEN_ACTION.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A34
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_SCREEN_ACTION
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

ccx_claimscreening_icare as (
    select
        id,
        claimid,
        publicid,
        claimscreenactioncode,
        claimscreeningdate,
        createtime,
        createuserid
    from {{ ref('v_ccx_claimscreening_icare_current') }}
    where retired = 0
),

cctl_claimscreenaction_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_claimscreenaction_icare_current') }}
    where retired = 0
),

cc_user as (
    select
        id,
        publicid
    from {{ ref('v_cc_user_current') }}
),

cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    clmscrn.id as src_claim_screen_action_id,
    clmscrn.publicid as claim_public_id,
    clmscrn.claimscreenactioncode as claim_screen_action_cd,
    scrncode.name as claim_screen_action_desc,
    CAST(clmscrn.claimscreeningdate AS TIMESTAMP_NTZ) as  claim_screen_action_dttm,
    cast(clmscrn.claimscreeningdate as date) as claim_screen_action_dt,
    CAST(clmscrn.createtime AS TIMESTAMP_NTZ) as src_create_dttm,
    cast(clmscrn.createtime as date) as src_create_dt,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'usr.publicid'
    ]) }} as varchar(150)) as screen_action_create_user_sk,
    row_number() over (partition by clm.id order by clmscrn.createtime desc) as latest_screen_action_rank,
    row_number() over (partition by clm.id order by clmscrn.createtime asc) as earliest_screen_action_rank,
    clm.file_ingestion_timestamp
from ccx_claimscreening_icare clmscrn
inner join cc_claim clm
    on clm.id = clmscrn.claimid
inner join cctl_claimscreenaction_icare scrncode
    on scrncode.id = clmscrn.claimscreenactioncode
inner join cc_user usr
    on usr.id = clmscrn.createuserid
)
select 
    claim_sk,
    source_system,
    claim_nbr,
    src_claim_id,
    src_claim_screen_action_id,
    claim_public_id,
    claim_screen_action_cd,
    claim_screen_action_desc,
    claim_screen_action_dttm,
    claim_screen_action_dt,
    src_create_dttm,
    src_create_dt,
    screen_action_create_user_sk,
    latest_screen_action_rank,
    earliest_screen_action_rank,
    file_ingestion_timestamp
from
    cte_join
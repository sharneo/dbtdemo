{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for user-team relationship.
-#}   

{{
  config(
    materialized='incremental',
    unique_key=['user_sk', 'team_sk'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 29_USER_TEAM.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A29
  TBL_NM: MSC_QLK_ASPIRE_USER_TEAM
-#}

with cc_groupuser as (
    select
        groupid,
        userid,
        file_ingestion_timestamp
    from {{ ref('v_cc_groupuser_current') }}
    {% if is_incremental() %}
    WHERE file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_group as (
    select
        id,
        publicid
    from {{ ref('v_cc_group_current') }}
    where retired = 0
),

cc_user as (
    select
        id,
        publicid,
        source_system
    from {{ ref('v_cc_user_current') }}
    where retired = 0
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'usr.source_system',
        'usr.publicid'
    ]) }} as varchar(150)) as user_sk,
    cast({{ dbt_utils.generate_surrogate_key([
        'usr.source_system',
        'grp.publicid'
    ]) }} as varchar(150)) as team_sk,
    usr.source_system as src_system_cd,
    grpusr.file_ingestion_timestamp

from cc_groupuser grpusr

left join cc_group grp
    on grpusr.groupid = grp.id

left join cc_user usr
    on grpusr.userid = usr.id
)
select 
    user_sk,
    team_sk,
    src_system_cd,
    file_ingestion_timestamp
from    
    cte_join
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for team hierarchy (parent-child).
-#}   

{{
  config(
    materialized='incremental',
    unique_key=['src_parent_team_id', 'src_child_team_id'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 26_TEAM_HIER.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A26
  TBL_NM: MSC_QLK_ASPIRE_TEAM_HIER
-#}

with cc_parentgroup as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_cc_parentgroup_current') }}
),

cc_group as (
    select
        id,
        name,
        publicid,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_group_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'parngrp.source_system',
        'parngrp.publicid'
    ]) }} as varchar(150)) as parent_team_sk,
    parngrp.name as parent_team_name,
    parngrp.publicid as parent_team_id,
    parngrp.id as src_parent_team_id,
    cast({{ dbt_utils.generate_surrogate_key([
        'childgrp.source_system',
        'childgrp.publicid'
    ]) }} as varchar(150)) as child_team_sk,
    childgrp.name as child_team_name,
    childgrp.publicid as child_team_id,
    childgrp.id as src_child_team_id,
    parngrp.file_ingestion_timestamp

from cc_parentgroup pgrp
inner join cc_group parngrp
    on parngrp.id = pgrp.foreignentityid
inner join cc_group childgrp
    on childgrp.id = pgrp.ownerid
)
select 
    parent_team_sk,
    parent_team_name,
    parent_team_id,
    src_parent_team_id,
    child_team_sk,
    child_team_name,
    child_team_id,
    src_child_team_id,
    file_ingestion_timestamp,    
from
    cte_join
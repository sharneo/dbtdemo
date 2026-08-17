{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for team hierarchy (6-level).
-#}   

{{
  config(
    materialized='incremental',
    unique_key='level_leaf_src_team_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 25_TEAM.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A25
  TBL_NM: MSC_QLK_ASPIRE_TEAM
-#}

with cc_group as (
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

cc_parentgroup as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_cc_parentgroup_current') }}
),

cte_group as (
    select
        grp1.name as team_name,
        grp1.id as src_team_id,
        grp1.source_system as source_system,
        cast({{ dbt_utils.generate_surrogate_key([
            'grp1.source_system',
            'grp1.publicid'
        ]) }} as varchar(150)) as team_sk,
        pgrp1.ownerid as child_src_team_id
    from cc_group grp1
    left join cc_parentgroup pgrp1
        on grp1.id = pgrp1.foreignentityid
),
cte_join as 
(
select
    case
        when lvl1.team_name is null then
            case when lvl2.team_name is null then
                case when lvl3.team_name is null then
                    case when lvl4.team_name is null then
                        case when lvl5.team_name is null then
                            case when lvl6.team_name is null then grpleaf.name
                            else lvl6.team_name end
                        else lvl5.team_name end
                    else lvl4.team_name end
                else lvl3.team_name end
            else lvl2.team_name end
        else lvl1.team_name end as level_01_team_name,
    case
        when lvl1.team_name is not null then lvl2.team_name
        else case when lvl2.team_name is not null then lvl3.team_name
            else case when lvl3.team_name is not null then lvl4.team_name
                else case when lvl4.team_name is not null then lvl5.team_name
                    else case when lvl5.team_name is not null then lvl6.team_name
                        else case when lvl6.team_name is not null then grpleaf.name
                            else grpleaf.name end end end end end end as level_02_team_name,
    case
        when lvl1.team_name is not null then lvl3.team_name
        else case when lvl2.team_name is not null then lvl4.team_name
            else case when lvl3.team_name is not null then lvl5.team_name
                else case when lvl4.team_name is not null then lvl6.team_name
                    else case when lvl5.team_name is not null then grpleaf.name
                        else grpleaf.name end end end end end as level_03_team_name,
    case
        when lvl1.team_name is not null then lvl4.team_name
        else case when lvl2.team_name is not null then lvl5.team_name
            else case when lvl3.team_name is not null then lvl6.team_name
                else case when lvl4.team_name is not null then grpleaf.name
                    else grpleaf.name end end end end as level_04_team_name,
    case
        when lvl1.team_name is not null then lvl5.team_name
        else case when lvl2.team_name is not null then lvl6.team_name
            else case when lvl3.team_name is not null then grpleaf.name
                else grpleaf.name end end end as level_05_team_name,
    case
        when lvl1.team_name is not null then lvl6.team_name
        else case when lvl2.team_name is not null then grpleaf.name
            else grpleaf.name end end as level_06_team_name,
    cast({{ dbt_utils.generate_surrogate_key([
        'grpleaf.source_system',
        'grpleaf.publicid'
    ]) }} as varchar(150)) as level_leaf_team_sk,
    grpleaf.id as level_leaf_src_team_id,
    grpleaf.name as level_leaf_team_name,
    grpleaf.file_ingestion_timestamp

from cc_group grpleaf

left join cte_group lvl6
    on grpleaf.id = lvl6.child_src_team_id

left join cte_group lvl5
    on lvl6.src_team_id = lvl5.child_src_team_id

left join cte_group lvl4
    on lvl5.src_team_id = lvl4.child_src_team_id

left join cte_group lvl3
    on lvl4.src_team_id = lvl3.child_src_team_id

left join cte_group lvl2
    on lvl3.src_team_id = lvl2.child_src_team_id

left join cte_group lvl1
    on lvl2.src_team_id = lvl1.child_src_team_id
)
select 
    level_01_team_name,
    level_02_team_name,
    level_03_team_name,
    level_04_team_name,
    level_05_team_name,
    level_06_team_name,
    level_leaf_team_sk,
    level_leaf_src_team_id,
    level_leaf_team_name,
    file_ingestion_timestamp
from
    cte_join
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental model for claim assignment rule.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_assignment_rule_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 39_CLAIM_ASSIGNMENT_RULE.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A39
  TBL_NM: MSC_QLK_ASPIRE_CLAIM_ASSIGNMENT_RULE
-#}

with ccx_clmassignmentrules_icare as (
    select
        id,
        publicid,
        policynumber,
        policyname,
        groupnumber,
        groupname,
        policystatus,
        commencementdate,
        effecitvedate,
        expirydate,
        createtime,
        updatetime,
        file_ingestion_timestamp,
        source_system
    from {{ ref('v_ccx_clmassignmentrules_icare_current') }}
    where retired = 0
    {% if is_incremental() %}
        AND  file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cctl_policystatus as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_policystatus_current') }}
    where retired = 0
),
cte_join as 
(
SELECT 
    cast({{ dbt_utils.generate_surrogate_key([
        'rul.source_system',
        'rul.publicid'
    ]) }} as varchar(150)) as assignment_rule_sk,
    rul.publicid as assignment_rule_id,
    rul.id as src_assignment_rule_id,
    rul.policynumber as policy_nbr,
    rul.policyname as policy_name,
    rul.groupnumber as policy_group_nbr,
    rul.groupname as policy_group_name,
    sta.typecode as policy_status_cd,
    sta.name as policy_status_desc,
    CAST(rul.commencementdate as TIMESTAMP_NTZ) as  commencement_dttm,
    CAST(rul.effecitvedate as TIMESTAMP_NTZ) as term_start_dttm,
    CAST(rul.expirydate as TIMESTAMP_NTZ) as term_end_dttm,
    CAST(rul.createtime as  TIMESTAMP_NTZ) as  src_create_dttm,
    CAST(rul.updatetime as TIMESTAMP_NTZ) as src_update_dttm,
    rul.file_ingestion_timestamp
from ccx_clmassignmentrules_icare rul
left join cctl_policystatus sta
    on sta.id = rul.policystatus
)
SELECT 
    assignment_rule_sk,
    assignment_rule_id,
    src_assignment_rule_id,
    policy_nbr,
    policy_name,
    policy_group_nbr,
    policy_group_name,
    policy_status_cd,
    policy_status_desc,
    commencement_dttm,
    term_start_dttm,
    term_end_dttm,
    src_create_dttm,
    src_update_dttm,
    file_ingestion_timestamp
FROM 
    cte_join
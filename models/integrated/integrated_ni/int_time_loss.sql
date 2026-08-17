{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for time loss.
                                                claim_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_lost_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 27_TIME_LOSS.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A27
  TBL_NM: MSC_QLK_ASPIRE_TIME_LOSS
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        claimworkcompid,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_workcomp as (
    select
        id,
        timelossreport
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
        and timelossreport = 1
),

ccx_losttimerecord_icare as (
    select
        id,
        claimworkcompid,
        createtime,
        estresumeworkdate,
        ceasedworkdate,
        actualresumedworkdate,
        updatetime,
        retired
    from {{ ref('v_ccx_losttimerecord_icare_current') }}
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    source_system as src_system_cd,
    clm.claimnumber as claim_nbr,
    lost.createtime as src_create_dttm,
    lost.id as src_lost_id,
    cast(lost.createtime as date) as src_create_dt,
    lost.estresumeworkdate as est_resume_work_dttm,
    cast(lost.estresumeworkdate as date) as est_resume_work_dt,
    lost.ceasedworkdate as ceased_work_dttm,
    cast(lost.ceasedworkdate as date) as ceased_work_dt,
    lost.actualresumedworkdate as actual_resumed_work_dttm,
    cast(lost.actualresumedworkdate as date) as actual_resumed_work_dt,
    datediff(day, cast(lost.ceasedworkdate as date), cast(lost.actualresumedworkdate as date)) as number_days_off_work,
    min(cast(lost.ceasedworkdate as date)) over (partition by clm.claimnumber) as original_ceased_work_dt,
    max(cast(lost.estresumeworkdate as date)) over (partition by clm.claimnumber) as last_est_resumed_work_dt,
    max(cast(lost.actualresumedworkdate as date)) over (partition by clm.claimnumber) as last_actual_resumed_work_dt,
    sum(datediff(day, cast(lost.ceasedworkdate as date), cast(lost.actualresumedworkdate as date))) over (partition by clm.claimnumber) as total_number_days_off_work,
    row_number() over (partition by clm.claimnumber order by lost.ceasedworkdate asc, lost.createtime) as earliest_time_loss_rank,
    'N' as retired_ind,
    lost.updatetime as src_updated_dttm,
    cast(lost.updatetime as date) as src_updated_dt,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_workcomp wrk
    on clm.claimworkcompid = wrk.id

inner join ccx_losttimerecord_icare lost
    on lost.claimworkcompid = wrk.id
    and lost.retired = 0

union all

select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    source_system as src_system_cd,
    clm.claimnumber as claim_nbr,
    CAST(lost.createtime as TIMESTAMP_NTZ) as  src_create_dttm,
    lost.id as src_lost_id,
    cast(lost.createtime as date) as src_create_dt,
    CAST(lost.estresumeworkdate as TIMESTAMP_NTZ) as est_resume_work_dttm,
    cast(lost.estresumeworkdate as date) as est_resume_work_dt,
    CAST(lost.ceasedworkdate as TIMESTAMP_NTZ) as ceased_work_dttm,
    cast(lost.ceasedworkdate as date) as ceased_work_dt,
    CAST(lost.actualresumedworkdate as TIMESTAMP_NTZ) as actual_resumed_work_dttm,
    cast(lost.actualresumedworkdate as date) as actual_resumed_work_dt,
    datediff(day, cast(lost.ceasedworkdate as date), cast(lost.actualresumedworkdate as date)) as number_days_off_work,
    null as original_ceased_work_dt,
    null as last_est_resumed_work_dt,
    null as last_actual_resumed_work_dt,
    null as total_number_days_off_work,
    null as earliest_time_loss_rank,
    'Y' as retired_ind,
    lost.updatetime as src_updated_dttm,
    cast(lost.updatetime as date) as src_updated_dt,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_workcomp wrk
    on clm.claimworkcompid = wrk.id

inner join ccx_losttimerecord_icare lost
    on lost.claimworkcompid = wrk.id
    and lost.retired > 0
)
select  
        claim_sk,
        src_system_cd,
        claim_nbr,
        CAST(src_create_dttm AS TIMESTAMP_NTZ) AS src_create_dttm,
        src_lost_id,
        src_create_dt,
        CAST(est_resume_work_dttm AS TIMESTAMP_NTZ) AS est_resume_work_dttm,
        est_resume_work_dt,
        CAST(ceased_work_dttm AS TIMESTAMP_NTZ) AS ceased_work_dttm,
        ceased_work_dt,
        CAST(actual_resumed_work_dttm AS TIMESTAMP_NTZ) as actual_resumed_work_dttm,
        actual_resumed_work_dt,
        number_days_off_work,
        original_ceased_work_dt,
        last_est_resumed_work_dt,
        last_actual_resumed_work_dt,
        total_number_days_off_work,
        earliest_time_loss_rank,
        retired_ind,
        CAST(src_updated_dttm AS TIMESTAMP_NTZ) as src_updated_dttm,
        src_updated_dt,
        file_ingestion_timestamp
from
    cte_join
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-04-20      1.0                             Converted to incremental with merge strategy

-#}

{{
  config(
    materialized='incremental',
    unique_key=['src_claim_id', 'latest_certificate_of_capacity_record_rank'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 04_CERTIFICATE_OF_CAPACITY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A04
  TBL_NM: MSC_QLK_ASPIRE_CERTIFICATE_OF_CAPACITY
-#}

with cc_claim as (
    select
        id,
        claimnumber,
        claimworkcompid,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    {% if is_incremental() %}
        WHERE  file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

ccx_employmentcapacity_icare as (
    select
        id,
        claimworkcompid,
        fitness,
        startdate,
        enddate,
        totalhoursperweek,
        createtime,
        retired,
        cocstatus
    from {{ ref('v_ccx_employmentcapacity_icare_current') }}
),

cctl_fitness_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_fitness_icare_current') }}
),

cctl_cocstatus_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_cocstatus_icare_current') }}
),

cte_join as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
        clm.source_system,
        clm.id as src_claim_id,
        clm.claimnumber as claim_nbr,
        dimfit.typecode as fitness_cd,
        dimfit.name as fitness_desc,
        cast(empcap.startdate as timestamp_ntz) as certificate_of_capacity_start_dt,
        cast(empcap.enddate as timestamp_ntz) as certificate_of_capacity_end_dt,
        empcap.totalhoursperweek as certificate_of_capacity_hours_worked_per_week,
        cast(empcap.createtime as timestamp_ntz) as src_create_dttm,
        clm.file_ingestion_timestamp,
        row_number() over (
            partition by clm.id
            order by
                empcap.retired asc,
                empcap.startdate desc,
                coalesce (empcap.enddate, '9999-12-31 00:00:00.000') desc,
                empcap.createtime desc
        ) as latest_certificate_of_capacity_record_rank,
        case
            when empcap.retired = 0 then 'N'
            else 'Y'
        end as retired_ind,
        case
            when empcap.cocstatus is null then ''
            else sta.name
        end as certificate_of_capacity_status
    from cc_claim as clm
    inner join ccx_employmentcapacity_icare as empcap
        on clm.claimworkcompid = empcap.claimworkcompid
    inner join cctl_fitness_icare as dimfit
        on empcap.fitness = dimfit.id
    left join cctl_cocstatus_icare as sta
        on empcap.cocstatus = sta.id
    where empcap.cocstatus is null or sta.typecode = 'valid'
)

select
    claim_sk,
    source_system,
    src_claim_id,
    claim_nbr,
    fitness_cd,
    fitness_desc,
    certificate_of_capacity_start_dt,
    certificate_of_capacity_end_dt,
    certificate_of_capacity_hours_worked_per_week,
    src_create_dttm,
    latest_certificate_of_capacity_record_rank,
    retired_ind,
    certificate_of_capacity_status,
    file_ingestion_timestamp
from
    cte_join

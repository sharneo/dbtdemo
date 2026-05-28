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
)

select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    dimfit.typecode as fitness_cd,
    dimfit.name as fitness_desc,
    empcap.startdate as certificate_of_capacity_start_dt,
    empcap.enddate as certificate_of_capacity_end_dt,
    empcap.totalhoursperweek as certificate_of_capacity_hours_worked_per_week,
    empcap.createtime as src_create_dttm,
    row_number() over (
        partition by clm.id
        order by
            empcap.retired,
            empcap.startdate desc,
            case
                when empcap.enddate is null then '9999-12-31 00:00:00.000'
                else empcap.enddate
            end desc,
            empcap.createtime desc
    ) as latest_certificate_of_capacity_record_rank,
    case
        when empcap.retired = 0 then 'N'
        else 'Y'
    end as retired_ind,
    case
        when empcap.cocstatus is null then ''
        else sta.name
    end as certificate_of_capacity_status,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join ccx_employmentcapacity_icare empcap
    on clm.claimworkcompid = empcap.claimworkcompid

inner join cctl_fitness_icare dimfit
    on empcap.fitness = dimfit.id

left join cctl_cocstatus_icare sta
    on sta.id = empcap.cocstatus

where empcap.cocstatus is null or sta.typecode = 'valid'

{% if is_incremental() %}
    and clm.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}

{{
  config(
    materialized='incremental',
    unique_key='src_claim_id'
  )
}}

with

cc_claim as (
    select
          id
        , claimnumber
        , claimworkcompid
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

ccx_employmentcapacity_icare as (
    select
          claimworkcompid
        , fitness
        , startdate
        , enddate
        , totalhoursperweek
        , createtime
        , retired
        , cocstatus
        , file_ingestion_timestamp
    from {{ ref('v_ccx_employmentcapacity_icare_current') }}
),

cctl_fitness_icare as (
    select
          id
        , typecode
        , name
    from {{ ref('v_cctl_fitness_icare_current') }}
),

cctl_cocstatus_icare as (
    select
          id
        , typecode
        , name
    from {{ ref('v_cctl_cocstatus_icare_current') }}
),

final as (
    select
          md5(concat('GWCC', clm.claimnumber)) as claim_sk
        , 'GWCC' as src_system_cd
        , clm.id as src_claim_id
        , clm.claimnumber as claim_nbr
        , dim_fit.typecode as fitness_cd
        , dim_fit.name as fitness_desc
        , emp_cap.startdate as certificate_of_capacity_start_dt
        , emp_cap.enddate as certificate_of_capacity_end_dt
        , emp_cap.totalhoursperweek as certificate_of_capacity_hours_worked_per_week
        , emp_cap.createtime as src_create_dttm
        , row_number() over (
            partition by clm.id 
            order by 
                emp_cap.retired, 
                emp_cap.startdate desc, 
                coalesce(emp_cap.enddate, cast('9999-12-31' as timestamp)) desc, 
                emp_cap.createtime desc
          ) as latest_certificate_of_capacity_record_rank
        , case when emp_cap.retired = 0 then 'N' else 'Y' end as retired_ind
        , case when emp_cap.cocstatus is null then '' else sta.name end as certificate_of_capacity_status
        , emp_cap.file_ingestion_timestamp
        , current_date() as extract_date

    from cc_claim clm

    join ccx_employmentcapacity_icare emp_cap
        on clm.claimworkcompid = emp_cap.claimworkcompid

    join cctl_fitness_icare dim_fit
        on emp_cap.fitness = dim_fit.id

    left join cctl_cocstatus_icare sta
        on emp_cap.cocstatus = sta.id

    where emp_cap.cocstatus is null or sta.typecode = 'valid'
)

select * from final

{% if is_incremental() %}
  where file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}

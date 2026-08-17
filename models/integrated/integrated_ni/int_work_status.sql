{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for work status.
                                                claim_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_work_status_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 31_WORK_STATUS.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A31
  TBL_NM: MSC_QLK_ASPIRE_WORK_STATUS
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
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_claimempdata as (
    select
        ownerid,
        foreignentityid
    from {{ ref('v_cc_claimempdata_current') }}
),

cc_employmentdata as (
    select
        id,
        hoursworkedweek_icare,
        numdaysworked,
        wageamount,
        payperiod,
        firstdaypaycycle_icare,
        paymentsaligntopaycycle_icare,
        terminationdate_icare,
        terminationreason_icare,
        industrycode_icareid
    from {{ ref('v_cc_employmentdata_current') }}
    where retired = 0
),

cc_workstatus as (
    select
        id,
        publicid,
        employmentdataid,
        comments,
        statusdate,
        statusenddate,
        status,
        wageloss_icare,
        createtime,
        updatetime,
        retired
    from {{ ref('v_cc_workstatus_current') }}
),

cctl_workcapacity as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_workcapacity_current') }}
),

cctl_employmnttermreason_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_employmnttermreason_icare_current') }}
),

ccx_industrycode_icare as (
    select
        id,
        industrycode,
        groupdesc,
        industrydesc
    from {{ ref('v_ccx_industrycode_icare_current') }}
    where retired = 0
),

cctl_payperiodtype as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_payperiodtype_current') }}
),

cctl_weekdays as (
    select
        id,
        name
    from {{ ref('v_cctl_weekdays_current') }}
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as src_system_cd,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    wrk.publicid as work_status_id,
    wrk.id as src_work_status_id,
    wrk.comments as work_status_comments_txt,
    emp.hoursworkedweek_icare as hours_worked_per_wk_nbr,
    emp.numdaysworked as days_worked_per_wk_nbr,
    CAST(wrk.statusdate as TIMESTAMP_NTZ) AS work_status_start_dt,
    CAST(wrk.statusenddate AS TIMESTAMP_NTZ) AS  work_status_end_dt,
    case
        when wrk.wageloss_icare = 1 then 'Y'
        else 'N'
    end as wage_loss_ind,
    wrkcap.typecode as work_status_cd,
    wrkcap.name as work_status_desc,
    emp.wageamount as avg_wkly_wage_at_lodgement,
    dim_payperiod.typecode as pay_period_cd,
    dim_payperiod.name as pay_period_desc,
    dim_weekdays.name as first_day_of_pay_cycle,
    case
        when emp.paymentsaligntopaycycle_icare is null then null
        when emp.paymentsaligntopaycycle_icare = 1 then 'Y'
        else 'N'
    end as payments_align_to_paycycle_ind,
    row_number() over (
        partition by clm.id
        order by (case when wrk.retired = 0 then 0 else 1 end), wrk.statusdate desc
    ) as most_recent_work_status_record_rank,
    case
        when wrk.retired > 0 then 'N'
        when wrk.statusdate <= current_date()
            and row_number() over (partition by clm.id order by wrk.retired, wrk.statusdate desc) = 1 then 'Y'
        else 'N'
    end as current_work_status_record_ind,
    case
        when wrkcap.typecode in ('06', '08')
            then datediff(day, cast(wrk.statusdate as date), coalesce(cast(wrk.statusenddate as date), current_date()))
        when wrkcap.typecode in ('02', '04') and wrk.wageloss_icare = 1
            then datediff(day, cast(wrk.statusdate as date), coalesce(cast(wrk.statusenddate as date), current_date()))
        else 0
    end as forecasted_daily_payments,
    cast(emp.terminationdate_icare as date) as termination_dt,
    dimtermreason.typecode as termination_reason_cd,
    dimtermreason.name as termination_reason_desc,
    CAST(wrk.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    CAST(wrk.updatetime as  TIMESTAMP_NTZ) as src_update_dttm,
    empindustry.industrycode as anzsic,
    empindustry.groupdesc as anzsic_group_desc,
    empindustry.industrydesc as anzsic_industry_desc,
    case
        when wrk.retired = 0 then 'N'
        else 'Y'
    end as retired_ind,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_claimempdata clmemp
    on clm.id = clmemp.ownerid

inner join cc_employmentdata emp
    on clmemp.foreignentityid = emp.id

inner join cc_workstatus wrk
    on emp.id = wrk.employmentdataid

left join cctl_workcapacity wrkcap
    on wrk.status = wrkcap.id

left join cctl_employmnttermreason_icare dimtermreason
    on emp.terminationreason_icare = dimtermreason.id

left join ccx_industrycode_icare empindustry
    on emp.industrycode_icareid = empindustry.id

left join cctl_payperiodtype dim_payperiod
    on emp.payperiod = dim_payperiod.id

left join cctl_weekdays dim_weekdays
    on emp.firstdaypaycycle_icare = dim_weekdays.id
)
select 
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_claim_id,
    work_status_id,
    src_work_status_id,
    work_status_comments_txt,
    hours_worked_per_wk_nbr,
    days_worked_per_wk_nbr,
    work_status_start_dt,
    work_status_end_dt,
    wage_loss_ind,
    work_status_cd,
    work_status_desc,
    avg_wkly_wage_at_lodgement,
    pay_period_cd,
    pay_period_desc,
    first_day_of_pay_cycle,
    payments_align_to_paycycle_ind,
    most_recent_work_status_record_rank,
    current_work_status_record_ind,
    forecasted_daily_payments,
    termination_dt,
    termination_reason_cd,
    termination_reason_desc,
    src_create_dttm,
    src_update_dttm,
    anzsic,
    anzsic_group_desc,
    anzsic_industry_desc,
    retired_ind,
    file_ingestion_timestamp
from
    cte_join
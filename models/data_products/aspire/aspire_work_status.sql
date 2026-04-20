{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire

-#}   

{{ config(
    materialized='table',
    tags=["aspire","daily","sas","legacy"]
) }}
with base_cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('vw_cc_claim_current') }}
    where retired = 0
),

base_cc_claimempdata as (
    select
        ownerid,
        foreignentityid
    from {{ ref('vw_cc_claimempdata_current') }}
),

base_cc_employmentdata as (
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
    from {{ ref('vw_cc_employmentdata_current') }}
    where retired = 0
),

base_cc_workstatus as (
    select
        id,
        publicid,
        employmentdataid,
        status,
        statusdate,
        statusenddate,
        wageloss_icare,
        comments,
        retired,
        createtime,
        updatetime
    from {{ ref('vw_cc_workstatus_current') }}
),

base_cctl_workcapacity as (
    select id, typecode, name
    from {{ ref('vw_cctl_workcapacity_current') }}
),

base_cctl_employmnttermreason_icare as (
    select id, typecode, name
    from {{ ref('vw_cctl_employmnttermreason_icare_current') }}
),

base_ccx_industrycode_icare as (
    select
        id,
        industrycode,
        groupdesc,
        industrydesc
    from {{ ref('vw_ccx_industrycode_icare_current') }}
    where retired = 0
),

base_cctl_payperiodtype as (
    select id, typecode, name
    from {{ ref('vw_cctl_payperiodtype_current') }}
),

base_cctl_weekdays as (
    select id, name
    from {{ ref('vw_cctl_weekdays_current') }}
)

select
    md5(concat('GWCC', clm.claimnumber))             as claim_sk,
    'GWCC'                                            as src_system_cd,
    clm.claimnumber                                  as claim_nbr,
    clm.id                                           as src_claim_id,
    wrk.publicid                                     as work_status_id,
    wrk.id                                           as src_work_status_id,
    wrk.comments                                     as work_status_comments_txt,
    emp.hoursworkedweek_icare                        as hours_worked_per_wk_nbr,
    emp.numdaysworked                                as days_worked_per_wk_nbr,
    wrk.statusdate                                   as work_status_start_dt,
    wrk.statusenddate                                as work_status_end_dt,
    iff(wrk.wageloss_icare = true, 'Y', 'N')        as wage_loss_ind,
    wrkcap.typecode                                  as work_status_cd,
    wrkcap.name                                      as work_status_desc,
    emp.wageamount                                   as avg_wkly_wage_at_lodgement,
    dim_payperiod.typecode                           as pay_period_cd,
    dim_payperiod.name                               as pay_period_desc,
    dim_weekdays.name                                as first_day_of_pay_cycle,
    case
        when emp.paymentsaligntopaycycle_icare is null then null
        when emp.paymentsaligntopaycycle_icare = true then 'Y'
        else 'N'
    end                                              as payments_align_to_paycycle_ind,
    row_number() over (
        partition by clm.id
        order by
            case when wrk.retired = 0 then 0 else 1 end,
            wrk.statusdate desc
    )                                                as most_recent_work_status_record_rank,
    case
        when wrk.retired > 0 then 'N'
        when wrk.statusdate <= current_date()
            and row_number() over (
                partition by clm.id
                order by wrk.retired, wrk.statusdate desc
            ) = 1
        then 'Y'
        else 'N'
    end                                              as current_work_status_record_ind,
    case
        when wrkcap.typecode in ('06', '08')
            then datediff('day', wrk.statusdate::date, coalesce(wrk.statusenddate::date, current_date()))
        when wrkcap.typecode in ('02', '04') and wrk.wageloss_icare = true
            then datediff('day', wrk.statusdate::date, coalesce(wrk.statusenddate::date, current_date()))
        else 0
    end                                              as forecasted_daily_payments,
    emp.terminationdate_icare::date                  as termination_dt,
    dimtermreason.typecode                           as termination_reason_cd,
    dimtermreason.name                               as termination_reason_desc,
    wrk.createtime                                   as src_create_dttm,
    wrk.updatetime                                   as src_update_dttm,
    empindustry.industrycode                         as anzsic,
    empindustry.groupdesc                            as anzsic_group_desc,
    empindustry.industrydesc                         as anzsic_industry_desc,
    iff(wrk.retired = 0, 'N', 'Y')                  as retired_ind
from base_cc_claim as clm
inner join base_cc_claimempdata as clmemp
    on clmemp.ownerid = clm.id
inner join base_cc_employmentdata as emp
    on emp.id = clmemp.foreignentityid
inner join base_cc_workstatus as wrk
    on wrk.employmentdataid = emp.id
left join base_cctl_workcapacity as wrkcap
    on wrkcap.id = wrk.status
left join base_cctl_employmnttermreason_icare as dimtermreason
    on dimtermreason.id = emp.terminationreason_icare
left join base_ccx_industrycode_icare as empindustry
    on empindustry.id = emp.industrycode_icareid
left join base_cctl_payperiodtype as dim_payperiod
    on dim_payperiod.id = emp.payperiod
left join base_cctl_weekdays as dim_weekdays
    on dim_weekdays.id = emp.firstdaypaycycle_icare
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Certificate of Capacity View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with ccx_employmentcapacity_icare as (
    select
        id,
        claimworkcompid,
        startdate,
        enddate,
        consultationdate,
        createtime,
        fitness
    from {{ ref('v_ccx_employmentcapacity_icare_current') }}
    where retired = 0 and coalesce(cocstatus, 10003) = 10003
),

cctl_fitness_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_fitness_icare_current') }}
),

submission_period as (
    select
        submission_period,
        submission_period_end_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

base as (
    select
        subperiod.submission_period,
        fi.typecode,
        fi.name,
        ec.claimworkcompid,
        ec.startdate,
        ec.enddate,
        ec.id
    from ccx_employmentcapacity_icare as ec
    left join cctl_fitness_icare as fi on fi.id = ec.fitness
    inner join submission_period as subperiod
        on ec.startdate <= subperiod.submission_period_end_dt
        and coalesce(ec.consultationdate, subperiod.submission_period_end_dt) <= subperiod.submission_period_end_dt
)

select
    submission_period,
    typecode,
    name,
    claimworkcompid,
    startdate,
    enddate,
    id
from base

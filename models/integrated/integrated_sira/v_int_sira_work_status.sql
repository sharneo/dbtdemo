{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Work Status View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_workstatus as (
    select
        employmentdataid,
        statusdate,
        statusenddate,
        status
    from {{ ref('v_cc_workstatus_current') }}
    where retired = 0
),

cctl_workcapacity as (
    select
        id,
        typecode
    from {{ ref('v_cctl_workcapacity_current') }}
),

cc_claimempdata as (
    select
        foreignentityid,
        ownerid
    from {{ ref('v_cc_claimempdata_current') }}
),

cc_claim as (
    select
        id,
        claimnumber
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

submission_period as (
    select
        submission_period,
        submission_period_end_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

work_status_base as (
    select
        subperiod.submission_period,
        ws.employmentdataid,
        ws.statusdate,
        coalesce(ws.statusenddate, '9999-01-01 00:00:00.0000000') as statusenddate,
        wc.typecode,
        ws.status,
        claim.claimnumber
    from cc_workstatus as ws
    left join cctl_workcapacity as wc on wc.id = ws.status
    left join cc_claimempdata as empdata on empdata.foreignentityid = ws.employmentdataid
    left join cc_claim as claim on claim.id = empdata.ownerid
    inner join submission_period as subperiod
        on left(to_char(ws.statusdate, 'YYYYMMDD'), 6) <= subperiod.submission_period
),

work_status_delta as (
    select
        submission_period,
        employmentdataid,
        statusdate,
        statusenddate,
        typecode,
        status,
        claimnumber,
        row_number() over (partition by employmentdataid order by statusdate)
            - row_number() over (partition by employmentdataid, typecode order by statusdate) as delta
    from work_status_base
),

work_status_grouped as (
    select
        submission_period,
        employmentdataid,
        min(statusdate) as statusdate,
        max(statusenddate) as statusenddate,
        typecode,
        status,
        claimnumber,
        delta
    from work_status_delta
    group by
        submission_period,
        employmentdataid,
        typecode,
        status,
        claimnumber,
        delta
)

select
    submission_period,
    employmentdataid,
    statusdate,
    statusenddate,
    typecode,
    status,
    claimnumber,
    rank() over (partition by employmentdataid order by statusdate desc, statusenddate asc) as ranking
from work_status_grouped

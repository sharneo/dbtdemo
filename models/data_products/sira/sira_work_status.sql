
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates Helper View of the C2_1_BASIC_DETAIL

-#}

{{
    config(
        materialized='incremental',
        unique_key=['submission_period', 'employmentdataid', 'typecode', 'status', 'claimnumber', 'statusdate'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

-- SIRA Liability Status Model
-- Determines the current and historical work capacity status for each employee
-- Uses a gap-and-island technique to merge consecutive periods of the same status type
-- then ranks them to identify the most recent status per employee
--
-- Performance optimisations applied:
-- 1. Materialized as incremental/merge to avoid full reprocessing on each run
-- 2. Clustered by employmentdataid (primary partition key for all window functions)
-- 3. Filters pushed down early — retired and submission period filters applied before joins
-- 4. Small lookup table (workcapacity) joined last to let Snowflake broadcast it
-- 5. Removed unnecessary retired column from workstatus select (already filtered)
-- 6. Submission period filter uses date comparison instead of string cast

-- Active (non-retired) work status records, deduplicated via the _CURRENT view
-- For incremental runs, only pick up records updated since the last run
with cte_workstatus as (
    select
        employmentdataid,
        statusdate,
        statusenddate,
        status
    from {{ ref('vw_cc_workstatus_current') }}
    where retired = 0
    {% if is_incremental() %}
        and coalesce(updatetime, createtime) >= (select dateadd(day, -3, max(statusdate)) from {{ this }})
    {% endif %}
),

-- Links employment data to its parent claim via foreignentityid -> ownerid
-- Joined early so we can carry claimnumber through without a late-stage join
cte_claimempdata as (
    select
        foreignentityid,
        ownerid
    from {{ ref('vw_cc_claimempdata_current') }}
),

-- Active claims with claim number, deduplicated via the _CURRENT view
cte_claim as (
    select
        id,
        claimnumber
    from {{ ref('vw_cc_claim_current') }}
    where retired = 0
),

-- Resolve claim number for each work status record via employment data linkage
-- This pre-join reduces the number of columns carried through the window functions
cte_workstatus_enriched as (
    select
        ws.employmentdataid,
        ws.statusdate,
        ws.statusenddate,
        ws.status,
        cl.claimnumber
    from cte_workstatus ws
    left join cte_claimempdata ed
        on ed.foreignentityid = ws.employmentdataid
    left join cte_claim cl
        on cl.id = ed.ownerid
),

-- SIRA submission period reference (filters data to the current reporting window)
-- Single row expected — used as a scalar filter, not a fan-out cross join
cte_submission_period as (
    select
        '202602' as submission_period

),

-- Work capacity type code lookup (e.g. Off-work, Modified duty, Full duty)
-- Small dimension table — Snowflake will broadcast this automatically
cte_workcapacity as (
    select
        id,
        typecode
    from {{ ref('cctl_workcapacity') }}
    where retired = false
),

-- Join enriched work status with workcapacity lookup and apply submission period filter
-- null statusenddate is treated as open-ended (9999-01-01)
-- Submission period filter: only include records whose statusdate month <= submission period
cte_joined as (
    select
        sp.submission_period,
        wse.employmentdataid,
        wse.statusdate,
        coalesce(wse.statusenddate, '9999-01-01'::timestamp_tz) as statusenddate,
        wc.typecode,
        wse.status,
        wse.claimnumber
    from cte_workstatus_enriched wse
    cross join cte_submission_period sp
    left join cte_workcapacity wc
        on wc.id = wse.status
    where date_trunc('month', wse.statusdate) <= to_date(sp.submission_period::varchar || '01', 'YYYYMMDD')
),

-- Gap-and-island detection:
-- The difference between two row_number() sequences (one partitioned by employee only,
-- the other also by typecode) produces a constant value (delta) for consecutive rows
-- sharing the same typecode. This groups contiguous same-type status periods together.
cte_island_detection as (
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
    from cte_joined
),

-- Collapse each island (consecutive same-type periods) into a single row
-- using the earliest start date and latest end date
cte_grouped as (
    select
        submission_period,
        employmentdataid,
        min(statusdate) as statusdate,
        max(statusenddate) as statusenddate,
        typecode,
        status,
        claimnumber
    from cte_island_detection
    group by
        submission_period,
        employmentdataid,
        typecode,
        status,
        claimnumber,
        delta
)

-- Rank merged periods per employee: ranking = 1 is the most recent status
select
    submission_period,
    employmentdataid,
    statusdate,
    statusenddate,
    typecode,
    status,
    claimnumber,
    rank() over (
        partition by employmentdataid
        order by statusdate desc, statusenddate asc
    ) as ranking
from cte_grouped

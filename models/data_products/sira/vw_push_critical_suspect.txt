{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates Helper View of the VW_PUSH_CRITICAL_SUSPECT

-#}

{{ config(
    materialized='view',
    tags=["sira", "business_critical","hlper_views"]
) }}

with sira_error_submission_control as (
    select
        '70' as id,
        'CLM402' as report_id,
        '2019-08-01' as submission_start_dttm,
        '133113' as submission_no,
        '201908' as submission_period
),

sira_clm402_cum as (
    select
        '2001097701' as claim_no,
        '10000059' as control_id,
        '' as error_number,
        'CRITICAL' as error_severity
),

sira_clm403_cum as (
    select
        '2001097701' as claim_no,
        '10000059' as control_id,
        '' as error_number,
        'CRITICAL' as error_severity
),

sira_clm404_cum as (
    select
        '2001097701' as claim_no,
        '10000059' as control_id,
        '' as error_number,
        'CRITICAL' as error_severity
),

ranked_submissions as (
    select
        id,
        submission_period,
        report_id,
        row_number()
            over (partition by report_id, submission_start_dttm order by report_id desc, submission_no desc)
            as rank_reportid
    from sira_error_submission_control
),

all_errors as (
    select
        claim_no,
        control_id,
        error_number,
        error_severity
    from sira_clm402_cum
    union all
    select
        claim_no,
        control_id,
        error_number,
        error_severity
    from sira_clm403_cum
    union all
    select
        claim_no,
        control_id,
        error_number,
        error_severity
    from sira_clm404_cum
),

final as (
    select
        a.submission_period,
        a.rank_reportid,
        a.report_id,
        errs.claim_no as claim_number,
        errs.control_id,
        errs.error_number,
        errs.error_severity
    from ranked_submissions as a
    left join all_errors as errs on a.id = errs.control_id
    where a.rank_reportid = 1
)

select
    submission_period,
    rank_reportid,
    report_id,
    claim_number,
    control_id,
    error_number,
    error_severity
from final

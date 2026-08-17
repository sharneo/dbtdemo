{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Latest Review View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with submission_period as (
    select
        submission_period,
        submission_period_end_dt
    from {{ ref('v_sira_submission_period_current') }}
    where current_submission_flag = 'Y'
),

estimates_payments as (
    select
        claim_sk,
        createtime
    from {{ ref('v_int_sira_c_2_6_estimates_payments') }}
)

select
    ep.claim_sk,
    sp.submission_period,
    max(ep.createtime) as latestreviewdate
from estimates_payments as ep
inner join submission_period as sp on ep.createtime <= sp.submission_period_end_dt
group by
    ep.claim_sk,
    sp.submission_period
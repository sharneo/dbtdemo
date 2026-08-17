{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a PIAWE View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_exposure as (
    select
        id,
        claimid,
        exposuretype
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
),

ccx_piawe_icare as (
    select
        id,
        exposureid,
        piawetype_icare,
        ordinaryearnings_icare,
        totalshiftweek,
        totalovertimeweek,
        piawefirst52_icare,
        piawelater52_icare,
        createtime,
        effectivedate_icare,
        piawedeactivated_icare,
        rpstartdate,
        rpenddate,
        draft
    from {{ ref('v_ccx_piawe_icare_current') }}
    where retired = 0
        and draft = 0
        and piawedeactivated_icare = 0
),

cctl_piawetype_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_piawetype_icare_current') }}
    where retired = 0
),

cctl_exposuretype as (
    select
        id,
        typecode
    from {{ ref('v_cctl_exposuretype_current') }}
    where retired = 0 and typecode = 'LostWages'
),

ccx_benefitsaccrual_icare as (
    select
        exposureid,
        totalweekspaid
    from {{ ref('v_ccx_benefitsaccrual_icare_current') }}
    where retired = 0
),

cc_claim as (
    select
        id,
        claimnumber,
        lossdate
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

submission_period as (
    select
        submission_period_end_dt
    from {{ ref('v_sira_submission_period_current') }}
),

base as (
    select
        c.claimnumber,
        e.claimid,
        et.typecode,
        ba.totalweekspaid,
        case
            when cast(c.lossdate as date) <= '2018-10-25'
                then case
                    when t.name in ('Calculated PIAWE', 'Manual PIAWE', 'Migrated PIAWE')
                        then coalesce(p.ordinaryearnings_icare, 0) + coalesce(p.totalshiftweek, 0) + coalesce(p.totalovertimeweek, 0)
                    when t.name in ('Interim PIAWE', 'Transitional Rate')
                        then p.piawefirst52_icare
                    when t.name = 'Indexed PIAWE'
                        then case when ba.totalweekspaid <= 52 then p.piawefirst52_icare else p.piawelater52_icare end
                    when t.name in ('WCC PIAWE', 'Agreement PIAWE')
                        then 0
                end
            when cast(c.lossdate as date) between '2018-10-26' and '2019-10-20'
                then case
                    when t.name in ('Calculated PIAWE', 'Manual PIAWE', 'Migrated PIAWE')
                        then coalesce(p.ordinaryearnings_icare, 0) + coalesce(p.totalshiftweek, 0) + coalesce(p.totalovertimeweek, 0)
                    when t.name in ('Interim PIAWE', 'Transitional Rate', 'Indexed PIAWE')
                        then p.piawefirst52_icare
                    when t.name in ('WCC PIAWE', 'Agreement PIAWE')
                        then 0
                end
            when cast(c.lossdate as date) >= '2019-10-21'
                then p.piawefirst52_icare
        end as pre_injuiry_average_weekly,
        case
            when cast(c.lossdate as date) <= '2018-10-25'
                then case
                    when t.name in ('Calculated PIAWE', 'Manual PIAWE', 'Migrated PIAWE')
                        then p.ordinaryearnings_icare
                    when t.name in ('Interim PIAWE', 'Transitional Rate')
                        then p.piawefirst52_icare
                    when t.name = 'Indexed PIAWE'
                        then case when ba.totalweekspaid <= 52 then p.piawefirst52_icare else p.piawelater52_icare end
                    when t.name in ('WCC PIAWE', 'Agreement PIAWE')
                        then 0
                end
            when cast(c.lossdate as date) between '2018-10-26' and '2019-10-20'
                then case
                    when t.name in ('Calculated PIAWE', 'Manual PIAWE', 'Migrated PIAWE')
                        then p.ordinaryearnings_icare
                    when t.name in ('Interim PIAWE', 'Transitional Rate', 'Indexed PIAWE')
                        then p.piawefirst52_icare
                    when t.name in ('WCC PIAWE', 'Agreement PIAWE')
                        then 0
                end
            when cast(c.lossdate as date) >= '2019-10-21'
                then 0
        end as ordinary_earnings,
        case
            when cast(c.lossdate as date) <= '2018-10-25'
                or cast(c.lossdate as date) between '2018-10-26' and '2019-10-20'
                then case
                    when t.name in ('Calculated PIAWE', 'Indexed PIAWE', 'Manual PIAWE', 'Migrated PIAWE')
                        then p.totalshiftweek
                    when t.name in ('WCC PIAWE', 'Agreement PIAWE', 'Interim PIAWE', 'Transitional Rate')
                        then 0
                end
            when cast(c.lossdate as date) >= '2019-10-21'
                then 0
        end as shift_allowance,
        case
            when cast(c.lossdate as date) <= '2018-10-25'
                or cast(c.lossdate as date) between '2018-10-26' and '2019-10-20'
                then case
                    when t.name in ('Calculated PIAWE', 'Indexed PIAWE', 'Manual PIAWE', 'Migrated PIAWE')
                        then p.totalovertimeweek
                    when t.name in ('WCC PIAWE', 'Agreement PIAWE', 'Interim PIAWE', 'Transitional Rate')
                        then 0
                end
            when cast(c.lossdate as date) >= '2019-10-21'
                then 0
        end as overtime,
        p.createtime,
        p.effectivedate_icare,
        p.piawedeactivated_icare,
        p.rpstartdate,
        p.rpenddate,
        p.draft,
        p.exposureid
    from cc_exposure as e
    left join ccx_piawe_icare as p on e.id = p.exposureid
    left join cctl_piawetype_icare as t on p.piawetype_icare = t.id
    left join cctl_exposuretype as et on et.id = e.exposuretype
    left join ccx_benefitsaccrual_icare as ba on ba.exposureid = e.id
    left join cc_claim as c on c.id = e.claimid
    inner join submission_period as sp on p.effectivedate_icare <= sp.submission_period_end_dt
    where et.typecode = 'LostWages'
)

select
    claimnumber,
    claimid,
    typecode,
    totalweekspaid,
    pre_injuiry_average_weekly,
    ordinary_earnings,
    shift_allowance,
    overtime,
    createtime,
    effectivedate_icare,
    piawedeactivated_icare,
    rpstartdate,
    rpenddate,
    draft,
    exposureid
from base

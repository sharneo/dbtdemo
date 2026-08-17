{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Time Lost View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with ccx_losttimerecord_icare as (
    select
        id,
        claimworkcompid,
        ceasedworkdate,
        estresumeworkdate,
        actualresumedworkdate,
        retired,
        createtime,
        updatetime
    from {{ ref('v_ccx_losttimerecord_icare_current') }}
),

cc_workcomp as (
    select
        id,
        timelossreport
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

cc_claim as (
    select
        id,
        claim_sk,
        claimnumber,
        claimworkcompid,
        lodgingagent_icare,
        claimsagent_icare,
        lossdate
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cctl_claimagent_icare_lag as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimagent_icare_current') }}
),

cctl_claimagent_icare_mag as (
    select
        id,
        typecode
    from {{ ref('v_cctl_claimagent_icare_current') }}
),

cc_incident_deceased as (
    select
        claimid,
        deceaseddate_icare,
        updatetime
    from {{ ref('v_cc_incident_current') }}
    where retired = 0
        and claimincident = 1
        and deceaseddate_icare is not null
),

branch1 as (
    select
        cc.claim_sk,
        cc.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        cc.claimnumber || '^' || 'GWCC' as claimbk,
        cc.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        cast(coalesce(mag_agent.typecode, lag_agent.typecode) as varchar(5)) as agentcode,
        coalesce(cw.timelossreport, 1) as timelossreport,
        cast(cli.ceasedworkdate as date) as ceasedworkdate,
        cast(cli.estresumeworkdate as date) as estresumeworkdate,
        cast(cli.actualresumedworkdate as date) as actualresumedworkdate,
        cast(i.deceaseddate_icare as date) as deceaseddate_icare,
        cast(cc.lossdate as date) as lossdate,
        cli.retired,
        cli.createtime,
        cli.updatetime,
        cli.id as lost_time_id
    from ccx_losttimerecord_icare as cli
    inner join cc_workcomp as cw on cli.claimworkcompid = cw.id
    inner join cc_claim as cc on cc.claimworkcompid = cw.id
    left join cctl_claimagent_icare_lag as lag_agent on cc.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on cc.claimsagent_icare = mag_agent.id
    left join cc_incident_deceased as i
        on i.claimid = cc.id
        and cli.updatetime >= i.updatetime
),

branch2 as (
    select
        cc.claim_sk,
        cc.claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        cc.claimnumber || '^' || 'GWCC' as claimbk,
        cc.claimnumber || coalesce(lag_agent.typecode, '701') as siraclaimnumber,
        cast(coalesce(mag_agent.typecode, lag_agent.typecode) as varchar(5)) as agentcode,
        coalesce(cw.timelossreport, 1) as timelossreport,
        cast(cli.ceasedworkdate as date) as ceasedworkdate,
        cast(cli.estresumeworkdate as date) as estresumeworkdate,
        cast(cli.actualresumedworkdate as date) as actualresumedworkdate,
        cast(i.deceaseddate_icare as date) as deceaseddate_icare,
        cast(cc.lossdate as date) as lossdate,
        cli.retired,
        cli.createtime,
        i.updatetime,
        cli.id as lost_time_id
    from cc_incident_deceased as i
    inner join cc_claim as cc on cc.id = i.claimid
    left join cctl_claimagent_icare_lag as lag_agent on cc.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on cc.claimsagent_icare = mag_agent.id
    inner join cc_workcomp as cw on cc.claimworkcompid = cw.id
    inner join ccx_losttimerecord_icare as cli
        on cli.claimworkcompid = cw.id
        and i.updatetime >= cli.updatetime
),

combined as (
    select * from branch1
    union
    select * from branch2
),

base as (
    select
        claim_sk,
        claimnumber,
        srcsystemcd,
        claimbk,
        siraclaimnumber,
        agentcode,
        timelossreport,
        ceasedworkdate,
        estresumeworkdate,
        actualresumedworkdate,
        deceaseddate_icare,
        lossdate,
        retired,
        createtime,
        updatetime,
        lost_time_id,
        row_number() over (
            partition by siraclaimnumber, ceasedworkdate
            order by retired asc, updatetime desc, createtime desc, ceasedworkdate, actualresumedworkdate desc
        ) as rnumbau,
        row_number() over (
            partition by siraclaimnumber
            order by retired asc, updatetime desc, createtime desc, actualresumedworkdate desc
        ) as rnum
    from combined
)

select
    cast(
        ltrim(rtrim(siraclaimnumber)) || '^' ||
        ltrim(rtrim(cast(ceasedworkdate as varchar))) || '^' ||
        ltrim(rtrim(cast(lost_time_id as varchar)))
    as varchar(100)) as tl_hlp_key,
    claim_sk,
    claimnumber,
    srcsystemcd,
    claimbk,
    siraclaimnumber,
    agentcode,
    timelossreport,
    ceasedworkdate,
    estresumeworkdate,
    actualresumedworkdate,
    deceaseddate_icare,
    lossdate,
    retired,
    createtime,
    updatetime,
    lost_time_id,
    rnumbau,
    rnum
from base

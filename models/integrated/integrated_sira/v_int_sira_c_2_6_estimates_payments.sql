{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates an Estimates Payments View for SIRA Reporting  

-#}

{{ config(
    materialized='view',
    tags=['sira', 'business_critical']
) }}


with cc_workstatus as (
    select
        id,
        employmentdataid,
        status,
        statusdate,
        updatetime,
        cast(to_char(statusdate, 'YYYYMM') as integer) as statusdateyyyymm
    from {{ ref('v_cc_workstatus_current') }}
),

cctl_workcapacity as (
    select
        id,
        name,
        typecode as status_cd
    from {{ ref('v_cctl_workcapacity_current') }}
),

cc_employmentdata as (
    select
        id,
        claimid
    from {{ ref('v_cc_employmentdata_current') }}
    where retired = 0
),

cc_claim_ws as (
    select
        id,
        claimnumber
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

work_status as (
    select
        ws.employmentdataid,
        ws.statusdateyyyymm,
        ws.status,
        wc.name,
        wc.status_cd,
        ed.claimid,
        cl.claimnumber,
        ws.updatetime,
        row_number() over (
            partition by ws.statusdateyyyymm, ed.claimid
            order by ws.statusdate desc, ws.updatetime desc, ws.id desc
        ) as rownum
    from cc_workstatus as ws
    inner join cctl_workcapacity as wc on ws.status = wc.id
    inner join cc_employmentdata as ed on ws.employmentdataid = ed.id
    inner join cc_claim_ws as cl on ed.claimid = cl.id
),

cc_transactionlineitem as (
    select
        id,
        transactionid,
        transactionamount,
        retired,
        createtime,
        updatetime,
        cast(to_char(updatetime, 'YYYYMM') as integer) as transactiondateyyyymm
    from {{ ref('v_cc_transactionlineitem_current') }}
    where retired = 0
),

cc_transaction as (
    select
        id,
        claimid,
        costcategory,
        subtype,
        status,
        checkid
    from {{ ref('v_cc_transaction_current') }}
    where retired = 0
),

cctl_costcategory as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_costcategory_current') }}
    where typecode not in ('70', '71', '72', '73', '74', '75', '76', '77')
),

cc_check as (
    select
        id,
        claimid,
        issuedate,
        status,
        retired,
        createtime,
        updatetime,
        cast(to_char(issuedate, 'YYYYMM') as integer) as transactiondateyyyymm
    from {{ ref('v_cc_check_current') }}
    where retired = 0
        and issuedate is not null
),

cctl_transactionstatus as (
    select
        id,
        name
    from {{ ref('v_cctl_transactionstatus_current') }}
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

cc_workcomp as (
    select
        id
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

cc_incident as (
    select
        claimid,
        odgrtwdate_icare,
        odgduration_icare,
        updatetime,
        retired,
        row_number() over (
            partition by claimid
            order by odgrtwdate_icare desc, updatetime desc
        ) as rownum
    from {{ ref('v_cc_incident_current') }}
),

payments as (
    select
        cc_claim.claim_sk,
        cc_claim.claimnumber,
        coalesce(lag_agent.typecode, '701') as agentcode,
        coalesce(mag_agent.typecode, lag_agent.typecode) as mag,
        cat.typecode as costcategorycode,
        cat.name as costcategoryname,
        t.subtype,
        chk.transactiondateyyyymm,
        cast(chk.issuedate as date) as issuedate,
        coalesce(tli.transactionamount, 0) as transactionamount,
        cc_claim.lossdate as startdate,
        cast(ws.status_cd as varchar(2)) as status_cd,
        inc.odgrtwdate_icare,
        inc.odgduration_icare,
        cast(tli.createtime as date) as createtime,
        cast(tli.updatetime as date) as updatetime,
        tli.retired,
        tli.id
    from cc_transactionlineitem as tli
    inner join cc_transaction as t on tli.transactionid = t.id
    inner join cctl_costcategory as cat on t.costcategory = cat.id
    inner join cc_check as chk on t.checkid = chk.id
    inner join cctl_transactionstatus as sts on chk.status = sts.id
    inner join cc_claim on chk.claimid = cc_claim.id
    left join cctl_claimagent_icare_lag as lag_agent on cc_claim.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on cc_claim.claimsagent_icare = mag_agent.id
    left join cc_workcomp as wc on wc.id = cc_claim.claimworkcompid
    left join work_status as ws on ws.claimid = cc_claim.id and ws.statusdateyyyymm = tli.transactiondateyyyymm and ws.rownum = 1
    left join cc_incident as inc on inc.claimid = cc_claim.id and inc.rownum = 1 and inc.retired = 0
    where upper(sts.name) in ('ISSUED', 'CLEARED')
),

payment_reserves as (
    select
        cc_claim.claim_sk,
        cc_claim.claimnumber,
        coalesce(lag_agent.typecode, '701') as agentcode,
        coalesce(mag_agent.typecode, lag_agent.typecode) as mag,
        cat.typecode as costcategorycode,
        cat.name as costcategoryname,
        t.subtype,
        tli.transactiondateyyyymm,
        coalesce(tli.transactionamount, 0) as transactionamount,
        cc_claim.lossdate as startdate,
        cast(ws.status_cd as varchar(2)) as status_cd,
        inc.odgrtwdate_icare,
        inc.odgduration_icare,
        cast(tli.createtime as date) as createtime,
        cast(tli.updatetime as date) as updatetime,
        tli.retired,
        tli.id
    from cc_transactionlineitem as tli
    inner join cc_transaction as t on tli.transactionid = t.id
    inner join cctl_costcategory as cat on t.costcategory = cat.id
    inner join cc_claim on t.claimid = cc_claim.id
    inner join cctl_transactionstatus as sts on t.status = sts.id
    left join cctl_claimagent_icare_lag as lag_agent on cc_claim.lodgingagent_icare = lag_agent.id
    left join cctl_claimagent_icare_mag as mag_agent on cc_claim.claimsagent_icare = mag_agent.id
    left join cc_workcomp as wc on wc.id = cc_claim.claimworkcompid
    left join work_status as ws on ws.claimid = cc_claim.id and ws.statusdateyyyymm = tli.transactiondateyyyymm and ws.rownum = 1
    left join cc_incident as inc on inc.claimid = cc_claim.id and inc.rownum = 1 and inc.retired = 0
    where t.subtype = '2'
        and upper(sts.name) = 'SUBMITTED'
),

combined as (
    select
        claim_sk,
        claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        claimnumber || '^' || 'GWCC' as claimbk,
        claimnumber || agentcode as siraclaimnumber,
        mag as agentcode,
        costcategorycode,
        costcategoryname,
        cast(subtype as varchar(10)) as transactionsubtype,
        cast('RESERVES' as varchar(100)) as transactiontype,
        transactiondateyyyymm,
        updatetime as transactiondate,
        cast(transactionamount as decimal(20, 2)) as transactionamount,
        startdate,
        status_cd,
        odgrtwdate_icare,
        odgduration_icare,
        createtime,
        updatetime,
        retired,
        id
    from payment_reserves

    union all

    select
        claim_sk,
        claimnumber,
        cast('GWCC' as varchar(10)) as srcsystemcd,
        claimnumber || '^' || 'GWCC' as claimbk,
        claimnumber || agentcode as siraclaimnumber,
        mag as agentcode,
        costcategorycode,
        costcategoryname,
        cast(subtype as varchar(10)) as transactionsubtype,
        cast('PAYMENTS' as varchar(100)) as transactiontype,
        transactiondateyyyymm,
        issuedate as transactiondate,
        cast(transactionamount as decimal(20, 2)) as transactionamount,
        startdate,
        status_cd,
        odgrtwdate_icare,
        odgduration_icare,
        createtime,
        updatetime,
        retired,
        id
    from payments
),

base as (
    select
        cast(
            ltrim(rtrim(siraclaimnumber)) || '^' ||
            ltrim(rtrim(costcategorycode)) || '^' ||
            ltrim(rtrim(transactionsubtype)) || '^' ||
            ltrim(rtrim(cast(transactiondate as varchar))) || '^' ||
            ltrim(rtrim(cast(id as varchar)))
        as varchar(100)) as est_hlp_key,
        claim_sk,
        claimnumber,
        srcsystemcd,
        claimbk,
        siraclaimnumber,
        agentcode,
        costcategorycode,
        costcategoryname,
        transactionsubtype,
        transactiontype,
        transactiondateyyyymm,
        transactiondate,
        transactionamount,
        startdate,
        cast(status_cd as varchar(2)) as status_cd,
        odgrtwdate_icare,
        odgduration_icare,
        createtime,
        updatetime,
        retired,
        id
    from combined
)

select
    est_hlp_key,
    claim_sk,
    claimnumber,
    srcsystemcd,
    claimbk,
    siraclaimnumber,
    agentcode,
    costcategorycode,
    costcategoryname,
    transactionsubtype,
    transactiontype,
    transactiondateyyyymm,
    transactiondate,
    sum(transactionamount) as transactionamount,
    startdate,
    status_cd,
    odgrtwdate_icare,
    odgduration_icare,
    createtime,
    updatetime,
    retired,
    id
from base
group by
    est_hlp_key,
    claim_sk,
    claimnumber,
    srcsystemcd,
    claimbk,
    siraclaimnumber,
    agentcode,
    costcategorycode,
    costcategoryname,
    transactionsubtype,
    transactiontype,
    transactiondateyyyymm,
    transactiondate,
    startdate,
    status_cd,
    odgrtwdate_icare,
    odgduration_icare,
    createtime,
    updatetime,
    retired,
    id

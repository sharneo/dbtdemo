{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for subrogation party.
-#}   

{{
  config(
    materialized='incremental',
    unique_key='src_adverse_party_id',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 44_SUBROGATION_PARTY.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A44
  TBL_NM: MSC_QLK_ASPIRE_SUBROGATION_PARTY
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
        and file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_subrogationsummary as (
    select
        id,
        publicid,
        claimid
    from {{ ref('v_cc_subrogationsummary_current') }}
    where retired = 0
),

cc_subroadverseparty as (
    select
        id,
        subrogationsummaryid,
        adversepartyid,
        recoverytype_icare,
        outcome_icare,
        subrogationstatus_icare,
        strategy,
        fault,
        expectedrecovery,
        expectedrecoveryamount_icare,
        courtawardedinterest_icare,
        closingcomment_icare,
        closedate_icare,
        activityworflowdate_icare,
        duedate_icare,
        recoverycommenceddate_icare,
        createtime,
        updatetime
    from {{ ref('v_cc_subroadverseparty_current') }}
    where retired = 0
),

cc_contact as (
    select
        id,
        name,
        firstname,
        middlename,
        lastname
    from {{ ref('v_cc_contact_current') }}
),

cctl_recoverytype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_recoverytype_icare_current') }}
    where retired = 0
),

cctl_subroclosedoutcome as (
    select
        id,
        typecode,
        l_en_au
    from {{ ref('v_cctl_subroclosedoutcome_current') }}
    where retired = 0
),

cctl_subrogationstatus as (
    select
        id,
        typecode,
        l_en_au
    from {{ ref('v_cctl_subrogationstatus_current') }}
    where retired = 0
),

cctl_subrostrategy as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_subrostrategy_current') }}
    where retired = 0
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'summ.publicid'
    ]) }} as varchar(150)) as subrogation_sk,
    clm.claimnumber as claim_nbr,
    clm.id as src_claim_id,
    summ.id as src_subrogation_id,
    advprty.id as src_adverse_party_id,
    advprty.adversepartyid as responsible_party_id,
    case
        when ctt.name is not null then ctt.name
        else concat(ctt.firstname, ' ', ctt.middlename, ' ', ctt.lastname)
    end as responsible_party_name,
    sts.typecode as subrogation_status_cd,
    sts.l_en_au as subrogation_status_desc,
    strat.typecode as strategy_cd,
    strat.name as strategy_desc,
    rtyp.typecode as recovery_type_cd,
    rtyp.name as recovery_type_desc,
    otc.typecode as outcome_cd,
    otc.l_en_au as outcome_desc,
    advprty.fault as liability_percent,
    advprty.expectedrecovery as expected_recovery_percent,
    advprty.expectedrecoveryamount_icare as expected_recovery_amt,
    advprty.courtawardedinterest_icare as court_awarded_interest_amt,
    advprty.closingcomment_icare as close_comment,
    cast(advprty.closedate_icare as date) as close_dt,
    cast(advprty.activityworflowdate_icare as date) as activity_workflow_dt,
    cast(advprty.duedate_icare as date) as due_dt,
    cast(advprty.recoverycommenceddate_icare as date) as recovery_commenced_dt,
    CAST(advprty.createtime as TIMESTAMP_NTZ) as src_create_dttm,
    CAST(advprty.updatetime as TIMESTAMP_NTZ) as src_update_dttm,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_subrogationsummary summ

inner join cc_claim clm
    on clm.id = summ.claimid

inner join cc_subroadverseparty advprty
    on advprty.subrogationsummaryid = summ.id

left join cctl_recoverytype_icare rtyp
    on rtyp.id = advprty.recoverytype_icare

left join cctl_subroclosedoutcome otc
    on otc.id = advprty.outcome_icare

left join cc_contact ctt
    on ctt.id = advprty.adversepartyid

left join cctl_subrogationstatus sts
    on sts.id = advprty.subrogationstatus_icare

left join cctl_subrostrategy strat
    on strat.id = advprty.strategy
)
select 
    claim_sk,
    subrogation_sk,
    claim_nbr,
    src_claim_id,
    src_subrogation_id,
    src_adverse_party_id,
    responsible_party_id,
    responsible_party_name,
    subrogation_status_cd,
    subrogation_status_desc,
    strategy_cd,
    strategy_desc,
    recovery_type_cd,
    recovery_type_desc,
    outcome_cd,
    outcome_desc,
    liability_percent,
    expected_recovery_percent,
    expected_recovery_amt,
    court_awarded_interest_amt,
    close_comment,
    close_dt,
    activity_workflow_dt,
    due_dt,
    recovery_commenced_dt,
    src_create_dttm,
    src_update_dttm,
    file_ingestion_timestamp
from
    cte_join
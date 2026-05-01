{% snapshot int_rel_claim_liability_status_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-03-30      0.0                             This Builds the Integrated Layer for int_rel_claim_liability_status_ni for NI
2026-03-30      0.0                             AF Change
-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='claim_liability_status_sk',
        alias='int_rel_claim_liability_status_ni',
        strategy='check',
        check_cols='all',
        tags=['claim', 'integrated', 'NI', 'snapshot_ni']
    )
}}

--get latest record from cc_claim
with cte_claim as (
    select
        id,
        hash_key,
        managingentity_icare,
        claimworkcompid,
        source_system
    from
       {{ ref('v_cc_claim_current') }}
),
--get latest record from cc_claim
cte_ccx_liabilitystatushist_icare as (
    select
        hash_key,
        refid,
        liabilitystatus,
        liabilitystatusdate,
        liabilitystatusdecisiondate,
        provisionalweeks,
        id,
        retired,
        createtime,
        createuserid,
        updatetime,
        claimworkcompid
   from
       {{ ref('v_ccx_liabilitystatushist_icare_current') }}
	),
--get latest record from cctl_compensabilitydecision 
cte_cctl_compensabilitydecision  as (
    select
        id,
        typecode
    from
       {{ ref('v_cctl_compensabilitydecision_current') }} 
),
--get latest record from ccx_managingentity_icare
cte_ccx_managingentity_icare as (
    select
        id,
        name
    from
       {{ ref('v_ccx_managingentity_icare_current') }}
),
--get latest record from int_claim_ni
cte_int_claim_ni as (
    select
        claim_sk,
        src_claim_id
    from
       {{ ref('int_claim_ni_snapshot') }}
    where
        dbt_valid_to is null
),
cte_join as (
    select
        CAST({{ dbt_utils.generate_surrogate_key(['cc_claim.id', 'ccx_liabilitystatushist_icare.id']) }} AS VARCHAR(150)) as claim_liability_status_sk,
       -- ccx_liabilitystatushist_icare.hash_key as claim_liability_status_sk,
        source_system,
        claim_sk,
        refid as claim_liability_status_id,
        liabilitystatus as claim_liability_status_ref_id,
        typecode as claim_liability_status_code,
        liabilitystatusdate as claim_liability_status_eff_ts,
        liabilitystatusdecisiondate as claim_liability_status_entered_ts,
        provisionalweeks as provisional_liability_approved_weeks_qty,
        ccx_liabilitystatushist_icare.id as src_liability_status_hist_id,
        case
            when retired = 0
            or retired is null then 'N'
            when retired >= 1 then 'Y'
        end as retired_ind,
        createtime as src_create_ts,
        createuserid as src_create_user,
        updatetime as src_eff_ts,
    from
        cte_ccx_liabilitystatushist_icare as ccx_liabilitystatushist_icare
        inner join cte_claim as  cc_claim on cc_claim.claimworkcompid = ccx_liabilitystatushist_icare.claimworkcompid
        left outer join cte_cctl_compensabilitydecision as cctl_compensabilitydecision on cctl_compensabilitydecision.id = ccx_liabilitystatushist_icare.liabilitystatus
        left join cte_int_claim_ni claim on claim.src_claim_id=cc_claim.id

) 
-- deduplicate to latest record per claim_liability_status_sk
select
    *
from
    cte_join qualify row_number() over (
        partition by claim_liability_status_sk
        order by
            src_eff_ts desc
    ) = 1
    
    
{% endsnapshot %}		
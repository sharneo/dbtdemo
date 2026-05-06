{% snapshot int_claim_injury_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-03-30      0.0                             This Builds the Integrated Layer for claim Injury for NI
2026-03-30      0.0                             AF Change
2026-03-30      0.0                             AF Changes

-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='loss_sk',
        alias='int_claim_injury_ni',
        strategy='check',
        check_cols='all',
        tags=['claim', 'integrated', 'NI', 'snapshot_ni']
    )
}}
-- Get latest cc_claim record per id
WITH 
cte_int_claim_ni AS (
    SELECT
        claim_sk,
        src_claim_id
    FROM
        {{ ref('int_claim_ni_snapshot') }}
    WHERE
        dbt_valid_to is null
),
-- Get latest cc_incident record per hash_key
cte_incident AS (
    SELECT
        hash_key,
        incident_sk,
        dutystatus_icare,
        description,
        multipleinjuries_icare,
        deceaseddate_icare,
        severity,
        significantinjurydate_icare,
        contactcompletedate_icare,
        fatalitynotificationdate_icare,
        resultofselfharm_icare,
        fatalityliabdec_icare,
        fatalityliabdecisiondate_icare,
        odgrtwdate_icare,
        odgduration_icare,
        breakdownagencycode_icare,
        breakdownagencydesc_icare,
        natureofinjurycode_icare,
        natureofinjurydesc_icare,
        mechanismofinjurycode_icare,
        mechanismofinjurydesc_icare,
        agencyofinjurycode_icare,
        agencyofinjurydesc_icare,
        id,
        retired,
        createtime,
        updatetime,
        claimincident,
        subtype,
        claimid,
        source_system
    FROM
        {{ ref('v_cc_incident_current') }}
    WHERE retired = 0
    AND claimincident = 1        
),
--get latest record from cctl_dutystatus_icare
cte_cctl_dutystatus_icare AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_dutystatus_icare_current') }}
),
--get latest record from cctl_fatalityliabdec_icare
cte_cctl_fatalityliabdec_icare AS (
    SELECT
        id,
        typecode
    FROM {{ ref('v_cctl_fatalityliabdec_icare_current')}}
),
--get latest record from cctl_severitytype
cte_cctl_severitytype AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_severitytype_current') }}
),
--get latest record from cctl_incident
cte_cctl_incident AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_incident_current') }}
    WHERE typecode = 'InjuryIncident'
),
cte_join AS (
    SELECT
       {{ dbt_utils.generate_surrogate_key(['i.id','claim.src_claim_id']) }} as loss_sk,
        --i.hash_key as loss_sk,
        claim.claim_sk as claim_sk,
        source_system,
        dutystatus_icare as claimant_duty_status_ref_id,
        ds.typecode as claimant_duty_status_code,
        description as injury_desc,
        case
            when multipleinjuries_icare = 0
            or multipleinjuries_icare is null then 'N'
            when multipleinjuries_icare >= 1 then 'Y'
        end as multiple_injuries_ind,
        deceaseddate_icare as deceased_dt,
        severity as result_of_injury_ref_id,
        s.typecode as result_of_injury_code,
        significantinjurydate_icare as significant_injury_dt,
        contactcompletedate_icare as contact_complete_dt,
        fatalitynotificationdate_icare as fatality_notification_dt,
         case
            when resultofselfharm_icare = 0
            or resultofselfharm_icare is null then 'N'
            when resultofselfharm_icare >= 1 then 'Y'
        end as fatality_result_of_self_harm_ind,
        fatalityliabdec_icare as fatality_liability_decision_status_ref_id,
        fa.typecode as fatality_liability_decision_status_code,
        fatalityliabdecisiondate_icare as fatality_liability_decision_dt,
        odgrtwdate_icare as odg_rtw_dt,
        odgduration_icare as odg_rtw_day_cnt,
        breakdownagencycode_icare as breakdown_agency_code,
        breakdownagencydesc_icare as breakdown_agency_desc,
        natureofinjurycode_icare as nature_of_injury_code,
        natureofinjurydesc_icare as nature_of_injury_desc,
        mechanismofinjurycode_icare as mechanism_of_injury_code,
        mechanismofinjurydesc_icare as mechanism_of_injury_desc,
        agencyofinjurycode_icare as agency_of_injury_code,
        agencyofinjurydesc_icare as agency_of_injury_desc,
        i.id as src_incident_id,
        case
            when retired = 0
            or retired is null then 'N'
            when retired >= 1 then 'Y'
        end as retired_ind,
        createtime as src_create_ts,
        updatetime as src_eff_ts
    from
        cte_incident i
        left join cte_cctl_dutystatus_icare ds on ds.ID = i.DutyStatus_icare
        left join cte_cctl_fatalityliabdec_icare fa on fa.id = i.FatalityLiabDec_icare
        left join cte_cctl_severitytype s on s.ID = i.Severity
        left join cte_cctl_incident ci on ci.ID = i.Subtype
        inner join cte_int_claim_ni claim on claim.SRC_CLAIM_ID=i.ClaimID
) 

SELECT
    *
FROM
    cte_join QUALIFY ROW_NUMBER() OVER (
        PARTITION BY loss_sk
        ORDER BY
            src_eff_ts DESC
    ) = 1
	
{% endsnapshot %}
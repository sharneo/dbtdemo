{% snapshot int_claim_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change
2026-03-30      0.0                             This Builds the Integrated Layer for Claim NI
-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='claim_nbr',
        alias='int_claim_ni',
        strategy='check',
        check_cols='all',
        tags=['claim', 'integrated', 'NI', 'snapshot_ni']
    )
}}

-- Get latest claim records per hash_key
WITH cte_claim AS (
    SELECT
        hash_key,
        claim_sk,
        ClaimNumber,
        ClaimsAgent_icare,
        BranchInsurer_icare,
        State,
        ReportedDate,
        ReportedByType,
        DateMade_icare,
        AssignmentDate,
        AssignmentStatus,
        CloseDate,
        ClosedOutcome,
        ReOpenDate,
        ReopenedReason,
        SharedClaim_icare,
        LossType,
        LossCause,
        description,
        LossDate,
        IncidentReport,
        ID,
        Retired,
        CreateTime,
        UpdateTime,
        DateRptdToEmployer,
        PolicyID,
        ManagingEntity_icare,
        source_system
    FROM
        {{ ref('v_cc_claim_current') }}
    WHERE
        retired = 0
),
cte_policy AS (
    SELECT
        ID,
        Verified
    FROM
        {{ ref('v_cc_policy_current') }}
    WHERE
        retired = 0
),
cte_cctL_assignmentstatus AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_assignmentstatus_current') }}
),
--get latest record from cctl_claimagent_icare
cte_cctl_claimagent_icare AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_claimagent_icare_current') }}
),
--get latest record from cctl_claimclosedoutcometype
cte_cctl_claimclosedoutcometype AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_claimclosedoutcometype_current') }}
),
--get latest record from cctl_claimreopenedreason
cte_cctl_claimreopenedreason AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_claimreopenedreason_current') }}
),
--get latest record from cctl_claimstate
cte_cctl_claimstate AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_claimstate_current') }}
),
--get latest record from cctl_insurerbranch_icare
cte_cctl_insurerbranch_icare AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_insurerbranch_icare_current') }}
),
cte_cctl_losscause AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_losscause_current') }}
),
--get latest record from cctl_losstype
cte_cctl_losstype AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_losstype_current') }}
),
cte_cctl_personrelationtype AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_personrelationtype_current') }}
),
cte_cctl_sharedclaim_icare AS (
    SELECT
        id,
        typecode
    FROM
        {{ ref('v_cctl_sharedclaim_icare_current') }}
),

cte_ccx_managingentity_icare AS (
    SELECT
        id,
        Name
    FROM
        {{ ref('v_ccx_managingentity_icare_current') }}
),

cte_join AS (
    SELECT
        claim_sk,
        source_system,
        ClaimNumber AS claim_nbr,
        ClaimsAgent_icare AS claim_scheme_agent_ref_id,
        ca.TYPECODE AS claim_scheme_agent_code,
        BranchInsurer_icare AS claim_scheme_agent_branch_ref_id,
        ib.TYPECODE AS claim_scheme_agent_branch_code,
        State AS claim_state_ref_id,
        cs.TYPECODE AS claim_state_code,
        ReportedDate AS claim_report_ts,
        ReportedByType AS claim_report_by_type_ref_id,
        pr.TYPECODE AS claim_report_by_type_code,
        DateMade_icare AS claim_made_ts,
        AssignmentDate AS claim_assignment_ts,
        AssignmentStatus AS claim_assignment_status_ref_id,
        ass.TYPECODE AS claim_assignment_status_code,
        CloseDate AS claim_close_ts,
        ClosedOutcome AS claim_close_outcome_ref_id,
        cco.TYPECODE AS claim_close_outcome_code,
        ReOpenDate AS claim_reopen_ts,
        ReopenedReason AS claim_reopen_reason_ref_id,
        crr.TYPECODE AS claim_reopen_reason_code,
        SharedClaim_icare AS shared_claim_ref_id,
        sc.TYPECODE AS shared_claim_code,
        LossType AS loss_type_ref_id,
        lt.TYPECODE AS loss_type_code,
        LossCause AS loss_cause_ref_id,
        lc.TYPECODE AS loss_cause_code,
        DESCRIPTION AS loss_desc,
        LossDate AS loss_ts,
        CASE
            WHEN IncidentReport = 0
            OR IncidentReport IS NULL THEN 'N'
            WHEN IncidentReport >= 1 THEN 'Y'
        END AS incident_only_ind,
        DateRptdToEmployer AS report_to_emp_ts,
        CASE
            WHEN Verified = 0
            OR Verified IS NULL THEN 'N'
            WHEN Verified >= 1 THEN 'Y'
        END AS verified_policy_ind,
        cc.ID AS src_claim_id,
        CASE
            WHEN Retired = 0
            OR Retired IS NULL THEN 'N'
            WHEN Retired >= 1 THEN 'Y'
        END AS retired_ind,
        CreateTime AS src_create_ts,
        UpdateTime AS src_eff_ts
    from
        cte_claim cc
        left join cte_policy pc on pc.ID = cc.PolicyID
        left join cte_cctl_assignmentstatus ass on ass.ID = cc.AssignmentStatus
        left join cte_cctl_claimagent_icare ca on ca.id = cc.claimsagent_icare
        left join cte_cctl_claimclosedoutcometype cco on cco.ID = cc.ClosedOutcome
        left join cte_cctl_claimreopenedreason crr on crr.ID = cc.ReopenedReason
        left join cte_cctl_claimstate cs on cs.ID = cc.State
        left join cte_cctl_insurerbranch_icare ib on ib.id = cc.BranchInsurer_icare
        left join cte_cctl_personrelationtype pr on pr.ID = cc.ReportedByType
        left join cte_cctl_sharedclaim_icare sc on sc.id = cc.SharedClaim_icare
        left join cte_cctl_losstype lt on lt.ID = cc.LossType
        left join cte_cctl_losscause lc on lc.ID = cc.LossCause
        left join cte_ccx_managingentity_icare cxm on cxm.ID = cc.ManagingEntity_icare
        WHERE cxm.Name like 'NI%'
) 

SELECT
    *
FROM
    cte_join QUALIFY ROW_NUMBER() OVER (
        PARTITION BY claim_nbr
        ORDER BY
            src_eff_ts DESC
    ) = 1
{% endsnapshot %}
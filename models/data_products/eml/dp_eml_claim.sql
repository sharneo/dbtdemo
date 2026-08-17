
{#-

Project: Data Uplift Program
Date            Version         Author          Description of Change           
2026-01-01      0.5                             Data Products for Claim  

-#}


{{
  config(
    materialized='table',
    tags=['data_products', 'integrated_ni', 'eml', 'business_critical']
  )
}}

with cte_claim as (
    select *
    from {{ ref('int_claim') }}
    WHERE managing_entity_cd='NI_DP_EML'
),

cte_management_entity as (
    select
        mgmt.managing_entity_sk,
        array_agg(object_construct_keep_null(
            'managingEntityId', mgmt.managing_entity_id,
            'portalDesc', mgmt.portal_display_name,
            'managingEntityName', mgmt.managing_entity_name,
            'managingEntityRole', mgmt.managing_entity_role
        )) as management_entity
    from {{ ref('int_managing_entity') }} mgmt
    group by 1
),

cte_claim_transfer as (
    select
        transfer.claim_nbr,
        array_agg(object_construct_keep_null(
            'claimNbr', transfer.claim_nbr,
            'srcClaimNbr', transfer.src_claim_id,
            'fromManagingEntity', transfer.from_managing_entity,
            'toManagingEntity', transfer.to_managing_entity,
            'transferDttm', cast(transfer.transfer_dttm as date)
        )) as transfer_entity
    from {{ ref('int_claim_me_transfer') }} transfer
    group by 1
),

cte_claim_financial_summary as (
    select
        fs.claim_nbr,
        array_agg(object_construct_keep_null(
            'claimNbr', fs.claim_nbr,
            'srcClaimNbr', fs.src_claim_id,
            'exposure_type_code', fs.exposure_type_cd,
            'exposure_type_desc', fs.exposure_type_desc,
            'financials', object_construct_keep_null(
                'available_reserves', fs.available_reserves,
                'open_reserves', fs.open_reserves,
                'open_recovery_reserves', fs.open_recovery_reserves,
                'total_payments_made',fs.total_payments_made,
                'total_recoveries',fs.total_recoveries,
                'summary_eff_date',CAST(fs.summary_eff_dttm AS DATE )
            ),
            'status', object_construct_keep_null(
                'status', fs.exp_status_desc
            )
        )) as financial_summary
    from {{ ref('int_claim_financial_summary') }} fs
    group by 1
),

cte_claim_transaction as (
    select
    txn.claim_nbr,
        array_agg(object_construct_keep_null(
            'claimNbr', txn.claim_nbr,
            'srcClaimNbr', txn.src_claim_id,
            'dates', object_construct_keep_null(
                'txn_submitted_date', txn.txn_submitted_dt,
                'txn_approval_date', txn.txn_approval_dt
            ),
            'status', object_construct_keep_null(
                'txn_approval_status_cd', txn.txn_approval_status_cd,
                'txn_approval_status_desc', txn.txn_approval_status_desc,
                'txn_status_cd', txn.txn_status_cd,
                'txn_status_desc', txn.txn_status_desc,
                'txn_lifecycle_state_cd', txn.txn_lifecycle_state_cd,
                'txn_lifecycle_state_desc', txn.txn_lifecycle_state_desc
            )
        )) as claim_transaction
    from {{ ref('int_claim_txn') }} txn
    group by 1
),

cte_claim_payment as (
    select
    cp.claim_nbr,
        array_agg(object_construct_keep_null(
            'claimNbr', cp.claim_nbr,
            'dates', object_construct_keep_null(
                'payment_issue_date', cp.payment_issue_dt,
                'last_posting_dt', cp.last_posting_dt,
                'payment_presented_dt', cp.payment_presented_dt,
                'payment_scheduled_send_dt', cp.payment_scheduled_send_dt,
                'payment_transacted_dt', cp.payment_transacted_dt,
                'service_provision_dt', cp.service_provision_dt,
                'service_period_start_dt',cp.service_provision_dt,
                'service_period_end_dt',cp.service_period_end_dt,
                'payment_dt',cp.payment_dt
            ),
            'payments', object_construct_keep_null(
                'payg_amt', cp.payg_amt,
                'payment_amt', cp.payment_amt
            )
        )) as claim_payment
    from {{ ref('int_claim_payment') }} cp
    group by 1
),

cte_liability_statuses as (
    select
        liab.claim_sk,
        array_agg(object_construct_keep_null(
            'statusCd', liab.claim_liability_status_cd,
            'statusDesc', liab.claim_liability_status_desc,
            'effectiveDt', liab.claim_liability_status_eff_dttm,
            'effectiveDtDate', liab.claim_liability_status_eff_dt,
            'enteredDt', liab.claim_liability_status_ent_dttm,
            'enteredDtDate', liab.claim_liability_status_ent_dt,
            'reasonableExcuse', object_construct_keep_null(
                'code', liab.reasonable_excuse_cd,
                'desc', liab.reasonable_excuse_desc
            ),
            'provisionalLiabilityWeeks', liab.provisional_liab_appr_weeks_qty,
            'liabilityWklyBenfEndDt', liab.liability_wkly_benf_end_dt,
            'noticePeriod', liab.notice_period,
            'mbed', liab.mbed,
            'createUserSk', liab.liability_status_create_user_sk,
            'rankLatest', liab.latest_liab_status_record_rank,
            'rankEarliest', liab.earlst_liab_status_record_rank
        )) as liability_statuses
    from {{ ref('int_liability_status') }} liab
    group by 1
),

cte_accident as (
    select
        accident.claim_nbr,
        array_agg(object_construct_keep_null(
            'recovery_investigation_ind', accident.recovery_investigation_ind,
            'policy_location_addr', accident.policy_location_addr,
            'toocs_mechanism_of_injury_desc', accident.toocs_mechanism_of_injury_desc,
            'accident_location_type_cd', accident.accident_location_type_cd,
            'accident_location_type_desc', accident.accident_location_type_desc,
            'src_updated_dttm', accident.src_updated_dttm,
            'src_updated_dt', accident.src_updated_dt,
            'severity', accident.severity
        )) as accident_object
    from {{ ref('int_claim_accident') }} accident 
    group by 1
),

cte_piawe as (
    select
        piawe.claim_nbr,
        array_agg(object_construct_keep_null(
            'src_claim_id', piawe.src_claim_id,
            'piawe_effective_dt', piawe.piawe_effective_dt,
            'piawe_expiry_dt', piawe.piawe_expiry_dt,
            'piawe_type', piawe.piawe_type,
            'piawe_type_desc', piawe.piawe_type_desc,
            'src_create_dttm', piawe.src_create_dttm,
            'latest_piawe_rank', piawe.latest_piawe_rank
        )) as piawe_object
    from {{ ref('int_piawe') }} piawe 
    group by all
),

cte_coc as (
    select
        coc.claim_nbr,
        array_agg(object_construct_keep_null(
            'src_claim_id', coc.src_claim_id,
            'fitness_cd', coc.fitness_cd,
            'fitness_desc', coc.fitness_desc,
            'start_date',coc.certificate_of_capacity_start_dt,
            'end_date',coc.certificate_of_capacity_end_dt,
            'hrs_worked_per_week', coc.certificate_of_capacity_hours_worked_per_week ,
            'certificate_order',coc.latest_certificate_of_capacity_record_rank,
            'certificate_retired_ind',coc.retired_ind,
            'certificate_of_capacity_status',coc.certificate_of_capacity_status
        )) as coc_object
    from {{ ref('int_certificate_of_capacity') }} coc 
    group by all
),

cte_injury as (
    select
        injury.claim_nbr,
        array_agg(object_construct_keep_null(
            'src_claim_id', injury.src_claim_id,
            'src_incident_id', injury.src_incident_id,
            'injury_desc', injury.injury_desc,
            'multiple_injuries_ind',injury.multiple_injuries_ind
        )) as injury_object
    from {{ ref('int_injury') }} injury 
    group by all
)


select
    clm.claim_sk,
    clm.claim_nbr,
    clm.source_system,
    clm.managing_entity_cd,
    clm.loss_dt as loss_date,
    object_construct_keep_null(
    'claim',
    object_construct_keep_null(
        'claim', object_construct_keep_null(
            'header', object_construct_keep_null(
                'id', clm.src_claim_id,
                'claimNumber', clm.claim_nbr,
                'description', clm.loss_desc,
                'state', object_construct('code', clm.claim_state_cd, 'name', clm.claim_state_desc),
                'segment', object_construct('code', clm.claim_segment_cd, 'name', clm.claim_segment_desc),
                'assignmentStatus', object_construct('code', clm.claim_assignment_status_cd, 'name', clm.claim_assignment_status_desc),
                'closeOutcome', object_construct('code', clm.claim_close_outcome_cd, 'name', clm.claim_close_outcome_desc)
            ),
            'dates', object_construct_keep_null(
                'lossDate', CAST(clm.loss_dttm AS DATE),
                'reportDate', CAST(clm.claim_report_dttm AS DATE),
                'assignmentDate', CAST(clm.claim_assignment_dttm AS DATE),
                'createdDate', CAST(clm.src_create_dttm AS DATE),
                'reportToEmployer', clm.report_to_emp_dttm,
                'claimMadeDate', clm.claim_made_dttm,
                'close', object_construct_keep_null(
                    'closedDate', clm.claim_close_dttm,
                    'reopenDate', clm.claim_reopen_dttm,
                    'reopenCloseDate', clm.claim_reopen_close_dttm,
                    'reopenReason', object_construct_keep_null(
                        'code', clm.claim_reopen_reason_cd,
                        'name', clm.claim_reopen_reason_desc
                    )
                )
            ),
            'policy', object_construct_keep_null(
                'id', clm.policy_nbr,
                'policyNumber', clm.policy_nbr,
                'verified', clm.policy_verified = 'Y',
                'legacyPolicyNumber', clm.legacy_policy_nbr,
                'groupNumber', clm.policy_group_nbr,
                'policyType', object_construct('code', clm.policy_type, 'name', clm.policy_type)
            ),
            'assignment', object_construct_keep_null(
                'user', object_construct_keep_null('id', clm.case_owner_user_sk),
                'group', object_construct_keep_null('id', clm.case_owner_team_sk)
            ),
            'loss', object_construct_keep_null(
                'type', clm.loss_type_cd,
                'cause', clm.loss_cause_cd,
                'description', clm.loss_desc,
                'location', object_construct_keep_null(
                    'code', clm.accident_location_cd,
                    'description', clm.accident_location_desc
                ),
                'flags', object_construct_keep_null(
                    'incidentOnly', clm.incident_only_ind = 'Y',
                    'commonLaw', clm.common_law_ind = 'Y',
                    'lostTime', clm.lost_time_ind = 'Y',
                    'fatality', clm.claim_fatality_ind = 'Y',
                    'conflictOfInterest', clm.conflict_of_interest_ind = 'Y',
                    'sensitive', clm.sensitive_claim_cd is not null,
                    'labourHire', clm.claim_policy_labour_hire_flag
                )
            ),
            'injuredWorker', object_construct_keep_null(
                'dateOfBirth', clm.injured_worker_dob,
                'contactProhibited', clm.contact_prohibited = 'Y',
                'contactMobile', clm.contact_mobile,
                'employment', object_construct_keep_null(
                    'status', clm.employment_status,
                    'employerSize', clm.employer_size_desc,
                    'employerCategory', clm.employer_category_desc,
                    'hoursPerWeek', clm.total_hours_per_week_latest_cert_of_cap
                )
            ),
            'risk', object_construct_keep_null(
                'rating', clm.overall_risk_rating,
                'lastTriageDate', clm.last_triage_date,
                'litigation', object_construct_keep_null(
                    'statusCode', clm.litigation_status_cd,
                    'statusDesc', clm.litigation_status_desc,
                    'identifiedDate', clm.litigation_identified_dt
                ),
                'wic', object_construct_keep_null(
                    'code', clm.wic_code,
                    'description', clm.wic_desc
                )
            ),
            'schemeAgent', object_construct_keep_null(
                'code', clm.claim_scheme_agent_cd,
                'name', clm.claim_scheme_agent_desc,
                'branch', object_construct_keep_null(
                    'code', clm.claim_scheme_agent_branch_cd,
                    'name', clm.claim_scheme_agent_branch_desc
                )
            ),
            'costCentre', object_construct_keep_null(
                'number', clm.cost_centre_nbr,
                'name', clm.cost_centre_name
            )
        ),

        'liability', object_construct_keep_null(
            'currentStatus', object_construct_keep_null(
                'code', clm.liability_status_cd,
                'description', clm.liability_status_desc
            ),
            'statuses', coalesce(liab_agg.liability_statuses, array_construct())
        ),

        'financial_summary', object_construct_keep_null(
            'financial_summary', coalesce(fs_agg.financial_summary, array_construct())
        ),

        'claim_transaction', object_construct_keep_null(
            'claim_transactions', coalesce(claim_txn_agg.claim_transaction, array_construct())
        ),

        'claim_payment', object_construct_keep_null(
            'claim_payments', coalesce(claim_payment_agg.claim_payment, array_construct())
        ),

        'claim_injury', object_construct_keep_null(
            'claim_injuries', coalesce(injury_agg.injury_object, array_construct())
        ),

        'claim_piawe', object_construct_keep_null(
            'claim_piawe', coalesce(piawe_agg.piawe_object, array_construct())
        ),

        'claim_accident', object_construct_keep_null(
            'claim_accidents', coalesce(accident_agg.accident_object, array_construct())
        ),

        'claim_certificate_of_capacity', object_construct_keep_null(
            'certificates_of_capacity', coalesce(coc_agg.coc_object, array_construct())
        ),

        'managingEntity', object_construct_keep_null(
            'id', clm.managing_entity_sk,
            'code', clm.managing_entity_cd,
            'entities', coalesce(mgmt.management_entity, array_construct()),
            'transfers', coalesce(transfer.transfer_entity, array_construct())
        )
    )) as claim_json

from cte_claim as clm
left outer join cte_management_entity as mgmt
    on clm.managing_entity_sk = mgmt.managing_entity_sk
left outer join cte_claim_transfer as transfer
    on clm.claim_nbr = transfer.claim_nbr
left outer join cte_liability_statuses as liab_agg
    on clm.claim_sk = liab_agg.claim_sk
left outer join cte_claim_financial_summary as fs_agg
    on clm.claim_nbr = fs_agg.claim_nbr
left outer join cte_claim_transaction as claim_txn_agg
    on clm.claim_nbr = claim_txn_agg.claim_nbr
left outer join cte_claim_payment as claim_payment_agg
    on clm.claim_nbr = claim_payment_agg.claim_nbr
left outer join cte_accident as accident_agg
    on clm.claim_nbr = accident_agg.claim_nbr
left outer join cte_piawe as piawe_agg
    on clm.claim_nbr = piawe_agg.claim_nbr
left outer join cte_coc as coc_agg
    on clm.claim_nbr = coc_agg.claim_nbr
left outer join cte_injury as injury_agg
    on clm.claim_nbr = injury_agg.claim_nbr

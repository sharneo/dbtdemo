{% snapshot int_policy_period_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

General COALESCE is to ensure Deleted Records i.e. 1 for GWCP in Snowflake hence should be ignored. However AVRO files will not have this flag 

Date            Version         Author          Description of Change
2026-01-01      0.0                             This Builds the Integrated Layer for Policy NI
2026-03-12      0.0             Ranjita         This Builds includes R2 changes
2026-03-20      0.0             AF              Formatting & Snapshot
-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='policy_period_sk',
        alias='int_policy_ni_period',
        strategy='check',
        check_cols='all',
        tags=['policy', 'integrated', 'NI', 'snapshot_ni']
    )
}}

WITH cte_policy AS (

    SELECT
        id                                             AS policyid,
        hash_key                                       AS policy_sk,
        accountid,
        productcode                                    AS product_code
    FROM {{ ref('vw_pc_policy') }}
    WHERE retired = 0
    AND COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY
    ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY updatetime DESC) = 1

),

cte_policyperiod AS (

    SELECT
        hash_key                                       AS policy_period_sk,
        DATE(periodstart)                              AS period_start_date,
        DATE(periodend)                                AS period_end_date,
        policyid,
        policytermid,
        status,
        id,
        policynumber                                   AS policy_number,
        createtime,
        updatetime,
        termnumber,
        editeffectivedate,
        cancellationdate,
        jobid,
        primaryinsurednamedenorm,
        allocationofremainder,
        allowgapsbefore,
        altbillingaccountnumber,
        assignedrisk,
        basedonid,
        branchname,
        branchnumber,
        cancelonexpiry_icare,
        createuserid,
        depositamount,
        depositcollected,
        depositoverridepct,
        invoicestreamcode,
        islegacy_icare,
        modelnumber,
        overridebillingallocation,
        periodend,
        periodstart,
        premiumadjustmentreason,
        publicid,
        quotecloneoriginalperiod,
        quoteclonesequencenumber,
        rateasofdate,
        reasonlowwages_icare,
        refundcalcmethod,
        segment,
        totalcostrpt,
        totalpremiumrpt,
        transactioncostrpt,
        transactionpremiumrpt,
        updateuserid,
        writtendate,
        policy_number                            AS policy_bk
    FROM {{ ref('vw_pc_policyperiod') }}
    WHERE policynumber IS NOT NULL
      AND COALESCE(GWCBI_OPERATION,0) <> 1 
      AND policynumber NOT LIKE 'SIC%'
     QUALIFY ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY updatetime DESC) = 1

),

cte_policyterm AS (

    SELECT
        id,
        affinitygroupid,
        bound,
        depositreleased,
        finalauditoption,
        generatereinsurables,
        lossratio,
        lossratiocalculationdate,
        mostrecentterm,
        nextrenewalcheckdate,
        niladjustfailed_icare,
        nonrenewaddexplanation
    FROM {{ ref('vw_pc_policyterm') }}
    WHERE retired = 0
    AND COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatetime DESC) = 1

),

cte_pctl_policyperiodstatus AS (

    SELECT
        id,
        name                                           AS period_status_code
    FROM {{ ref('vw_pctl_policyperiodstatus') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_pc_job AS (

    SELECT
        policyid,
        subtype,
        id,
        cancelprocessdate,
        datequoteneeded,
        initialnotificationdate,
        lastnotifiedcancellationdate,
        nonrenewalnotifdate,
        notificationackdate,
        notificationdate,
        nottakennotifdate,
        producerchangesource_ext,
        quoteddate_icare,
        rejectreasontext,
        renewalnotifdate,
        rescindnotificationdate,
        schemeagentchange_icare,
        submissiondate
    FROM {{ ref('vw_pc_job') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY updatetime DESC) = 1

),

cte_pctl_job AS (

    SELECT
        id,
        name
    FROM {{ ref('vw_pctl_job') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_pc_account AS (

    SELECT
        id,
        accountorgtype,
        agencytype_ext,
        agencycode_ext
    FROM {{ ref('vw_pc_account') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY updatetime DESC) = 1

),

cte_pc_effectivedatedfields AS (

    SELECT
        id,
        billingcontact,
        policyaddress,
        primarylocation,
        publicid,
        branchid
    FROM {{ ref('vw_pc_effectivedatedfields') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatetime DESC) = 1

),

cte_pctl_accountorgtype AS (

    SELECT
        id,
        name
    FROM {{ ref('vw_pctl_accountorgtype') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_pctl_agencytype_ext AS (

    SELECT
        id,
        name
    FROM {{ ref('vw_pctl_agencytype_ext') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_policyline AS (

    SELECT
        branchid,
        id,
        subtype
    FROM {{ ref('vw_pc_policyline') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updatetime DESC) = 1

),

cte_pctl_policyline AS (

    SELECT
        id,
        name
    FROM {{ ref('vw_pctl_policyline') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_join AS (

    SELECT
        pcp.policy_period_sk,
        pp.policy_sk,
        pcp.policy_number || '~' || pcp.termnumber     AS policy_period_bk,
        'GWPC'                                          AS source_system,
        'NI'                                            AS scheme_name,
        pplctl.name                                     AS line_of_business,
        account_0.agencytype_ext                        AS agency_type_code,
        agencytype.name                                 AS agency_type_desc,
        account_0.agencycode_ext                        AS agency_code,
        pcp.primaryinsurednamedenorm                    AS policy_holder_name,
        pcp.period_start_date,
        pcp.period_end_date,
        CASE
            WHEN pcp.termnumber = 1                          THEN 'NEW'
            WHEN LOWER(pctlj.name) LIKE '%cancel%'           THEN 'CANCELLATION'
            WHEN LOWER(pctlj.name) LIKE '%policychange%'     THEN 'ENDORSEMENT'
            WHEN LOWER(pctlj.name) LIKE '%renewal%'          THEN 'RENEWAL'
            ELSE UPPER(pctlj.name)
        END                                             AS transaction_type,
        CASE
            WHEN LOWER(pj.subtype) LIKE '%cancel%' THEN pcp.cancellationdate
            ELSE pcp.editeffectivedate
        END                                             AS transaction_eff_date,
        COALESCE(pps.period_status_code, 'UNKNOWN')     AS period_status_code,
        pe.billingcontact                               AS source_bc_billing_contact_id,
        pe.policyaddress                                AS source_policy_addr_id,
        pe.primarylocation                              AS source_primary_location_id,
        pe.publicid                                     AS source_policy_term_other_attr_ext_id,
        pj.cancelprocessdate                            AS cancel_processed_ts,
        pj.datequoteneeded                              AS submission_quote_required_date,
        pj.initialnotificationdate                      AS cancel_init_notify_ts,
        pj.lastnotifiedcancellationdate                 AS cancel_last_notify_ts,
        pj.nonrenewalnotifdate                          AS non_renew_notify_ts,
        pj.notificationackdate                          AS cancel_notify_ack_ts,
        pj.notificationdate                             AS cancel_notify_ts,
        pj.nottakennotifdate                            AS not_taken_notify_sent_ts,
        pj.producerchangesource_ext                     AS bulk_change_source,
        pj.quoteddate_icare                             AS draft_ts,
        pj.rejectreasontext                             AS submission_reject_reason_notes,
        pj.renewalnotifdate                             AS renew_notify_sent_ts,
        pj.rescindnotificationdate                      AS cancel_rescind_notify_ts,
        pj.schemeagentchange_icare                      AS bulk_change_scheme_agent,
        pj.submissiondate                               AS submission_entered_ts,
        pcp.allocationofremainder                       AS source_remainder_cost_allocation_id,
        pcp.allowgapsbefore                             AS allow_term_gap_before_ind,
        pcp.altbillingaccountnumber                     AS source_bc_account_number,
        CASE
            WHEN pcp.assignedrisk = 0 OR pcp.assignedrisk IS NULL THEN 'N'
            WHEN pcp.assignedrisk >= 1                             THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS assigned_risk,
        pcp.basedonid                                   AS based_on_source_policy_term_id,
        pcp.branchname                                  AS side_by_side_branch_name,
        pcp.branchnumber                                AS side_by_side_branch_number,
        pcp.cancellationdate                            AS cancel_eff_ts,
        CASE
            WHEN pcp.cancelonexpiry_icare = 0 OR pcp.cancelonexpiry_icare IS NULL THEN 'N'
            WHEN pcp.cancelonexpiry_icare >= 1                                     THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS cancel_on_exp_ind,
        pcp.createtime                                  AS source_create_ts,
        pcp.createuserid                                AS source_create_user_id,
        pcp.depositamount                               AS deposit_amt,
        pcp.depositcollected                            AS deposit_collected,
        pcp.depositoverridepct                          AS deposit_override_pct,
        pcp.id                                          AS source_policy_term_id,
        pcp.invoicestreamcode                           AS source_billing_invoice_stream_id,
        CASE
            WHEN pcp.islegacy_icare = 0 OR pcp.islegacy_icare IS NULL THEN 'N'
            WHEN pcp.islegacy_icare >= 1                               THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS is_migrated_term_ind,
        pcp.modelnumber                                 AS source_policy_term_version_number,
        pcp.overridebillingallocation                   AS override_billing_allocation_ind,
        pcp.periodend                                   AS policy_term_exp_ts,
        pcp.periodstart                                 AS policy_term_eff_ts,
        pcp.premiumadjustmentreason                     AS prem_adj_reason,
        pcp.publicid                                    AS source_ext_policy_term_id,
        pcp.quotecloneoriginalperiod                    AS source_clone_master_policy_term_id,
        pcp.quoteclonesequencenumber                    AS source_clone_master_policy_term_seq_number,
        pcp.rateasofdate                                AS prem_calc_ts,
        pcp.reasonlowwages_icare                        AS low_wages_reason,
        pcp.refundcalcmethod                            AS source_refund_calc_method_id,
        pcp.segment                                     AS source_mkt_segment_id,
        pcp.termnumber                                  AS policy_term_number,
        pcp.totalcostrpt                                AS total_cost,
        pcp.totalpremiumrpt                             AS total_prem_cost,
        pcp.transactioncostrpt                          AS cost_change,
        pcp.transactionpremiumrpt                       AS prem_cost_change,
        pcp.updatetime                                  AS source_eff_ts,
        pcp.updateuserid                                AS source_update_user_id,
        pcp.writtendate                                 AS written_ts,
        pt.affinitygroupid                              AS source_affinity_group_id,
        CASE
            WHEN pt.bound = 0 OR pt.bound IS NULL THEN 'N'
            WHEN pt.bound >= 1                     THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS renew_complete_ind,
        CASE
            WHEN pt.depositreleased = 0 OR pt.depositreleased IS NULL THEN 'N'
            WHEN pt.depositreleased >= 1                               THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS deposit_released_ind,
        pt.finalauditoption                             AS source_final_audit_option,
        CASE
            WHEN pt.generatereinsurables = 0 OR pt.generatereinsurables IS NULL THEN 'N'
            WHEN pt.generatereinsurables >= 1                                    THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS generate_reinsurables_ind,
        pt.lossratio                                    AS loss_ratio,
        pt.lossratiocalculationdate                     AS loss_ratio_calc_ts,
        CASE
            WHEN pt.mostrecentterm = 0 OR pt.mostrecentterm IS NULL THEN 'N'
            WHEN pt.mostrecentterm >= 1                              THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS most_recent_term_ind,
        pt.nextrenewalcheckdate                         AS renew_next_check_ts,
        CASE
            WHEN pt.niladjustfailed_icare = 0 OR pt.niladjustfailed_icare IS NULL THEN 'N'
            WHEN pt.niladjustfailed_icare >= 1                                     THEN 'Y'
            ELSE 'UNKNOWN'
        END                                             AS nil_adjust_failed_ind,
        pt.nonrenewaddexplanation                       AS non_renew_notes,
        pcp.updatetime                                  AS effective_from_ts
    FROM cte_policyperiod AS pcp
    INNER JOIN cte_policy AS pp
        ON pcp.policyid = pp.policyid
    INNER JOIN cte_pctl_policyperiodstatus AS pps
        ON pps.id = pcp.status
    INNER JOIN cte_pc_account AS account_0
        ON account_0.id = pp.accountid
    INNER JOIN cte_pctl_accountorgtype AS accountorgtype_0
        ON accountorgtype_0.id = account_0.accountorgtype
    LEFT JOIN cte_pc_job AS pj
        ON pcp.jobid = pj.id
    LEFT JOIN cte_pctl_job AS pctlj
        ON pctlj.id = pj.subtype
    LEFT JOIN cte_pctl_agencytype_ext AS agencytype
        ON agencytype.id = account_0.agencytype_ext
    LEFT JOIN cte_policyline AS ppl
        ON pcp.policyid = ppl.id
    LEFT JOIN cte_pctl_policyline AS pplctl
        ON pplctl.id = ppl.subtype
    LEFT JOIN cte_policyterm AS pt
        ON pt.id = pcp.policytermid
    LEFT JOIN cte_pc_effectivedatedfields AS pe
        ON pe.branchid = pcp.id
    WHERE accountorgtype_0.name NOT LIKE 'TMF%'
      AND pp.product_code = 'WC_ICARE'

)

SELECT * FROM cte_join
     QUALIFY ROW_NUMBER() OVER (PARTITION BY policy_period_sk ORDER BY effective_from_ts DESC) = 1

{% endsnapshot %}
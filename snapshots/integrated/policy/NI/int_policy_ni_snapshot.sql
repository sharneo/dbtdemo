{% snapshot int_policy_ni_snapshot %}

{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

-- General COALESCE is to ensure Deleted Records are 1 in Snowflake hence should be ignored 
Date            Version         Author          Description of Change
2026-01-01      0.0                             This Builds the Integrated Layer for Policy NI
2026-03-12      0.0             Ranjita         This Builds includes R2 changes
2026-03-20      0.0             AF              Formatting & Snapshot 
                                                 

-#}

{{
    config(
        target_schema='integrated_ni',
        unique_key='policy_sk',
        alias='int_policy_ni',
        strategy='check',
        check_cols='all',
        tags=['policy', 'integrated', 'NI', 'snapshot_ni']
    )
}}

WITH cte_policy AS (

    SELECT
        hash_key                                    AS policy_sk,
        accountid,
        id                                          AS policyid,
        createtime,
        updatetime,
        productcode                                 AS product_code,
        crnnumber_icare,
        groupnumberfromportal,
        isportalpolicy_icare,
        losshistorytype,
        movedpolicysourceaccountid,
        packagerisk,
        priorpremiums,
        priortotalincurred,
        publicid,
        to_char(originaleffectivedate, 'dd-mm-yyyy') AS inception_date
    FROM {{ ref('vw_pc_policy') }}
    WHERE retired = 0
      AND COALESCE(GWCBI_OPERATION,0) <> 1 
      AND productcode = 'WC_ICARE'
 QUALIFY
  ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY updatetime DESC) = 1
),

cte_policyperiod AS (

    SELECT
        to_char(periodstart, 'dd-mm-yyyy')          AS policy_start_date,
        policyid,
        status,
        id,
        policynumber                                 AS policy_number,
        primaryinsurednamedenorm,
        commencementdate_icare,
        legacypolicynumber_icare,
        createtime,
        updatetime
    FROM {{ ref('vw_pc_policyperiod') }}
    WHERE policynumber IS NOT NULL
      AND COALESCE(GWCBI_OPERATION,0) <> 1  
      AND policy_number NOT LIKE 'SIC%'
 QUALIFY
  ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY updatetime DESC) = 1

),

cte_pc_account AS (

    SELECT
        id,
        accountorgtype,
        agencytype_ext,
        agencycode_ext
    FROM {{ ref('vw_pc_account') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1
 QUALIFY
  ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY updatetime DESC) = 1

),

cte_pctl_accountorgtype AS (

    SELECT
        id,
        name
    FROM {{ ref('vw_pctl_accountorgtype') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1
 QUALIFY
  ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_pctl_policyperiodstatus AS (

    SELECT
        id,
        name                                         AS current_status_code
    FROM {{ ref('vw_pctl_policyperiodstatus') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_policyline AS (

    SELECT
        branchid,
        id,
        subtype
    FROM {{ ref('vw_pc_policyline') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_pctl_policyline AS (

    SELECT
        id,
        name
    FROM {{ ref('vw_pctl_policyline') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_pc_job AS (

    SELECT
        policyid
    FROM {{ ref('vw_pc_job') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY policyid ORDER BY updatetime DESC) = 1

),

cte_pctl_agencytype_ext AS (

    SELECT
        id,
        name
    FROM {{ ref('vw_pctl_agencytype_ext') }}
    WHERE COALESCE(GWCBI_OPERATION,0) <> 1
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY file_ingestion_timestamp DESC) = 1

),

cte_join AS (

    SELECT
        pp.policy_sk,
        pcp.policy_number                            AS policy_bk,
        'GWPC'                                       AS source_system,
        'NI'                                         AS scheme_name,
        COALESCE(pp.product_code, 'UNKNOWN')         AS product_code,
        CASE pp.product_code
            WHEN 'BusinessAuto'       THEN 'MV'
            WHEN 'CommercialProperty' THEN 'PY'
            WHEN 'GeneralLiability'   THEN 'PL'
            WHEN 'Miscellaneous_Ext'  THEN 'MI'
            WHEN 'WC_ICARE'          THEN 'WC'
            ELSE pp.product_code
        END                                          AS product_name,
        pplctl.name                                  AS line_of_business,
        pp.accountid                                 AS account_id,
        account_0.accountorgtype                     AS account_org_type_code,
        accountorgtype_0.name                        AS account_org_type_desc,
        account_0.agencytype_ext                     AS agency_type_code,
        agencytype.name                              AS agency_type_desc,
        account_0.agencycode_ext                     AS agency_code,
        pcp.primaryinsurednamedenorm                 AS policy_holder_name,
        pp.inception_date,
        COALESCE(pps.current_status_code, 'UNKNOWN') AS current_status_code,
        pplctl.name                                  AS primary_risk_type,
        pp.crnnumber_icare                           AS bpay_emp_policy_ref_number,
        pp.groupnumberfromportal                     AS portal_emp_group_number,
        CASE
            WHEN pp.isportalpolicy_icare = 0  THEN 'N'
            WHEN pp.isportalpolicy_icare >= 1 THEN 'Y'
            ELSE 'N'
        END                                          AS policy_created_via_portal_ind,
        pp.losshistorytype                           AS loss_history_type,
        pp.movedpolicysourceaccountid                AS source_prior_emp_id,
        pp.packagerisk                               AS source_package_risk_id,
        pp.priorpremiums                             AS prior_premiums,
        pp.priortotalincurred                        AS prior_loss_total_incurred,
        pp.publicid                                  AS source_ext_policy_id,
        pcp.commencementdate_icare                   AS commencement_dt,
        pcp.legacypolicynumber_icare                 AS legacy_policy_number,
        pp.createtime                                AS created_ts,
        pp.updatetime                                AS updated_ts,
        pp.updatetime                                AS effective_from_ts,
        pcp.updatetime                               AS source_eff_ts
    FROM cte_policyperiod AS pcp
    INNER JOIN cte_policy AS pp
        ON pcp.policyid = pp.policyid
    INNER JOIN cte_pc_account AS account_0
        ON account_0.id = pp.accountid
    INNER JOIN cte_pctl_accountorgtype AS accountorgtype_0
        ON accountorgtype_0.id = account_0.accountorgtype
    INNER JOIN cte_pctl_policyperiodstatus AS pps
        ON pps.id = pcp.status
    INNER JOIN cte_policyline AS ppl
        ON pcp.policyid = ppl.id
    LEFT JOIN cte_pctl_policyline AS pplctl
        ON pplctl.id = ppl.subtype
    LEFT JOIN cte_pctl_agencytype_ext AS agencytype
        ON agencytype.id = account_0.agencytype_ext
    WHERE accountorgtype_0.name NOT LIKE 'TMF%'
      AND pp.product_code = 'WC_ICARE'

)

SELECT * FROM cte_join
QUALIFY
rOW_NUMBER() OVER (PARTITION BY policy_sk ORDER BY updated_ts DESC) = 1

{% endsnapshot %}
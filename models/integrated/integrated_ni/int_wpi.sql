{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for WPI.
                                                claim_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{
  config(
    materialized='incremental',
    unique_key=['claim_wpi_sk'],
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 32_WPI.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A32
  TBL_NM: MSC_QLK_ASPIRE_WPI
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
        AND file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

cc_exposure as (
    select
        id,
        claimid
    from {{ ref('v_cc_exposure_current') }}
    where retired = 0
),

ccx_wpiassessment_icare as (
    select
        id,
        exposureid
    from {{ ref('v_ccx_wpiassessment_icare_current') }}
    where retired = 0
),

ccx_wpiassessrecord_icare as (
    select
        id,
        wpiassessment_icareid,
        wpiresult_icare,
        claimedpercentage_icare,
        offeredwpipercentage_icare,
        assessedwpifors66,
        s66receiveddate_icare,
        bhlresult_icare,
        wpiassessmentstate_icare,
        settlementtype_icare,
        createtime
    from {{ ref('v_ccx_wpiassessrecord_icare_current') }}
    where retired = 0
),

ccx_wpidoctorassessment_icare as (
    select
        id,
        wpiassessrecord_icareid,
        reviewer_icareid,
        assessmentdate_icare,
        createtime
    from {{ ref('v_ccx_wpidoctorassessment_icare_current') }}
    where retired = 0
        and reviewer_icareid is not null
),

cctl_wpisettlementtype_icare as (
    select
        id,
        name
    from {{ ref('v_cctl_wpisettlementtype_icare_current') }}
),
cte_join as 
(
select
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as src_system_cd,
    clm.claimnumber as claim_nbr,
    asrcd.id as src_wpi_assess_record_id,
    dim_settlementtype.name as wpi_claim_type,
    case
        when asrcd.wpiassessmentstate_icare = 1 then 'Closed'
        else 'Open'
    end as wpi_status,
    asrcd.wpiresult_icare as wpi_result_pct,
    asrcd.claimedpercentage_icare as wpi_claimed_pct,
    asrcd.offeredwpipercentage_icare as wpi_offered_pct,
    asrcd.assessedwpifors66 as wpi_assessed_pct,
    cast(asrcd.s66receiveddate_icare as date) as s66_received_dt,
    asrcd.bhlresult_icare as binaural_hearing_loss_pct,
    docassess.reviewer_icareid as med_assess_src_review_user_id,
    case
        when row_number() over (
            partition by clm.claimnumber, asrcd.id
            order by docassess.assessmentdate_icare desc, docassess.createtime desc
        ) = 1 then 'Y'
        else 'N'
    end as latest_med_assess_review_ind,
    case
        when row_number() over (
            partition by clm.claimnumber
            order by asrcd.s66receiveddate_icare desc, asrcd.createtime desc
        ) = 1 then 'Y'
        else 'N'
    end as latest_wpi_record_ind,
    case
        when count(clm.claimnumber) over (partition by clm.claimnumber) > 1 then 'Y'
        else 'N'
    end as multiple_wpi_ind,
    current_date() as extract_date,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_exposure exps
    on exps.claimid = clm.id

inner join ccx_wpiassessment_icare asmt
    on asmt.exposureid = exps.id

inner join ccx_wpiassessrecord_icare asrcd
    on asrcd.wpiassessment_icareid = asmt.id

left join ccx_wpidoctorassessment_icare docassess
    on docassess.wpiassessrecord_icareid = asrcd.id

left join cctl_wpisettlementtype_icare dim_settlementtype
    on dim_settlementtype.id = asrcd.settlementtype_icare
)
select 
    claim_sk,
    src_system_cd,
    claim_nbr,
    src_wpi_assess_record_id,
    wpi_claim_type,
    wpi_status,
    wpi_result_pct,
    wpi_claimed_pct,
    wpi_offered_pct,
    wpi_assessed_pct,
    s66_received_dt,
    binaural_hearing_loss_pct,
    med_assess_src_review_user_id,
    latest_med_assess_review_ind,
    latest_wpi_record_ind,
    multiple_wpi_ind,
    file_ingestion_timestamp,
    cast({{ dbt_utils.generate_surrogate_key([
        'src_system_cd',
        'claim_nbr',
        'src_wpi_assess_record_id',
        'wpi_claim_type',
        'wpi_status',
        'multiple_wpi_ind'
    ]) }} as varchar(150)) as claim_wpi_sk

from
    cte_join
{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Aspire - original table materialization
2026-04-20      1.0                             Converted to incremental with merge strategy

-#}

{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge',
    tags=['integrated', 'integrated_ni', 'legacy', 'business_critical']
  )
}}

{#
  Source: 01_ACCIDENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A01
  TBL_NM: MSC_QLK_ASPIRE_ACCIDENT
-#}

with base_cc_claim as (
    select
        id,
        claimnumber,
        locationcodeid,
        claimworkcompid,
        updatetime,
        retired,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
    {% if is_incremental() %}
        AND file_ingestion_timestamp > (select max(file_ingestion_timestamp) from {{ this }})
    {% endif %}
),

base_cc_incident as (
    select
        id,
        claimid,
        claimincident,
        subtype,
        mechanismofinjurydesc_icare,
        retired,
        severity
    from {{ ref('v_cc_incident_current') }}
    where
        retired = 0
        and claimincident = 1
),

base_cctl_incident as (
    select
        id,
        typecode
    from {{ ref('v_cctl_incident_current') }}
    where lower(typecode) = 'injuryincident'
),

base_cc_policylocation as (
    select
        id,
        addressid,
        retired
    from {{ ref('v_cc_policylocation_current') }}
    where retired = 0
),

base_cc_address as (
    select
        id,
        COALESCE(addressline1,'') as addressline1,
        COALESCE(addressline2,'') as addressline2,
        COALESCE(addressline3,'') as addressline3,
        city,
        postalcode,
        retired
    from {{ ref('v_cc_address_current') }}
    where retired = 0
),

base_cc_workcomp as (
    select
        id,
        accidentlocationtype_icare,
        retired
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

base_cctl_accidentloctype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_accidentloctype_icare_current') }}
),

base_cc_subrogationsummary as (
    select
        id,
        claimid,
        retired
    from {{ ref('v_cc_subrogationsummary_current') }}
    where retired = 0
),

base_cc_subrogation as (
    select
        id,
        subrogationsummaryid,
        retired
    from {{ ref('v_cc_subrogation_current') }}
    where retired = 0
),

{# ============================================================
   BUSINESS LOGIC CTEs
   ============================================================ #}

cte_subrogation as (
    select distinct subrosumm.claimid
    from base_cc_subrogationsummary as subrosumm
    inner join base_cc_subrogation as subro
        on subrosumm.id = subro.subrogationsummaryid
),

cte_join as (
    select
        cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
        clm.source_system,
        clm.id as src_claim_id,
        clm.claimnumber as claim_nbr,
        inc.mechanismofinjurydesc_icare as toocs_mechanism_of_injury_desc,
        dimacc.typecode as accident_location_type_cd,
        dimacc.name as accident_location_type_desc,
        clm.updatetime as src_updated_dttm,
        cast(clm.updatetime as date) as src_updated_dt,
        severity,
        file_ingestion_timestamp,
        iff(subro.claimid is not null, 'Y', 'N') as recovery_investigation_ind,
        concat(
            rtrim(concat(
                pollocaddr.addressline1, ' ',
                pollocaddr.addressline2, ' ',
                pollocaddr.addressline3
            )),
            ' ', pollocaddr.city, ' ',
            pollocaddr.postalcode
        ) as policy_location_addr
    from base_cc_claim as clm
    inner join base_cc_incident as inc
        on clm.id = inc.claimid
    inner join base_cctl_incident as cctl_incident
        on inc.subtype = cctl_incident.id
    left join base_cc_policylocation as polloc
        on clm.locationcodeid = polloc.id
    inner join base_cc_address as pollocaddr
        on polloc.addressid = pollocaddr.id
    left join base_cc_workcomp as wrkcomp
        on clm.claimworkcompid = wrkcomp.id
    inner join base_cctl_accidentloctype_icare as dimacc
        on wrkcomp.accidentlocationtype_icare = dimacc.id
    left join cte_subrogation as subro
        on clm.id = subro.claimid
)

select
    claim_sk,
    source_system,
    claim_nbr,
    cast(recovery_investigation_ind as varchar(1)) as recovery_investigation_ind,
    cast(policy_location_addr as varchar(350)) as policy_location_addr,
    toocs_mechanism_of_injury_desc,
    accident_location_type_cd,
    accident_location_type_desc,
    src_updated_dttm,
    src_updated_dt,
    severity,
    file_ingestion_timestamp
from cte_join
qualify row_number() over (partition by claim_sk order by file_ingestion_timestamp desc) = 1

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

with cc_claim as (
    select
        id,
        claimnumber,
        locationcodeid,
        claimworkcompid,
        source_system,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
    where retired = 0
),

cc_incident as (
    select
        id,
        claimid,
        claimincident,
        subtype,
        mechanismofinjurydesc_icare,
        retired
    from {{ ref('v_cc_incident_current') }}
    where retired = 0
),

cctl_incident as (
    select
        id,
        typecode
    from {{ ref('v_cctl_incident_current') }}
),

cc_policylocation as (
    select
        id,
        addressid
    from {{ ref('v_cc_policylocation_current') }}
    where retired = 0
),

cc_address as (
    select
        id,
        addressline1,
        addressline2,
        addressline3,
        city,
        postalcode
    from {{ ref('v_cc_address_current') }}
    where retired = 0
),

cc_workcomp as (
    select
        id,
        accidentlocationtype_icare
    from {{ ref('v_cc_workcomp_current') }}
    where retired = 0
),

cctl_accidentloctype_icare as (
    select
        id,
        typecode,
        name
    from {{ ref('v_cctl_accidentloctype_icare_current') }}
),

cc_subrogationsummary as (
    select
        id,
        claimid
    from {{ ref('v_cc_subrogationsummary_current') }}
    where retired = 0
),

cc_subrogation as (
    select
        id,
        subrogationsummaryid
    from {{ ref('v_cc_subrogation_current') }}
    where retired = 0
),

subro as (
    select distinct
        subrosum.claimid
    from cc_subrogationsummary subrosum
    inner join cc_subrogation sub
        on sub.subrogationsummaryid = subrosum.id
)

select distinct
    cast({{ dbt_utils.generate_surrogate_key([
        'clm.source_system',
        'clm.claimnumber'
    ]) }} as varchar(150)) as claim_sk,
    clm.source_system as source_system,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    case
        when subro.claimid is not null then 'Y'
        else 'N'
    end as recovery_investigation_ind,
    concat(
        rtrim(concat(
            coalesce(pollocaddr.addressline1, ''), ' ',
            coalesce(pollocaddr.addressline2, ''), ' ',
            coalesce(pollocaddr.addressline3, '')
        )),
        ' ', coalesce(pollocaddr.city, ''),
        ' ', coalesce(pollocaddr.postalcode, '')
    ) as policy_location_addr,
    inc.mechanismofinjurydesc_icare as toocs_mechanism_of_injury_desc,
    dimacc.typecode as accident_location_type_cd,
    dimacc.name as accident_location_type_desc,
    clm.file_ingestion_timestamp

from cc_claim clm

inner join cc_incident inc
    on clm.id = inc.claimid
    and inc.claimincident = 1

inner join cctl_incident cctl_inc
    on cctl_inc.id = inc.subtype
    and lower(cctl_inc.typecode) = 'injuryincident'

left join cc_policylocation polloc
    on clm.locationcodeid = polloc.id

inner join cc_address pollocaddr
    on polloc.addressid = pollocaddr.id

left join cc_workcomp wrkcomp
    on clm.claimworkcompid = wrkcomp.id

inner join cctl_accidentloctype_icare dimacc
    on wrkcomp.accidentlocationtype_icare = dimacc.id

left join subro
    on subro.claimid = clm.id

{% if is_incremental() %}
where clm.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}

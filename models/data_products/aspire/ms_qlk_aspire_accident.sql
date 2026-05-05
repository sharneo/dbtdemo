{{
  config(
    materialized='incremental',
    unique_key='claim_sk',
    incremental_strategy='merge'
  )
}}

{#
  Source: 01_ACCIDENT.sas
  Original SAS Target: ASPIRE.GW_OUTPUT_A01
  TBL_NM: MSC_QLK_ASPIRE_ACCIDENT
#}

with cc_claim as (
    select
        id,
        claimnumber,
        locationcodeid,
        claimworkcompid,
        retired,
        file_ingestion_timestamp
    from {{ ref('v_cc_claim_current') }}
),
cc_incident as (
    select
        id,
        claimid,
        subtype,
        retired,
        claimincident,
        mechanismofinjury_icare
    from {{ ref('v_cc_incident_current') }}
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
        addressid,
        retired
    from {{ ref('v_cc_policylocation_current') }}
),
cc_address as (
    select
        id,
        addressline1,
        addressline2,
        addressline3,
        city,
        postalcode,
        retired
    from {{ ref('v_cc_address_current') }}
),
cc_workcomp as (
    select
        id,
        accidentlocationtype_icare,
        retired
    from {{ ref('v_cc_workcomp_current') }}
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
        claimid,
        retired
    from {{ ref('v_cc_subrogationsummary_current') }}
),
cc_subrogation as (
    select
        subrogationsummaryid,
        retired
    from {{ ref('v_cc_subrogation_current') }}
)

select distinct
    md5('GWCC' || clm.claimnumber) as claim_sk,
    'GWCC' as source_system,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    case
        when subro.claimid is not null then 'Y'
        else 'N'
    end as recovery_investigation_ind,
    concat(
        rtrim(
            concat(pollocaddr.addressline1, ' ',
                   pollocaddr.addressline2, ' ',
                   pollocaddr.addressline3
            )
        ),
        ' ', pollocaddr.city, ' ',
        pollocaddr.postalcode
    ) as policy_location_addr,
    inc.mechanismofinjury_icare as toocs_mechanism_if_injury_desc,
    dimacc.typecode as accident_location_type_cd,
    dimacc.name as accident_location_type_desc,
    clm.file_ingestion_timestamp

from cc_claim clm

join cc_incident inc
    on clm.id = inc.claimid
    and inc.claimincident = 1
    and inc.retired = 0

join cctl_incident cctl_incident
    on cctl_incident.id = inc.subtype
    and lower(cctl_incident.typecode) = 'injuryincident'

left join cc_policylocation polloc
    on clm.locationcodeid = polloc.id
    and polloc.retired = 0

join cc_address pollocaddr
    on polloc.addressid = pollocaddr.id
    and pollocaddr.retired = 0

left join cc_workcomp wrkcomp
    on clm.claimworkcompid = wrkcomp.id
    and wrkcomp.retired = 0

join cctl_accidentloctype_icare dimacc
    on wrkcomp.accidentlocationtype_icare = dimacc.id

left join (
    select distinct subrosumm.claimid
    from cc_subrogationsummary subrosumm
    inner join cc_subrogation subro
        on subro.subrogationsummaryid = subrosumm.id
        and subro.retired = 0
    where subrosumm.retired = 0
) subro
    on subro.claimid = clm.id

where clm.retired = 0

{% if is_incremental() %}
and clm.file_ingestion_timestamp >= (select max(file_ingestion_timestamp) from {{ this }})
{% endif %}

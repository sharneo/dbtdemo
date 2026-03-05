with cte_claim as (
    select
        id,
        claimnumber,
        locationcodeid,
        claimworkcompid
    from {{ ref('vw_cc_claim') }}
    where retired = 0
),

cte_incident as (
    select
        claimid,
        subtype,
        mechanismofinjurydesc_icare
    from {{ ref('vw_cc_incident') }}
    where claimincident = 1
      and retired = 0
),

cte_incident_type as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_incident') }}
    where lower(typecode) = 'injuryincident'
),

cte_policy_location as (
    select
        id,
        addressid
    from {{ ref('vw_cc_policylocation') }}
    where retired = 0
),

cte_address as (
    select
        id,
        addressline1,
        addressline2,
        addressline3,
        city,
        postalcode
    from {{ ref('vw_cc_address') }}
    where retired = 0
),

cte_workcomp as (
    select
        id,
        accidentlocationtype_icare
    from {{ ref('vw_cc_workcomp') }}
    where retired = 0
),

cte_accident_loc_type as (
    select
        id,
        typecode,
        name
    from {{ ref('vw_cctl_accidentloctype_icare') }}
),

cte_subrogation as (
    select distinct
        subrosumm.claimid
    from {{ ref('vw_cc_subrogationsummary') }} subrosumm
    inner join {{ ref('vw_cc_subrogation') }} subro
        on subro.subrogationsummaryid = subrosumm.id
        and subro.retired = 0
    where subrosumm.retired = 0
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['\'GWCC\'', 'clm.claimnumber']) }} as claim_sk,
    'GWCC' as src_system_cd,
    clm.id as src_claim_id,
    clm.claimnumber as claim_nbr,
    case when subro.claimid is not null then 'Y' else 'N' end as recovery_investigation_ind,
    concat(
        trim(concat(pollocaddr.addressline1, ' ', pollocaddr.addressline2, ' ', pollocaddr.addressline3)),
        ' ', pollocaddr.city, ' ', pollocaddr.postalcode
    ) as policy_location_addr,
    inc.mechanismofinjurydesc_icare as toocs_mechanism_if_injury_desc,
    dimacc.typecode as accident_location_type_cd,
    dimacc.name as accident_location_type_desc
from cte_claim clm
join cte_incident inc on clm.id = inc.claimid
join cte_incident_type cctl_incident on cctl_incident.id = inc.subtype
left join cte_policy_location polloc on clm.locationcodeid = polloc.id
join cte_address pollocaddr on polloc.addressid = pollocaddr.id
left join cte_workcomp wrkcomp on clm.claimworkcompid = wrkcomp.id
join cte_accident_loc_type dimacc on wrkcomp.accidentlocationtype_icare = dimacc.id
left join cte_subrogation subro on subro.claimid = clm.id
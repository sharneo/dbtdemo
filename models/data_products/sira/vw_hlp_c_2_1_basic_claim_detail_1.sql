
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates Helper View of the C2_1_BASIC_DETAIL

-#}

{{ config(
    materialized='view',
    tags=["sira", "business_critical","hlper_views"]
) }}

with cc_claim as (
    select
        claimnumber,
        id,
        reporteddate,
        createtime,
        lossdate,
        datemade_icare,
        losslocationid,
        policyid,
        claimworkcompid,
        sharedclaim_icare,
        branchinsurer_icare,
        lodgingagent_icare,
        claimsagent_icare,
        managingentity_icare,
        retired
    from {{ ref('vw_cc_claim') }}
),

claim_wic_icare as (
    select
        ownerid,
        foreignentityid,
        publicid
    from {{ ref('vw_ccx_claimwicicare') }}
),

ccx_wic_icare as (
    select
        id,
        retired,
        code,
        tariffrate
    from {{ ref('vw_ccx_wic_icare') }}
    where retired = 0
),

shared_claim as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_sharedclaim_icare') }}
    where retired = 0
),

insurer_branch as (
    select
        id,
        name
    from {{ ref('vw_cctl_insurerbranch_icare') }}
    where retired = 0
),

cc_policy as (
    select
        id,
        retired,
        policytype,
        policynumber,
        manualverify_icare,
        legacypolicynumber_icare,
        verified
    from {{ ref('vw_cc_policy') }}
    where retired = 0
),

cctl_policytype as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_policytype') }}
),

policy_period as (
    select
        publicid,
        policynumber,
        legacypolicynumber_icare,
        status,
        periodstart
    from {{ ref('vw_pc_policyperiod') }}
),

cc_incident as (
    select
        claimid,
        retired,
        claimincident,
        dutystatus_icare,
        id,
        severity,
        mechanismofinjurycode_icare,
        breakdownagencycode_icare,
        contactcompletedate_icare,
        significantinjurydate_icare,
        natureofinjurycode_icare,
        deceaseddate_icare,
        agencyofinjurycode_icare
    from {{ ref('vw_cc_incident') }}
    where retired = 0 and claimincident = 1
),

duty_status as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_dutystatus_icare') }}
    where retired = 0
),

cc_policy_location as (
    select
        policyid,
        id,
        retired,
        addressid
    from {{ ref('vw_cc_policylocation') }}
    where retired = 0
),

cc_address as (
    select
        country,
        id,
        retired,
        addressline1,
        addressline2,
        addressline3,
        postalcode,
        city,
        description
    from {{ ref('vw_cc_address') }}
    where retired = 0
),

country_ref as (
    select
        id,
        name
    from {{ ref('vw_cctl_country') }}
),

cc_claimempdata as (
    select
        ownerid,
        id,
        foreignentityid
    from {{ ref('vw_cc_claimempdata') }}
),

cc_employmentdata as (
    select
        id,
        retired,
        employmentstatus,
        trainingstatus_icare,
        industrycode_icareid,
        workplacesize_icare,
        hoursworkedweek_icare,
        lpad(floor(hoursworkedweek_icare)::varchar, 2, '0') as hoursworkedweek_icare_hours,
        lpad(round((hoursworkedweek_icare % 1) * 60)::int::varchar, 2, '0') as hoursworkedweek_icare_minutes
    from {{ ref('vw_cc_employmentdata') }}
),

ccx_industrycode_icare as (
    select
        id,
        retired,
        industrycode
    from {{ ref('vw_ccx_industrycode_icare') }}
    where retired = 0
),

employment_status_type as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_employmentstatustype') }}
),

training_status_icare as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_trainingstatus_icare') }}
),

cc_workcomp as (
    select
        id,
        retired,
        accidentlocationtype_icare
    from {{ ref('vw_cc_workcomp') }}
    where retired = 0
),

accident_loc_type as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_accidentloctype_icare') }}
    where retired = 0
),

toocs_bloi_connector as (
    select
        injuryincident_icareid,
        id,
        retired,
        injurycode,
        selectedasprimary
    from {{ ref('vw_ccx_toocsbloiconnector_icare') }}
    where retired = 0
),

severity_type as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_severitytype') }}
),

lodging_agent as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_claimagent_icare') }}
),

managing_agent as (
    select
        id,
        typecode
    from {{ ref('vw_cctl_claimagent_icare') }}
),

claimant as (
    select
        claimid,
        homephone,
        addressline1,
        city,
        postalcode,
        gender,
        dateofbirth,
        abslanguagecode_icare,
        occupation,
        occupationdetails_icareid,
        unitcode,
        cntryname,
        gendername
    from {{ ref('vw_sira_claimant') }}
),

dependents as (
    select
        claimid,
        sum(childunder16) as childunder16,
        sum(other) as other
    from {{ ref('vw_sira_dependent') }}
    group by claimid
),

piawe as (
    select
        claimnumber,
        claimid,
        typecode,
        totalweekspaid,
        pre_injuiry_average_weekly,
        ordinary_earnings,
        shift_allowance,
        overtime,
        createtime,
        effectivedate_icare,
        rpstartdate,
        rpenddate,
        draft,
        exposureid
    from {{ ref('vw_sira_piawe') }}
),

managing_entity as (
    select
        id,
        role,
        retired,
        code
    from {{ ref('vw_ccx_managingentity_icare') }}
),

insurer_control as (
    select
        '016' as insurer_number,
        'Y' as active_ind
),

claim_rule as (
    select
        '1023290702' as claim_number,
        '1' as rule_id
),

base as (
    select
        '1223' as claim_sk,
        2 as c_2_1_1_record_type,
        1 as c_2_1_3_record_identifier,
        ib.name as c_2_1_7_branch_of_insurer_handling_claim,
        '0000' as c_2_1_36_workplace_industry_asic,
        coalesce(idc.industrycode, '0000')::char(4) as c_2_1_37_workplace_industry_anzsic,
        cctl_poltype.typecode as policy_type,
        coalesce(mag.typecode, lag.typecode)::varchar(5) as insurer_number,
        'N' as sira_processing_ind,
        c.claimnumber || coalesce(lag.typecode, '701') as c_2_1_2_claim_number,
        coalesce(sc.typecode, 0) as c_2_1_5_shared_claim_code,
        coalesce(right(ma_ccx.code, 7), 'NA') as c_2_1_6_error_report_target,
        case
            when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD')
            else to_char(c.createtime, 'YYYYMMDD')
        end as c_2_1_8_date_claim_entered_on_insurers_system,
        coalesce(to_char(c.datemade_icare, 'YYYYMMDD'), '00000000') as c_2_1_9_date_claim_made,
        case
            when
                p.manualverify_icare = 1
                then
                    coalesce(
                        p.legacypolicynumber_icare,
                        coalesce(pp.legacypolicynumber_icare, coalesce(p.policynumber, pp.policynumber) || '701')
                    )
            when p.verified = 1 and p.legacypolicynumber_icare is not null then p.legacypolicynumber_icare
            when p.verified = 1 and pp.legacypolicynumber_icare is not null then pp.legacypolicynumber_icare
            when p.verified = 1 and p.policynumber is not null then p.policynumber || '701'
            when p.verified = 1 and pp.policynumber is not null then pp.policynumber || '701'
            when p.verified = 0 and pp.policynumber is not null then pp.policynumber || '701'
            else coalesce(p.policynumber || '701', 'UNVERIFIED')
        end as c_2_1_10_policyholder_identification_number,
        case when wic.code is null then coalesce(tariffrate::varchar, '000') else '000' end
            as c_2_1_12_tariff_rate_number,
        coalesce(claimant.addressline1, '') as c_2_1_16_claimant_address_street_information,
        upper(coalesce(claimant.city, '')) as c_2_1_17_claimant_address_locality_name,
        lpad(coalesce(claimant.postalcode, '0000')::varchar, 4, '0') as c_2_1_18_claimant_address_postcode,
        coalesce(claimant.gender, '') as c_2_1_19_claimants_gender_code,
        coalesce(to_char(claimant.dateofbirth, 'YYYYMMDD'), '00000000') as c_2_1_20_claimants_date_of_birth,
        coalesce(claimant.abslanguagecode_icare::varchar, '0000') as c_2_1_22_claimants_language_code,
        coalesce(claimant.unitcode, '0000') as c_2_1_24_claimants_occupation_code,
        lpad(coalesce(dependents.childunder16, 0)::varchar, 2, '0') as c_2_1_25_claimants_dependants_children,
        lpad(coalesce(dependents.other, 0)::varchar, 2, '0') as c_2_1_26_claimants_dependants_other,
        case coalesce(est.typecode, '0')
            when '4' then '1' when '5' then '1' when '8' then '1'
            when '6' then '2' when '7' then '2'
            when '3' then '3'
            else '1'
        end as c_2_1_28_permanent_employment_code,
        coalesce(tsi.typecode, 0) as c_2_1_29_training_status_code,
        coalesce(ed.hoursworkedweek_icare_hours || ed.hoursworkedweek_icare_minutes, '0000')
            as c_2_1_30_hours_worked_per_week,
        coalesce('+' || lpad((dolls.pre_injuiry_average_weekly * 100)::int::varchar, 7, '0'), '+0000000')
            as c_2_1_31_workers_pre_injury_average_weekly_earnings,
        coalesce(ds.typecode, '0') as c_2_1_32_duty_status_code,
        upper(case
            when
                coalesce(ad.country, 0) != 10015
                then
                    trim(
                        coalesce(ad.addressline1, '')
                        || ' '
                        || coalesce(ad.addressline2, '')
                        || ' '
                        || coalesce(ad.city, '')
                        || ' '
                        || coalesce(ad.postalcode, '')
                        || ' '
                        || coalesce(cntry.name, '')
                    )
            else
                trim(
                    coalesce(ad.addressline1, '')
                    || ' '
                    || coalesce(ad.addressline2, '')
                    || ' '
                    || coalesce(ad.addressline3, '')
                )
        end) as c_2_1_33_workplace_address_street_information,
        upper(case when coalesce(ad.country, 0) != 10015 then 'OS' else coalesce(ad.city, '') end)
            as c_2_1_34_workplace_address_locality_name,
        case
            when coalesce(ad.country, 0) != 10015 then '0000' else
                lpad(coalesce(ad.postalcode, '0000')::varchar, 4, '0')
        end as c_2_1_35_workplace_address_postcode,
        lpad(coalesce(ed.workplacesize_icare, 0)::varchar, 5, '0') as c_2_1_38_workplace_size,
        coalesce(alt.typecode, '99') as c_2_1_39_accident_location_code,
        case coalesce(alt.typecode, '99')
            when '00' then 'NA'
            when '01' then 'NA'
            else case when coalesce(l.country, 0) != 10015 then 'OS' else coalesce(l.description, 'NA') end
        end as c_2_1_40_accident_location_description,
        upper(case coalesce(alt.typecode, '99')
            when '00' then 'NA'
            when '01' then 'NA'
            else case when coalesce(l.country, 0) != 10015 then 'OS' else coalesce(l.city, '0000') end
        end) as c_2_1_41_accident_location_locality_name,
        case coalesce(alt.typecode, '99')
            when '00' then '0000'
            when '01' then '0000'
            else
                case
                    when coalesce(l.country, 0) != 10015 then '0000' else
                        lpad(coalesce(l.postalcode, '0000')::varchar, 4, '0')
                end
        end as c_2_1_42_accident_location_postcode,
        to_char(c.lossdate, 'YYYYMMDD') as c_2_1_43_date_of_injury,
        to_char(coalesce(c.lossdate, '1900-01-01'), 'HH24MI') as c_2_1_44_time_of_injury,
        case
            when
                case
                    when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else
                        to_char(c.createtime, 'YYYYMMDD')
                end
                < '19910701'
                then '000'
            else coalesce(i.natureofinjurycode_icare, '999')
        end as c_2_1_45_nature_of_injury_disease_code,
        case
            when
                case
                    when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else
                        to_char(c.createtime, 'YYYYMMDD')
                end
                < '19910701'
                then '000'
            else coalesce(tbc.injurycode, '900')
        end as c_2_1_46_bodily_location_of_injury_disease_code,
        coalesce(st.typecode, '0') as c_2_1_49_result_of_injury_code,
        coalesce(to_char(i.deceaseddate_icare, 'YYYYMMDD'), '00000000') as c_2_1_50_date_deceased,
        coalesce(wic.code::varchar, '000000') as c_2_1_52_workers_compensation_industry_classification_wic_rate_number,
        iff(c.retired = 0, 'N', 'Y') as claim_retired
    from cc_claim as c
    left join claim_wic_icare as cw on c.id = cw.ownerid
    left join ccx_wic_icare as wic on cw.foreignentityid = wic.id
    left join shared_claim as sc on c.sharedclaim_icare = sc.id
    left join insurer_branch as ib on c.branchinsurer_icare = ib.id
    left join cc_policy as p on c.policyid = p.id
    left join cctl_policytype as cctl_poltype on p.policytype = cctl_poltype.id
    left join policy_period as pp on p.policynumber = pp.policynumber
    left join cc_incident as i on c.id = i.claimid
    left join duty_status as ds on i.dutystatus_icare = ds.id
    left join cc_policy_location as pl on p.id = pl.policyid
    left join cc_address as ad on pl.addressid = ad.id
    left join country_ref as cntry on ad.country = cntry.id
    left join cc_claimempdata as ce on c.id = ce.ownerid
    left join cc_employmentdata as ed on ce.foreignentityid = ed.id and ed.retired = 0
    left join ccx_industrycode_icare as idc on ed.industrycode_icareid = idc.id
    left join employment_status_type as est on ed.employmentstatus = est.id
    left join training_status_icare as tsi on ed.trainingstatus_icare = tsi.id
    left join cc_workcomp as wc on c.claimworkcompid = wc.id
    left join accident_loc_type as alt on wc.accidentlocationtype_icare = alt.id
    left join cc_address as l on c.losslocationid = l.id and l.retired = 0
    left join toocs_bloi_connector as tbc on i.id = tbc.injuryincident_icareid and tbc.selectedasprimary = 1
    left join severity_type as st on i.severity = st.id
    left join lodging_agent as lag on c.lodgingagent_icare = lag.id
    left join managing_agent as mag on c.claimsagent_icare = mag.id
    left join claimant on c.id = claimant.claimid
    left join dependents on c.id = dependents.claimid
    left join piawe as dolls on c.id = dolls.claimid
    left join
        managing_entity as ma_ccx
        on c.managingentity_icare = ma_ccx.id and ma_ccx.retired = 0 and ma_ccx.role in ('10001', '10002', '10004')
    inner join insurer_control as ic on mag.typecode = ic.insurer_number
    left join claim_rule as cr on cr.claim_number = c.claimnumber || lag.typecode
    where coalesce(c.lodgingagent_icare, c.claimsagent_icare) is not null
)

select * from base

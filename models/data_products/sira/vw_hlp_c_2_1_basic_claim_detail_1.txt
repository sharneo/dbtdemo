{{ config(
    materialized='view',
    schema='sira'
) }}

with cc_claim as 
(
SELECT claimnumber,
       claim_sk,
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
from {{ ref('vw_cc_claim_current') }}
),
claim_wic_icare as (
    select  ownerid, 
            foreignentityid, 
            publicid
    from {{ ref('vw_ccx_claimwicicare_current') }}
),

ccx_wic_icare as (
    select id, 
           retired, 
           code, 
           tariffrate
    from {{ ref('vw_ccx_wic_icare_current') }}
    where retired = 0
),
shared_claim as (
    select id, 
            typecode
    from {{ ref('vw_cctl_sharedclaim_icare_current') }}
    where retired = 0
),

insurer_branch as (
    select id, 
           name
    from {{ ref('vw_cctl_insurerbranch_icare_current') }}
    where retired = 0
),

cc_policy as (
    select id, 
           retired, 
           policytype, 
           policynumber, 
           manualverify_icare, 
           legacypolicynumber_icare, 
           verified
    from {{ ref('vw_cc_policy_current') }}
    where retired = 0
),

cctl_policytype as (
    select id, 
            typecode
    from {{ ref('vw_cctl_policytype_current') }}
),

policy_period as (
    select 
            publicid, 
            policynumber, 
            legacypolicynumber_icare, 
            status, 
            periodstart
    from {{ ref('vw_pc_policyperiod_current') }}
),
cc_incident as (
    select
                claimid, 
                retired, 
                claimincident, 
                dutystatus_icare, 
                id, 
                severity,
        mechanismofinjurycode_icare, breakdownagencycode_icare, contactcompletedate_icare,
        significantinjurydate_icare, natureofinjurycode_icare, deceaseddate_icare, agencyofinjurycode_icare
    from {{ ref('vw_cc_incident_current') }}
    where retired = 0 and claimincident = 1
),

duty_status as (
    select id, typecode
    from {{ ref('vw_cctl_dutystatus_icare_current') }}
    where retired = 0
),

cc_policy_location as (
    select policyid, id, retired, addressid
    from {{ ref('vw_cc_policylocation_current') }}
    where retired = 0
),

cc_address as (
    select country, id, retired, addressline1, addressline2, addressline3, postalcode, city, description
    from {{ ref('vw_cc_address_current') }}
    where retired = 0
),

country_ref as (
    select id, name
    from {{ ref('vw_cctl_country_current') }}
),

cc_claimempdata as (
    select ownerid, id, foreignentityid
    from {{ ref('vw_cc_claimempdata_current') }}
),

cc_employmentdata as (
    select
        id, retired, employmentstatus, trainingstatus_icare, industrycode_icareid,
        workplacesize_icare, hoursworkedweek_icare,
        lpad(floor(hoursworkedweek_icare)::varchar, 2, '0') as hoursworkedweek_icare_hours,
        lpad(round((hoursworkedweek_icare % 1) * 60)::int::varchar, 2, '0') as hoursworkedweek_icare_minutes
    from {{ ref('vw_cc_employmentdata_current') }}
),

ccx_industrycode_icare as (
    select id, retired, industrycode
    from {{ ref('vw_ccx_industrycode_icare_current') }}
    where retired = 0
),

employment_status_type as (
    select id, typecode
    from {{ ref('vw_cctl_employmentstatustype_current') }}
),

training_status_icare as (
    select id, typecode
    from {{ ref('vw_cctl_trainingstatus_icare_current') }}
),

cc_workcomp as (
    select id, retired, accidentlocationtype_icare
    from {{ ref('vw_cc_workcomp_current') }}
    where retired = 0
),

accident_loc_type as (
    select id, typecode
    from {{ ref('vw_cctl_accidentloctype_icare_current') }}
    where retired = 0
),

toocs_bloi_connector as (
    select injuryincident_icareid, id, retired, injurycode, selectedasprimary
    from {{ ref('vw_ccx_toocsbloiconnector_icare_current') }}
    where retired = 0
),

severity_type as (
    select id, typecode
    from {{ ref('vw_cctl_severitytype_current') }}
),

lodging_agent as (
    select id, typecode
    from {{ ref('vw_cctl_claimagent_icare_current') }}
),

managing_agent as (
    select id, typecode
    from {{ ref('vw_cctl_claimagent_icare_current') }}
),
claimant as (
    select
        claimid, homephone, addressline1, city, postalcode, gender,
        dateofbirth, abslanguagecode_icare, occupation, occupationdetails_icareid,
        unitcode, cntryname, gendername
    from {{ ref('vw_sira_claimant') }}
),
dependents as (
    select claimid, sum(childunder16) as childunder16, sum(other) as other
    from {{ ref('vw_sira_dependent') }}
    group by claimid
),

piawe as (
    select
        claimnumber, claimid, typecode, totalweekspaid, pre_injuiry_average_weekly,
        ordinary_earnings, shift_allowance, overtime, createtime, effectivedate_icare,
        piawedelactivated_icare, rpstartdate, rpenddate, draft, exposureid
    from {{ ref('vw_sira_piawe') }}
),
managing_entity as (
    select id, role, retired, code
    from {{ ref('vw_ccx_managingentity_icare_current') }}
),
insurer_control as (
    select 
        '016' as insurer_number,
        'Y' as active_ind
),
claim_rule as (
    select claim_number, rule_id
    from {{ ref('vw_claim_rule_current') }}
    where rule_id = 1
),

base as (
    select
        c.claim_sk,
        dte.submission_period::varchar || '^' || c.claimnumber as bcd1_key,
        dte.submission_period,
        2 as c_2_1_1_record_type,
        c.claimnumber || coalesce(lag.typecode, '701') as c_2_1_2_claim_number,
        1 as c_2_1_3_record_identifier,
        coalesce(sc.typecode, 0) as c_2_1_5_shared_claim_code,
        coalesce(right(ma_ccx.code, 7), 'NA') as c_2_1_6_error_report_target,
        ib.name as c_2_1_7_branch_of_insurer_handling_claim,
        case
            when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD')
            else to_char(c.createtime, 'YYYYMMDD')
        end as c_2_1_8_date_claim_entered_on_insurers_system,
        coalesce(to_char(c.datemade_icare, 'YYYYMMDD'), '00000000') as c_2_1_9_date_claim_made,
        case
            when p.manualverify_icare = 1 then coalesce(p.legacypolicynumber_icare, coalesce(pp.legacypolicynumber_icare, coalesce(p.policynumber, pp.policynumber) || '701'))
            when p.verified = 1 and p.legacypolicynumber_icare is not null then p.legacypolicynumber_icare
            when p.verified = 1 and pp.legacypolicynumber_icare is not null then pp.legacypolicynumber_icare
            when p.verified = 1 and p.policynumber is not null then p.policynumber || '701'
            when p.verified = 1 and pp.policynumber is not null then pp.policynumber || '701'
            when p.verified = 0 and pp.policynumber is not null then pp.policynumber || '701'
            else coalesce(p.policynumber || '701', 'UNVERIFIED')
        end as c_2_1_10_policyholder_identification_number,
        case when wic.code is null then coalesce(tariffrate::varchar, '000') else '000' end as c_2_1_12_tariff_rate_number,
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
        coalesce(ed.hoursworkedweek_icare_hours || ed.hoursworkedweek_icare_minutes, '0000') as c_2_1_30_hours_worked_per_week,
        coalesce('+' || lpad((dolls.pre_injuiry_average_weekly * 100)::int::varchar, 7, '0'), '+0000000') as c_2_1_31_workers_pre_injury_average_weekly_earnings,
        coalesce(ds.typecode, '0') as c_2_1_32_duty_status_code,
        upper(case
            when coalesce(ad.country, 0) != 10015 then trim(coalesce(ad.addressline1, '') || ' ' || coalesce(ad.addressline2, '') || ' ' || coalesce(ad.city, '') || ' ' || coalesce(ad.postalcode, '') || ' ' || coalesce(cntry.name, ''))
            else trim(coalesce(ad.addressline1, '') || ' ' || coalesce(ad.addressline2, '') || ' ' || coalesce(ad.addressline3, ''))
        end) as c_2_1_33_workplace_address_street_information,
        upper(case when coalesce(ad.country, 0) != 10015 then 'OS' else coalesce(ad.city, '') end) as c_2_1_34_workplace_address_locality_name,
        case when coalesce(ad.country, 0) != 10015 then '0000' else lpad(coalesce(ad.postalcode, '0000')::varchar, 4, '0') end as c_2_1_35_workplace_address_postcode,
        '0000' as c_2_1_36_workplace_industry_asic,
        coalesce(idc.industrycode, '0000')::char(4) as c_2_1_37_workplace_industry_anzsic,
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
            else case when coalesce(l.country, 0) != 10015 then '0000' else lpad(coalesce(l.postalcode, '0000')::varchar, 4, '0') end
        end as c_2_1_42_accident_location_postcode,
        to_char(c.lossdate, 'YYYYMMDD') as c_2_1_43_date_of_injury,
        to_char(coalesce(c.lossdate, '1900-01-01'), 'HH24MI') as c_2_1_44_time_of_injury,
        case
            when case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end < '19910701' then '000'
            else coalesce(i.natureofinjurycode_icare, '999')
        end as c_2_1_45_nature_of_injury_disease_code,
        case
            when case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end < '19910701' then '000'
            else coalesce(tbc.injurycode, '900')
        end as c_2_1_46_bodily_location_of_injury_disease_code,
        case
            when case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end < '19910701' then '00'
            else coalesce(i.mechanismofinjurycode_icare, '99')
        end as c_2_1_47_toocs_mechanism,
        case
            when case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end < '19910701'
                or case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end > '20110630'
            then '000'
            else lpad(coalesce(i.breakdownagencycode_icare, '999')::varchar, 3, '0')
        end as c_2_1_48_breakdown_agency,
        coalesce(st.typecode, '0') as c_2_1_49_result_of_injury_code,
        coalesce(to_char(i.deceaseddate_icare, 'YYYYMMDD'), '00000000') as c_2_1_50_date_deceased,
        coalesce(wic.code::varchar, '000000') as c_2_1_52_workers_compensation_industry_classification_wic_rate_number,
        case
            when case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end < '19910701'
                or case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end > '20110630'
            then '000'
            else lpad(coalesce(i.agencyofinjurycode_icare, '999')::varchar, 3, '0')
        end as c_2_1_54_agency_of_injury_disease,
        case
            when i.significantinjurydate_icare > dte.submission_period_end_dt then '00000000'
            else coalesce(to_char(i.significantinjurydate_icare, 'YYYYMMDD'), '00000000')
        end as c_2_1_55_significant_injury_date,
        coalesce(to_char(i.contactcompletedate_icare, 'YYYYMMDD'), '00000000') as c_2_1_56_contact_complete_date,
        coalesce(claimant.homephone, 'NA') as c_2_1_58_worker_home_telephone_number,
        case
            when case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end < '20110701' then '0000'
            else lpad(coalesce(i.breakdownagencycode_icare, '9999')::varchar, 4, '0')
        end as c_2_1_59_toocs_breakdown_agency,
        case
            when case when cr.claim_number is not null then to_char(c.reporteddate, 'YYYYMMDD') else to_char(c.createtime, 'YYYYMMDD') end < '20110701' then '0000'
            else lpad(coalesce(i.agencyofinjurycode_icare, '9999')::varchar, 4, '0')
        end as c_2_1_60_toocs_agency_of_injury_disease,
        cctl_poltype.typecode as policy_type,
        iff(c.retired = 0, 'N', 'Y') as claim_retired,
        coalesce(mag.typecode, lag.typecode)::varchar(5) as insurer_number,
        c.omd_insert_module_instance_id,
        c.omd_insert_dttm,
        c.omd_record_src,
        c.omd_src_row_id,
        c.omd_cdc_operation,
        '' as omd_hash_full_record,
        c.omd_error_level,
        'Y' as omd_curr_record_ind,
        'N' as omd_del_record_ind,
        'N' as sira_processing_ind
    from cc_claim c
    left join claim_wic_icare cw on c.id = cw.ownerid and cw.row_num = 1 and cw.omd_cdc_operation <> 'D'
    left join ccx_wic_icare wic on wic.id = cw.foreignentityid
    left join shared_claim sc on sc.id = c.sharedclaim_icare
    left join insurer_branch ib on ib.id = c.branchinsurer_icare
    left join cc_policy p on p.id = c.policyid
    left join cctl_policytype cctl_poltype on cctl_poltype.id = p.policytype
    left join policy_period pp on pp.policynumber = p.policynumber
    left join cc_incident i on c.id = i.claimid
    left join duty_status ds on i.dutystatus_icare = ds.id
    left join cc_policy_location pl on pl.policyid = p.id
    left join cc_address ad on ad.id = pl.addressid
    left join country_ref cntry on cntry.id = ad.country
    left join cc_claimempdata ce on ce.ownerid = c.id
    left join cc_employmentdata ed on ed.id = ce.foreignentityid and ed.retired = 0
    left join ccx_industrycode_icare idc on ed.industrycode_icareid = idc.id
    left join employment_status_type est on est.id = ed.employmentstatus
    left join training_status_icare tsi on tsi.id = ed.trainingstatus_icare
    left join cc_workcomp wc on wc.id = c.claimworkcompid
    left join accident_loc_type alt on alt.id = wc.accidentlocationtype_icare
    left join cc_address l on l.id = c.losslocationid and l.retired = 0
    left join toocs_bloi_connector tbc on tbc.injuryincident_icareid = i.id and tbc.selectedasprimary = 1
    left join severity_type st on i.severity = st.id
    left join lodging_agent lag on c.lodgingagent_icare = lag.id
    left join managing_agent mag on c.claimsagent_icare = mag.id
    cross join submission_period dte
    left join claimant on claimant.claimid = c.id
    left join dependents on dependents.claimid = c.id
    left join piawe dolls on dolls.claimid = c.id and dolls.row_num = 1
    left join managing_entity ma_ccx on ma_ccx.id = c.managingentity_icare and ma_ccx.retired = 0 and ma_ccx.role in ('10001', '10002', '10004')
    inner join insurer_control ic on ic.insurer_number = mag.typecode
    left join claim_rule cr on cr.claim_number = c.claimnumber || lag.typecode
    where coalesce(c.lodgingagent_icare, c.claimsagent_icare) is not null
)

select * from base





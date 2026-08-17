{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_benefitsaccrual_icare.
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                record_insertion_date: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDA goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "business_critical"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:NonComplianceAmount AS NUMBER(18,2)) AS noncomplianceamount,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ExposureID::NUMBER AS exposureid,
                data_payload:FirstEntitlementWeeks::NUMBER AS firstentitlementweeks,
                CAST(data_payload:Section38Amount AS NUMBER(18,2)) AS section38amount,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:SecondEntitlementWeeks::NUMBER AS secondentitlementweeks,
                data_payload:TotalWeeksPaid::NUMBER AS totalweekspaid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                CAST(data_payload:TotalAmountPaid AS NUMBER(18,2)) AS totalamountpaid,
                data_payload:PostSecondWeek::NUMBER AS postsecondweek,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:PostSecondAmount AS NUMBER(18,2)) AS postsecondamount,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:SecondEntitlementAmount AS NUMBER(18,2)) AS secondentitlementamount,
                CAST(data_payload:FirstEntitlementAmount AS NUMBER(18,2)) AS firstentitlementamount,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ID::NUMBER AS id,
                data_payload:NonComplianceWeek::NUMBER AS noncomplianceweek,
                data_payload:Section38Week::NUMBER AS section38week,
                data_payload:Section41Week::NUMBER AS section41week,
                CAST(data_payload:Section41Amount AS NUMBER(18,2)) AS section41amount,
                CAST(data_payload:EWSec40Weeks AS NUMBER(7,2)) AS ewsec40weeks,
                data_payload:EWSec36Days::NUMBER AS ewsec36days,
                data_payload:EWSec37Days::NUMBER AS ewsec37days,
                CAST(data_payload:EWSec36Amount AS NUMBER(18,2)) AS ewsec36amount,
                CAST(data_payload:EWSec37Amount AS NUMBER(18,2)) AS ewsec37amount,
                data_payload:EWSec38Days::NUMBER AS ewsec38days,
                CAST(data_payload:EWSec38Amount AS NUMBER(18,2)) AS ewsec38amount,
                CAST(data_payload:EWSec37Weeks AS NUMBER(7,2)) AS ewsec37weeks,
                data_payload:EWTotalDays::NUMBER AS ewtotaldays,
                CAST(data_payload:EWTotalAmount AS NUMBER(18,2)) AS ewtotalamount,
                CAST(data_payload:EWTotalWeeks AS NUMBER(7,2)) AS ewtotalweeks,
                data_payload:EWSec40Days::NUMBER AS ewsec40days,
                CAST(data_payload:EWSec40Amount AS NUMBER(18,2)) AS ewsec40amount,
                CAST(data_payload:EWSec38Weeks AS NUMBER(7,2)) AS ewsec38weeks,
                CAST(data_payload:EWSec36Weeks AS NUMBER(7,2)) AS ewsec36weeks,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS VARCHAR(300)) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_benefitsaccrual_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:noncomplianceamount AS NUMBER(18,2)) AS noncomplianceamount,
                $1:createuserid::NUMBER AS createuserid,
                $1:exposureid::NUMBER AS exposureid,
                $1:firstentitlementweeks::NUMBER AS firstentitlementweeks,
                CAST($1:section38amount AS NUMBER(18,2)) AS section38amount,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:secondentitlementweeks::NUMBER AS secondentitlementweeks,
                $1:totalweekspaid::NUMBER AS totalweekspaid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                CAST($1:totalamountpaid AS NUMBER(18,2)) AS totalamountpaid,
                $1:postsecondweek::NUMBER AS postsecondweek,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:postsecondamount AS NUMBER(18,2)) AS postsecondamount,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:secondentitlementamount AS NUMBER(18,2)) AS secondentitlementamount,
                CAST($1:firstentitlementamount AS NUMBER(18,2)) AS firstentitlementamount,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:id::NUMBER AS id,
                $1:noncomplianceweek::NUMBER AS noncomplianceweek,
                $1:section38week::NUMBER AS section38week,
                $1:section41week::NUMBER AS section41week,
                CAST($1:section41amount AS NUMBER(18,2)) AS section41amount,
                CAST($1:ewsec40weeks AS NUMBER(7,2)) AS ewsec40weeks,
                $1:ewsec36days::NUMBER AS ewsec36days,
                $1:ewsec37days::NUMBER AS ewsec37days,
                CAST($1:ewsec36amount AS NUMBER(18,2)) AS ewsec36amount,
                CAST($1:ewsec37amount AS NUMBER(18,2)) AS ewsec37amount,
                $1:ewsec38days::NUMBER AS ewsec38days,
                CAST($1:ewsec38amount AS NUMBER(18,2)) AS ewsec38amount,
                CAST($1:ewsec37weeks AS NUMBER(7,2)) AS ewsec37weeks,
                $1:ewtotaldays::NUMBER AS ewtotaldays,
                CAST($1:ewtotalamount AS NUMBER(18,2)) AS ewtotalamount,
                CAST($1:ewtotalweeks AS NUMBER(7,2)) AS ewtotalweeks,
                $1:ewsec40days::NUMBER AS ewsec40days,
                CAST($1:ewsec40amount AS NUMBER(18,2)) AS ewsec40amount,
                CAST($1:ewsec38weeks AS NUMBER(7,2)) AS ewsec38weeks,
                CAST($1:ewsec36weeks AS NUMBER(7,2)) AS ewsec36weeks,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::VARCHAR(300) as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                metadata_row_number,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'ccx_benefitsaccrual_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS benefitsaccrual_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'noncomplianceamount',
                        'createuserid',
                        'exposureid',
                        'firstentitlementweeks',
                        'section38amount',
                        'publicid',
                        'secondentitlementweeks',
                        'totalweekspaid',
                        'beanversion',
                        'archivepartition',
                        'totalamountpaid',
                        'postsecondweek',
                        'createtime',
                        'retired',
                        'postsecondamount',
                        'updateuserid',
                        'secondentitlementamount',
                        'firstentitlementamount',
                        'updatetime',
                        'noncomplianceweek',
                        'section38week',
                        'section41week',
                        'section41amount',
                        'ewsec40weeks',
                        'ewsec36days',
                        'ewsec37days',
                        'ewsec36amount',
                        'ewsec37amount',
                        'ewsec38days',
                        'ewsec38amount',
                        'ewsec37weeks',
                        'ewtotaldays',
                        'ewtotalamount',
                        'ewtotalweeks',
                        'ewsec40days',
                        'ewsec40amount',
                        'ewsec38weeks',
                        'ewsec36weeks'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
    WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
    ORDER BY record_insertion_date

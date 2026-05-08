{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_usergroupstats.
                                                usergroupstats_sk: Entity identity surrogate key on PK ('id')
                                                hash_key: State change detection surrogate key on all business cols excluding PK
                                                dbt_updated_at: COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp)
                                                Uniform across AVRO and PARQUET - no code change when CDC goes live
                                                UNION ALL for BackFill of Data i.e. Incase of Rebuild of Upstream Model 
-#}   

{{ config(
    materialized='incremental',
    transient=True,
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_usergroupstats"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:SubLitAgingOne::NUMBER AS sublitagingone,
                data_payload:LitAgingOne::NUMBER AS litagingone,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ExpAgingOne::NUMBER AS expagingone,
                data_payload:AllSubOpen::NUMBER AS allsubopen,
                data_payload:SubroActiveExposure::NUMBER AS subroactiveexposure,
                data_payload:ClaimAgingTwo::NUMBER AS claimagingtwo,
                data_payload:DueToday::NUMBER AS duetoday,
                data_payload:SubAgingTwo::NUMBER AS subagingtwo,
                TO_TIMESTAMP_TZ(data_payload:CalculateDate::NUMBER/1000) AS calculatedate,
                data_payload:CompletedToday::NUMBER AS completedtoday,
                data_payload:SubroActiveClaim::NUMBER AS subroactiveclaim,
                data_payload:Flagged::NUMBER AS flagged,
                data_payload:GroupID::NUMBER AS groupid,
                data_payload:ID::NUMBER AS id,
                data_payload:AllActOpen::NUMBER AS allactopen,
                data_payload:ClaimAgingThree::NUMBER AS claimagingthree,
                data_payload:Overdue::NUMBER AS overdue,
                data_payload:SubAgingFour::NUMBER AS subagingfour,
                data_payload:SubAgingThree::NUMBER AS subagingthree,
                data_payload:ExpAgingFour::NUMBER AS expagingfour,
                data_payload:UserID::NUMBER AS userid,
                data_payload:ClaimAgingOne::NUMBER AS claimagingone,
                data_payload:ClaimAgingFour::NUMBER AS claimagingfour,
                data_payload:SubLitAgingTwo::NUMBER AS sublitagingtwo,
                data_payload:LitAgingTwo::NUMBER AS litagingtwo,
                data_payload:SubroClosed::NUMBER AS subroclosed,
                data_payload:SubAgingOne::NUMBER AS subagingone,
                data_payload:SubLitAgingFour::NUMBER AS sublitagingfour,
                data_payload:AllOpen::NUMBER AS allopen,
                data_payload:LitAgingFour::NUMBER AS litagingfour,
                data_payload:AllMatterOpen::NUMBER AS allmatteropen,
                data_payload:ExpAgingTwo::NUMBER AS expagingtwo,
                data_payload:NewThisWeek::NUMBER AS newthisweek,
                data_payload:ClaimWorkload::NUMBER AS claimworkload,
                data_payload:SubLitAgingThree::NUMBER AS sublitagingthree,
                data_payload:LitAgingThree::NUMBER AS litagingthree,
                data_payload:TotalWorkload::NUMBER AS totalworkload,
                data_payload:SubroActiveAll::NUMBER AS subroactiveall,
                data_payload:ExposureWorkload::NUMBER AS exposureworkload,
                data_payload:SubClosedThisWeek::NUMBER AS subclosedthisweek,
                data_payload:ClosedThisWeek::NUMBER AS closedthisweek,
                data_payload:ExpAgingThree::NUMBER AS expagingthree,
                data_payload:MatterClosedThisWeek::NUMBER AS matterclosedthisweek,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS STRING) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' file_type
            FROM {{ source('gwcc', 'cc_usergroupstats') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:sublitagingone::NUMBER AS sublitagingone,
                $1:litagingone::NUMBER AS litagingone,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:expagingone::NUMBER AS expagingone,
                $1:allsubopen::NUMBER AS allsubopen,
                $1:subroactiveexposure::NUMBER AS subroactiveexposure,
                $1:claimagingtwo::NUMBER AS claimagingtwo,
                $1:duetoday::NUMBER AS duetoday,
                $1:subagingtwo::NUMBER AS subagingtwo,
                $1:calculatedate::TIMESTAMP_TZ AS calculatedate,
                $1:completedtoday::NUMBER AS completedtoday,
                $1:subroactiveclaim::NUMBER AS subroactiveclaim,
                $1:flagged::NUMBER AS flagged,
                $1:groupid::NUMBER AS groupid,
                $1:id::NUMBER AS id,
                $1:allactopen::NUMBER AS allactopen,
                $1:claimagingthree::NUMBER AS claimagingthree,
                $1:overdue::NUMBER AS overdue,
                $1:subagingfour::NUMBER AS subagingfour,
                $1:subagingthree::NUMBER AS subagingthree,
                $1:expagingfour::NUMBER AS expagingfour,
                $1:userid::NUMBER AS userid,
                $1:claimagingone::NUMBER AS claimagingone,
                $1:claimagingfour::NUMBER AS claimagingfour,
                $1:sublitagingtwo::NUMBER AS sublitagingtwo,
                $1:litagingtwo::NUMBER AS litagingtwo,
                $1:subroclosed::NUMBER AS subroclosed,
                $1:subagingone::NUMBER AS subagingone,
                $1:sublitagingfour::NUMBER AS sublitagingfour,
                $1:allopen::NUMBER AS allopen,
                $1:litagingfour::NUMBER AS litagingfour,
                $1:allmatteropen::NUMBER AS allmatteropen,
                $1:expagingtwo::NUMBER AS expagingtwo,
                $1:newthisweek::NUMBER AS newthisweek,
                $1:claimworkload::NUMBER AS claimworkload,
                $1:sublitagingthree::NUMBER AS sublitagingthree,
                $1:litagingthree::NUMBER AS litagingthree,
                $1:totalworkload::NUMBER AS totalworkload,
                $1:subroactiveall::NUMBER AS subroactiveall,
                $1:exposureworkload::NUMBER AS exposureworkload,
                $1:subclosedthisweek::NUMBER AS subclosedthisweek,
                $1:closedthisweek::NUMBER AS closedthisweek,
                $1:expagingthree::NUMBER AS expagingthree,
                $1:matterclosedthisweek::NUMBER AS matterclosedthisweek,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' file_type
            FROM {{ source('gwcc', 'cc_usergroupstats') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS usergroupstats_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'sublitagingone',
                        'litagingone',
                        'publicid',
                        'expagingone',
                        'allsubopen',
                        'subroactiveexposure',
                        'claimagingtwo',
                        'duetoday',
                        'subagingtwo',
                        'calculatedate',
                        'completedtoday',
                        'subroactiveclaim',
                        'flagged',
                        'groupid',
                        'allactopen',
                        'claimagingthree',
                        'overdue',
                        'subagingfour',
                        'subagingthree',
                        'expagingfour',
                        'userid',
                        'claimagingone',
                        'claimagingfour',
                        'sublitagingtwo',
                        'litagingtwo',
                        'subroclosed',
                        'subagingone',
                        'sublitagingfour',
                        'allopen',
                        'litagingfour',
                        'allmatteropen',
                        'expagingtwo',
                        'newthisweek',
                        'claimworkload',
                        'sublitagingthree',
                        'litagingthree',
                        'totalworkload',
                        'subroactiveall',
                        'exposureworkload',
                        'subclosedthisweek',
                        'closedthisweek',
                        'expagingthree',
                        'matterclosedthisweek'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
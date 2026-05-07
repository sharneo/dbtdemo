{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_weeklybenefitinput_icare.
                                                weeklybenefitinput_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "ccx_weeklybenefitinput_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:CheckID::NUMBER AS checkid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:PermanentImpairment AS NUMBER(19,2)) AS permanentimpairment,
                data_payload:SurgeryDateRecieved::BOOLEAN AS surgerydaterecieved,
                CAST(data_payload:PIAWEPost52Weeks AS NUMBER(18,2)) AS piawepost52weeks,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PIAWEPre52Weeks AS NUMBER(18,2)) AS piawepre52weeks,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:PIAWE AS NUMBER(18,2)) AS piawe,
                data_payload:ContinuationOfWBApplication::BOOLEAN AS continuationofwbapplication,
                CAST(data_payload:WorkCapacityAsstPostSurgery::TEXT AS VARCHAR(512)) AS workcapacityasstpostsurgery,
                data_payload:ID::NUMBER AS id,
                data_payload:IncapableOfAddEmplymntIndfntly::BOOLEAN AS incapableofaddemplymntindfntly,
                CAST(data_payload:Capacity::TEXT AS VARCHAR(512)) AS capacity,
                data_payload:Section38DecEligible::BOOLEAN AS section38deceligible,
                TO_TIMESTAMP_TZ(data_payload:DateOfNotification::NUMBER/1000) AS dateofnotification,
                data_payload:ExistingReceipientFlag::BOOLEAN AS existingreceipientflag,
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
            FROM {{ source('gwcc', 'ccx_weeklybenefitinput_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:createuserid::NUMBER AS createuserid,
                $1:checkid::NUMBER AS checkid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:permanentimpairment AS NUMBER(19,2)) AS permanentimpairment,
                $1:surgerydaterecieved::BOOLEAN AS surgerydaterecieved,
                CAST($1:piawepost52weeks AS NUMBER(18,2)) AS piawepost52weeks,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:piawepre52weeks AS NUMBER(18,2)) AS piawepre52weeks,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:piawe AS NUMBER(18,2)) AS piawe,
                $1:continuationofwbapplication::BOOLEAN AS continuationofwbapplication,
                CAST($1:workcapacityasstpostsurgery::TEXT AS VARCHAR(512)) AS workcapacityasstpostsurgery,
                $1:id::NUMBER AS id,
                $1:incapableofaddemplymntindfntly::BOOLEAN AS incapableofaddemplymntindfntly,
                CAST($1:capacity::TEXT AS VARCHAR(512)) AS capacity,
                $1:section38deceligible::BOOLEAN AS section38deceligible,
                $1:dateofnotification::TIMESTAMP_TZ AS dateofnotification,
                $1:existingreceipientflag::BOOLEAN AS existingreceipientflag,
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
            FROM {{ source('gwcc', 'ccx_weeklybenefitinput_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS weeklybenefitinput_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'createuserid',
                        'checkid',
                        'publicid',
                        'beanversion',
                        'archivepartition',
                        'createtime',
                        'permanentimpairment',
                        'surgerydaterecieved',
                        'piawepost52weeks',
                        'updateuserid',
                        'piawepre52weeks',
                        'updatetime',
                        'piawe',
                        'continuationofwbapplication',
                        'workcapacityasstpostsurgery',
                        'incapableofaddemplymntindfntly',
                        'capacity',
                        'section38deceligible',
                        'dateofnotification',
                        'existingreceipientflag'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
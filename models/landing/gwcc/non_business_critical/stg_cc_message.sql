{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for cc_message.
                                                message_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwcc", "claim_centre", "non_business_critical", "cc_message"]
) }}


WITH cte_source_data AS 
(

            SELECT
                CAST(data_payload:ObjectKey::TEXT AS VARCHAR(60)) AS objectkey,
                TO_TIMESTAMP_TZ(data_payload:SendLockTime::NUMBER/1000) AS sendlocktime,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ErrorCategory::NUMBER AS errorcategory,
                data_payload:SendOrder::NUMBER AS sendorder,
                CAST(data_payload:Payload::TEXT AS VARCHAR(16777216)) AS payload,
                data_payload:OptionalInt::NUMBER AS optionalint,
                TO_TIMESTAMP_TZ(data_payload:BeforeSendTime::NUMBER/1000) AS beforesendtime,
                TO_TIMESTAMP_TZ(data_payload:SendTime::NUMBER/1000) AS sendtime,
                TO_TIMESTAMP_TZ(data_payload:QueryTime::NUMBER/1000) AS querytime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:LateBound::BOOLEAN AS latebound,
                CAST(data_payload:SenderRefID::TEXT AS VARCHAR(64)) AS senderrefid,
                data_payload:ID::NUMBER AS id,
                data_payload:ISOMessageType::NUMBER AS isomessagetype,
                CAST(data_payload:KeyMap::TEXT AS VARCHAR(16777216)) AS keymap,
                TO_TIMESTAMP_TZ(data_payload:BeforeSendLockedTime::NUMBER/1000) AS beforesendlockedtime,
                TO_TIMESTAMP_TZ(data_payload:SendLockedTime::NUMBER/1000) AS sendlockedtime,
                CAST(data_payload:OptionalMoney AS NUMBER(18,2)) AS optionalmoney,
                CAST(data_payload:EventRootKey::TEXT AS VARCHAR(60)) AS eventrootkey,
                data_payload:DuplicateCount::NUMBER AS duplicatecount,
                CAST(data_payload:EventName::TEXT AS VARCHAR(255)) AS eventname,
                data_payload:UserID::NUMBER AS userid,
                data_payload:AckCount::NUMBER AS ackcount,
                data_payload:RetryCount::NUMBER AS retrycount,
                data_payload:DestinationID::NUMBER AS destinationid,
                TO_TIMESTAMP_TZ(data_payload:CreationTime::NUMBER/1000) AS creationtime,
                data_payload:LockingColumn::NUMBER AS lockingcolumn,
                CAST(data_payload:OptionalString::TEXT AS VARCHAR(255)) AS optionalstring,
                TO_TIMESTAMP_TZ(data_payload:AfterSendTime::NUMBER/1000) AS aftersendtime,
                data_payload:Status::NUMBER AS status,
                CAST(data_payload:MessageCode::TEXT AS VARCHAR(255)) AS messagecode,
                CAST(data_payload:ErrorDescription::TEXT AS VARCHAR(255)) AS errordescription,
                TO_TIMESTAMP_TZ(data_payload:RetryTime::NUMBER/1000) AS retrytime,
                CAST(data_payload:AckCode::TEXT AS VARCHAR(255)) AS ackcode,
                data_payload:contactID::NUMBER AS contactid,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                data_payload:Bound::BOOLEAN AS bound,
                TO_TIMESTAMP_TZ(data_payload:BeforeSendLockTime::NUMBER/1000) AS beforesendlocktime,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_connector_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_lsn,
                CAST(NULL AS NUMBER) as gwcbi_operation,
                CAST(NULL AS TIMESTAMP_LTZ) as gwcbi_payload_ts_ms,
                CAST(NULL AS NUMBER) as gwcbi_seqval,
                CAST(NULL AS STRING) as gwcbi_seqval_hex,
                CAST(NULL AS NUMBER) as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'AVRO' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_message') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                CAST($1:objectkey::TEXT AS VARCHAR(60)) AS objectkey,
                $1:sendlocktime::TIMESTAMP_TZ AS sendlocktime,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:errorcategory::NUMBER AS errorcategory,
                $1:sendorder::NUMBER AS sendorder,
                CAST($1:payload::TEXT AS VARCHAR(16777216)) AS payload,
                $1:optionalint::NUMBER AS optionalint,
                $1:beforesendtime::TIMESTAMP_TZ AS beforesendtime,
                $1:sendtime::TIMESTAMP_TZ AS sendtime,
                $1:querytime::TIMESTAMP_TZ AS querytime,
                $1:claimid::NUMBER AS claimid,
                $1:latebound::BOOLEAN AS latebound,
                CAST($1:senderrefid::TEXT AS VARCHAR(64)) AS senderrefid,
                $1:id::NUMBER AS id,
                $1:isomessagetype::NUMBER AS isomessagetype,
                CAST($1:keymap::TEXT AS VARCHAR(16777216)) AS keymap,
                $1:beforesendlockedtime::TIMESTAMP_TZ AS beforesendlockedtime,
                $1:sendlockedtime::TIMESTAMP_TZ AS sendlockedtime,
                CAST($1:optionalmoney AS NUMBER(18,2)) AS optionalmoney,
                CAST($1:eventrootkey::TEXT AS VARCHAR(60)) AS eventrootkey,
                $1:duplicatecount::NUMBER AS duplicatecount,
                CAST($1:eventname::TEXT AS VARCHAR(255)) AS eventname,
                $1:userid::NUMBER AS userid,
                $1:ackcount::NUMBER AS ackcount,
                $1:retrycount::NUMBER AS retrycount,
                $1:destinationid::NUMBER AS destinationid,
                $1:creationtime::TIMESTAMP_TZ AS creationtime,
                $1:lockingcolumn::NUMBER AS lockingcolumn,
                CAST($1:optionalstring::TEXT AS VARCHAR(255)) AS optionalstring,
                $1:aftersendtime::TIMESTAMP_TZ AS aftersendtime,
                $1:status::NUMBER AS status,
                CAST($1:messagecode::TEXT AS VARCHAR(255)) AS messagecode,
                CAST($1:errordescription::TEXT AS VARCHAR(255)) AS errordescription,
                $1:retrytime::TIMESTAMP_TZ AS retrytime,
                CAST($1:ackcode::TEXT AS VARCHAR(255)) AS ackcode,
                $1:contactid::NUMBER AS contactid,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:bound::BOOLEAN AS bound,
                $1:beforesendlocktime::TIMESTAMP_TZ AS beforesendlocktime,
                TO_TIMESTAMP($1:gwcbi___connector_ts_ms::NUMBER / 1000) as gwcbi_connector_ts_ms,
                $1:gwcbi___lsn::NUMBER as gwcbi_lsn,
                $1:gwcbi___operation::NUMBER as gwcbi_operation,
                TO_TIMESTAMP($1:gwcbi___payload_ts_ms::NUMBER / 1000) as gwcbi_payload_ts_ms,
                $1:gwcbi___seqval::NUMBER as gwcbi_seqval,
                $1:gwcbi___seqval_hex::STRING as gwcbi_seqval_hex,
                $1:gwcbi___tx_id::NUMBER as gwcbi_tx_id,
                metadata_file_name,
                file_ingestion_timestamp,
                'PARQUET' as file_type,
                'GWCC' as source_system
            FROM {{ source('gwcc', 'cc_message') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS message_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'objectkey',
                        'sendlocktime',
                        'publicid',
                        'errorcategory',
                        'sendorder',
                        'payload',
                        'optionalint',
                        'beforesendtime',
                        'sendtime',
                        'querytime',
                        'claimid',
                        'latebound',
                        'senderrefid',
                        'isomessagetype',
                        'keymap',
                        'beforesendlockedtime',
                        'sendlockedtime',
                        'optionalmoney',
                        'eventrootkey',
                        'duplicatecount',
                        'eventname',
                        'userid',
                        'ackcount',
                        'retrycount',
                        'destinationid',
                        'creationtime',
                        'lockingcolumn',
                        'optionalstring',
                        'aftersendtime',
                        'status',
                        'messagecode',
                        'errordescription',
                        'retrytime',
                        'ackcode',
                        'contactid',
                        'description',
                        'bound',
                        'beforesendlocktime'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
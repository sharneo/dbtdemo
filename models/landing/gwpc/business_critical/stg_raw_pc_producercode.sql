{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_producercode.
                                                producercode_sk: Entity identity surrogate key on PK ('id')
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
    tags=["landing", "gwpc", "policy_centre", "business_critical", "pc_producercode"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                data_payload:ProducerStatus::NUMBER AS producerstatus,
                data_payload:PreferredUnderwriterID::NUMBER AS preferredunderwriterid,
                data_payload:BeanVersion::NUMBER AS beanversion,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:CodeDenorm::TEXT AS VARCHAR(255)) AS codedenorm,
                CAST(data_payload:Code::TEXT AS VARCHAR(255)) AS code,
                TO_TIMESTAMP_TZ(data_payload:AppointmentDate::NUMBER/1000) AS appointmentdate,
                data_payload:OrganizationID::NUMBER AS organizationid,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:AddressID::NUMBER AS addressid,
                TO_TIMESTAMP_TZ(data_payload:TerminationDate::NUMBER/1000) AS terminationdate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:DescriptionDenorm::TEXT AS VARCHAR(255)) AS descriptiondenorm,
                data_payload:ID::NUMBER AS id,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                data_payload:BranchID::NUMBER AS branchid,
                CAST(data_payload:EmailAddress_icare::TEXT AS VARCHAR(60)) AS emailaddress_icare,
                data_payload:ComunicatnPrfrnce_icare::NUMBER AS comunicatnprfrnce_icare,
                CAST(data_payload:TotalBTP_icare AS NUMBER(18,2)) AS totalbtp_icare,
                data_payload:PolicyCount_icare::NUMBER AS policycount_icare,
                CAST(data_payload:AddressPublicID::TEXT AS VARCHAR(64)) AS addresspublicid,
                data_payload:StopCorrespondence_Ext::BOOLEAN AS stopcorrespondence_ext,
                data_payload:StopBCCorrespondence_Ext::BOOLEAN AS stopbccorrespondence_ext,
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
            FROM {{ source('gwpc', 'pc_producercode') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:producerstatus::NUMBER AS producerstatus,
                $1:preferredunderwriterid::NUMBER AS preferredunderwriterid,
                $1:beanversion::NUMBER AS beanversion,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:retired::NUMBER AS retired,
                CAST($1:codedenorm::TEXT AS VARCHAR(255)) AS codedenorm,
                CAST($1:code::TEXT AS VARCHAR(255)) AS code,
                $1:appointmentdate::TIMESTAMP_TZ AS appointmentdate,
                $1:organizationid::NUMBER AS organizationid,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:addressid::NUMBER AS addressid,
                $1:terminationdate::TIMESTAMP_TZ AS terminationdate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:descriptiondenorm::TEXT AS VARCHAR(255)) AS descriptiondenorm,
                $1:id::NUMBER AS id,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:branchid::NUMBER AS branchid,
                CAST($1:emailaddress_icare::TEXT AS VARCHAR(60)) AS emailaddress_icare,
                $1:comunicatnprfrnce_icare::NUMBER AS comunicatnprfrnce_icare,
                CAST($1:totalbtp_icare AS NUMBER(18,2)) AS totalbtp_icare,
                $1:policycount_icare::NUMBER AS policycount_icare,
                CAST($1:addresspublicid::TEXT AS VARCHAR(64)) AS addresspublicid,
                $1:stopcorrespondence_ext::BOOLEAN AS stopcorrespondence_ext,
                $1:stopbccorrespondence_ext::BOOLEAN AS stopbccorrespondence_ext,
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
            FROM {{ source('gwpc', 'pc_producercode') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS producercode_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'createuserid',
                        'publicid',
                        'producerstatus',
                        'preferredunderwriterid',
                        'beanversion',
                        'createtime',
                        'retired',
                        'codedenorm',
                        'code',
                        'appointmentdate',
                        'organizationid',
                        'updateuserid',
                        'addressid',
                        'terminationdate',
                        'updatetime',
                        'descriptiondenorm',
                        'description',
                        'branchid',
                        'emailaddress_icare',
                        'comunicatnprfrnce_icare',
                        'totalbtp_icare',
                        'policycount_icare',
                        'addresspublicid',
                        'stopcorrespondence_ext',
                        'stopbccorrespondence_ext'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
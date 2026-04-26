{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for pc_document.
                                                document_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_policy_centre", "policy_centre", "non_business_critical", "pc_document"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:DocumentIdentifierDenorm::TEXT AS VARCHAR(60)) AS documentidentifierdenorm,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:DocumentIdentifier::TEXT AS VARCHAR(60)) AS documentidentifier,
                CAST(data_payload:PortalSecurityRealm_Ext::TEXT AS VARCHAR(60)) AS portalsecurityrealm_ext,
                CAST(data_payload:AuthorDenorm::TEXT AS VARCHAR(60)) AS authordenorm,
                CAST(data_payload:NameDenorm::TEXT AS VARCHAR(150)) AS namedenorm,
                data_payload:AccountID::NUMBER AS accountid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:DMS::BOOLEAN AS dms,
                CAST(data_payload:Author::TEXT AS VARCHAR(60)) AS author,
                CAST(data_payload:Name::TEXT AS VARCHAR(150)) AS name,
                data_payload:PolicyID::NUMBER AS policyid,
                data_payload:PackType_icare::NUMBER AS packtype_icare,
                data_payload:PolicyPeriodID::NUMBER AS policyperiodid,
                data_payload:Contingency::NUMBER AS contingency,
                data_payload:ECMDocStatusID_icare::NUMBER AS ecmdocstatusid_icare,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:OnBaseDocTypeID_icare::TEXT AS VARCHAR(10)) AS onbasedoctypeid_icare,
                CAST(data_payload:DocUID::TEXT AS VARCHAR(255)) AS docuid,
                data_payload:Language::NUMBER AS language,
                data_payload:JobID::NUMBER AS jobid,
                data_payload:Obsolete::BOOLEAN AS obsolete,
                CAST(data_payload:Recipient::TEXT AS VARCHAR(60)) AS recipient,
                data_payload:ID::NUMBER AS id,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:Section::NUMBER AS section,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:Retired::NUMBER AS retired,
                CAST(data_payload:MimeType::TEXT AS VARCHAR(80)) AS mimetype,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                CAST(data_payload:PendingDocUID::TEXT AS VARCHAR(255)) AS pendingdocuid,
                data_payload:Status::NUMBER AS status,
                data_payload:PolicyTerm_icareID::NUMBER AS policyterm_icareid,
                CAST(data_payload:TransactionID_icare::TEXT AS VARCHAR(15)) AS transactionid_icare,
                data_payload:Resend_icare::BOOLEAN AS resend_icare,
                TO_TIMESTAMP_TZ(data_payload:DateModified::NUMBER/1000) AS datemodified,
                data_payload:Inbound::BOOLEAN AS inbound,
                TO_TIMESTAMP_TZ(data_payload:DateCreated::NUMBER/1000) AS datecreated,
                data_payload:SecurityType::NUMBER AS securitytype,
                data_payload:Type::NUMBER AS type,
                CAST(data_payload:Description::TEXT AS VARCHAR(255)) AS description,
                data_payload:AddresseeType_icare::NUMBER AS addresseetype_icare,
                data_payload:SourceSystem_icare::NUMBER AS sourcesystem_icare,
                CAST(data_payload:S3URL_icare::TEXT AS VARCHAR(500)) AS s3url_icare,
                TO_TIMESTAMP_TZ(data_payload:S3UploadDate_icare::NUMBER/1000) AS s3uploaddate_icare,
                TO_TIMESTAMP_TZ(data_payload:OnBaseStoreDate_icare::NUMBER/1000) AS onbasestoredate_icare,
                data_payload:ExternalDocTypeID_icare::NUMBER AS externaldoctypeid_icare,
                CAST(data_payload:TemplateID_Icare::TEXT AS VARCHAR(15)) AS templateid_icare,
                CAST(data_payload:OnBaseURL_icare::TEXT AS VARCHAR(500)) AS onbaseurl_icare,
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
            FROM {{ source('gwpc', 'pc_document') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:documentidentifierdenorm::TEXT AS VARCHAR(60)) AS documentidentifierdenorm,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:documentidentifier::TEXT AS VARCHAR(60)) AS documentidentifier,
                CAST($1:portalsecurityrealm_ext::TEXT AS VARCHAR(60)) AS portalsecurityrealm_ext,
                CAST($1:authordenorm::TEXT AS VARCHAR(60)) AS authordenorm,
                CAST($1:namedenorm::TEXT AS VARCHAR(150)) AS namedenorm,
                $1:accountid::NUMBER AS accountid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:dms::BOOLEAN AS dms,
                CAST($1:author::TEXT AS VARCHAR(60)) AS author,
                CAST($1:name::TEXT AS VARCHAR(150)) AS name,
                $1:policyid::NUMBER AS policyid,
                $1:packtype_icare::NUMBER AS packtype_icare,
                $1:policyperiodid::NUMBER AS policyperiodid,
                $1:contingency::NUMBER AS contingency,
                $1:ecmdocstatusid_icare::NUMBER AS ecmdocstatusid_icare,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:onbasedoctypeid_icare::TEXT AS VARCHAR(10)) AS onbasedoctypeid_icare,
                CAST($1:docuid::TEXT AS VARCHAR(255)) AS docuid,
                $1:language::NUMBER AS language,
                $1:jobid::NUMBER AS jobid,
                $1:obsolete::BOOLEAN AS obsolete,
                CAST($1:recipient::TEXT AS VARCHAR(60)) AS recipient,
                $1:id::NUMBER AS id,
                $1:createuserid::NUMBER AS createuserid,
                $1:section::NUMBER AS section,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:beanversion::NUMBER AS beanversion,
                $1:retired::NUMBER AS retired,
                CAST($1:mimetype::TEXT AS VARCHAR(80)) AS mimetype,
                $1:updateuserid::NUMBER AS updateuserid,
                CAST($1:pendingdocuid::TEXT AS VARCHAR(255)) AS pendingdocuid,
                $1:status::NUMBER AS status,
                $1:policyterm_icareid::NUMBER AS policyterm_icareid,
                CAST($1:transactionid_icare::TEXT AS VARCHAR(15)) AS transactionid_icare,
                $1:resend_icare::BOOLEAN AS resend_icare,
                $1:datemodified::TIMESTAMP_TZ AS datemodified,
                $1:inbound::BOOLEAN AS inbound,
                $1:datecreated::TIMESTAMP_TZ AS datecreated,
                $1:securitytype::NUMBER AS securitytype,
                $1:type::NUMBER AS type,
                CAST($1:description::TEXT AS VARCHAR(255)) AS description,
                $1:addresseetype_icare::NUMBER AS addresseetype_icare,
                $1:sourcesystem_icare::NUMBER AS sourcesystem_icare,
                CAST($1:s3url_icare::TEXT AS VARCHAR(500)) AS s3url_icare,
                $1:s3uploaddate_icare::TIMESTAMP_TZ AS s3uploaddate_icare,
                $1:onbasestoredate_icare::TIMESTAMP_TZ AS onbasestoredate_icare,
                $1:externaldoctypeid_icare::NUMBER AS externaldoctypeid_icare,
                CAST($1:templateid_icare::TEXT AS VARCHAR(15)) AS templateid_icare,
                CAST($1:onbaseurl_icare::TEXT AS VARCHAR(500)) AS onbaseurl_icare,
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
            FROM {{ source('gwpc', 'pc_document') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS document_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'documentidentifierdenorm',
                        'publicid',
                        'documentidentifier',
                        'portalsecurityrealm_ext',
                        'authordenorm',
                        'namedenorm',
                        'accountid',
                        'createtime',
                        'dms',
                        'author',
                        'name',
                        'policyid',
                        'packtype_icare',
                        'policyperiodid',
                        'contingency',
                        'ecmdocstatusid_icare',
                        'updatetime',
                        'onbasedoctypeid_icare',
                        'docuid',
                        'language',
                        'jobid',
                        'obsolete',
                        'recipient',
                        'createuserid',
                        'section',
                        'archivepartition',
                        'beanversion',
                        'retired',
                        'mimetype',
                        'updateuserid',
                        'pendingdocuid',
                        'status',
                        'policyterm_icareid',
                        'transactionid_icare',
                        'resend_icare',
                        'datemodified',
                        'inbound',
                        'datecreated',
                        'securitytype',
                        'type',
                        'description',
                        'addresseetype_icare',
                        'sourcesystem_icare',
                        's3url_icare',
                        's3uploaddate_icare',
                        'onbasestoredate_icare',
                        'externaldoctypeid_icare',
                        'templateid_icare',
                        'onbaseurl_icare'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
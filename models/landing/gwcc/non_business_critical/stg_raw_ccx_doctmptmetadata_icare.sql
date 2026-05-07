{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_doctmptmetadata_icare.
                                                doctmptmetadata_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_doctmptmetadata_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                data_payload:Legislation::NUMBER AS legislation,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                CAST(data_payload:Domain::TEXT AS VARCHAR(255)) AS domain,
                CAST(data_payload:Brand::TEXT AS VARCHAR(255)) AS brand,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                data_payload:ChannelPrint::BOOLEAN AS channelprint,
                CAST(data_payload:Name::TEXT AS VARCHAR(1333)) AS name,
                data_payload:LineOfBusiness::NUMBER AS lineofbusiness,
                data_payload:RecipientType::NUMBER AS recipienttype,
                data_payload:AttachDocuments::NUMBER AS attachdocuments,
                CAST(data_payload:TemplateID::TEXT AS VARCHAR(255)) AS templateid,
                TO_TIMESTAMP_TZ(data_payload:EffectiveDate::NUMBER/1000) AS effectivedate,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                CAST(data_payload:Version::TEXT AS VARCHAR(255)) AS version,
                data_payload:TemplateXML_icare::BINARY AS templatexml_icare,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:ExpirationDate::NUMBER/1000) AS expirationdate,
                data_payload:CreateUserID::NUMBER AS createuserid,
                data_payload:ApprovalRequired::BOOLEAN AS approvalrequired,
                data_payload:Interactive::BOOLEAN AS interactive,
                data_payload:SubSection::NUMBER AS subsection,
                data_payload:BeanVersion::NUMBER AS beanversion,
                CAST(data_payload:Keywords::TEXT AS VARCHAR(1333)) AS keywords,
                CAST(data_payload:Code::TEXT AS VARCHAR(255)) AS code,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                data_payload:TemplateType::NUMBER AS templatetype,
                data_payload:ChannelEmail::BOOLEAN AS channelemail,
                data_payload:SelectorID::NUMBER AS selectorid,
                CAST(data_payload:Description::TEXT AS VARCHAR(1333)) AS description,
                data_payload:VisibleInUI::BOOLEAN AS visibleinui,
                data_payload:ChannelArchival::BOOLEAN AS channelarchival,
                data_payload:DocumentSearchConfigID::NUMBER AS documentsearchconfigid,
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
            FROM {{ source('gwcc', 'ccx_doctmptmetadata_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                $1:legislation::NUMBER AS legislation,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                CAST($1:domain::TEXT AS VARCHAR(255)) AS domain,
                CAST($1:brand::TEXT AS VARCHAR(255)) AS brand,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                $1:channelprint::BOOLEAN AS channelprint,
                CAST($1:name::TEXT AS VARCHAR(1333)) AS name,
                $1:lineofbusiness::NUMBER AS lineofbusiness,
                $1:recipienttype::NUMBER AS recipienttype,
                $1:attachdocuments::NUMBER AS attachdocuments,
                CAST($1:templateid::TEXT AS VARCHAR(255)) AS templateid,
                $1:effectivedate::TIMESTAMP_TZ AS effectivedate,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                CAST($1:version::TEXT AS VARCHAR(255)) AS version,
                $1:templatexml_icare::BINARY AS templatexml_icare,
                $1:id::NUMBER AS id,
                $1:expirationdate::TIMESTAMP_TZ AS expirationdate,
                $1:createuserid::NUMBER AS createuserid,
                $1:approvalrequired::BOOLEAN AS approvalrequired,
                $1:interactive::BOOLEAN AS interactive,
                $1:subsection::NUMBER AS subsection,
                $1:beanversion::NUMBER AS beanversion,
                CAST($1:keywords::TEXT AS VARCHAR(1333)) AS keywords,
                CAST($1:code::TEXT AS VARCHAR(255)) AS code,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:templatetype::NUMBER AS templatetype,
                $1:channelemail::BOOLEAN AS channelemail,
                $1:selectorid::NUMBER AS selectorid,
                CAST($1:description::TEXT AS VARCHAR(1333)) AS description,
                $1:visibleinui::BOOLEAN AS visibleinui,
                $1:channelarchival::BOOLEAN AS channelarchival,
                $1:documentsearchconfigid::NUMBER AS documentsearchconfigid,
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
            FROM {{ source('gwcc', 'ccx_doctmptmetadata_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS doctmptmetadata_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'legislation',
                        'publicid',
                        'domain',
                        'brand',
                        'createtime',
                        'channelprint',
                        'name',
                        'lineofbusiness',
                        'recipienttype',
                        'attachdocuments',
                        'templateid',
                        'effectivedate',
                        'updatetime',
                        'version',
                        'templatexml_icare',
                        'expirationdate',
                        'createuserid',
                        'approvalrequired',
                        'interactive',
                        'subsection',
                        'beanversion',
                        'keywords',
                        'code',
                        'updateuserid',
                        'templatetype',
                        'channelemail',
                        'selectorid',
                        'description',
                        'visibleinui',
                        'channelarchival',
                        'documentsearchconfigid'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) as record_insertion_date
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
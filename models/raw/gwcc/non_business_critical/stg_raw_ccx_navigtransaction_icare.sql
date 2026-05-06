{#-

Project: Data Uplift Program 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2026-01-01      0.0                             Incremental staging model for ccx_navigtransaction_icare.
                                                navigtransaction_icare_sk: Entity identity surrogate key on PK ('id')
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
    tags=["raw_layer", "raw_claim_centre", "claim_centre", "non_business_critical", "ccx_navigtransaction_icare"]
) }}


WITH cte_source_data AS 
(

            SELECT
                data_payload:LoadCommandID::NUMBER AS loadcommandid,
                CAST(data_payload:NAVClaimNumber::TEXT AS VARCHAR(19)) AS navclaimnumber,
                CAST(data_payload:PublicID::TEXT AS VARCHAR(64)) AS publicid,
                TO_TIMESTAMP_TZ(data_payload:CreateTime::NUMBER/1000) AS createtime,
                CAST(data_payload:HICNumber::TEXT AS VARCHAR(20)) AS hicnumber,
                data_payload:CostCategory::NUMBER AS costcategory,
                data_payload:RecordTypeNo::NUMBER AS recordtypeno,
                CAST(data_payload:LineAmount AS NUMBER(18,2)) AS lineamount,
                CAST(data_payload:ServiceProvABN::TEXT AS VARCHAR(20)) AS serviceprovabn,
                TO_TIMESTAMP_TZ(data_payload:DateOfService::NUMBER/1000) AS dateofservice,
                CAST(data_payload:AccreditedWCProviderID::TEXT AS VARCHAR(20)) AS accreditedwcproviderid,
                data_payload:RecordNo::NUMBER AS recordno,
                TO_TIMESTAMP_TZ(data_payload:UpdateTime::NUMBER/1000) AS updatetime,
                data_payload:ClaimID::NUMBER AS claimid,
                data_payload:ID::NUMBER AS id,
                TO_TIMESTAMP_TZ(data_payload:InvoiceDate::NUMBER/1000) AS invoicedate,
                CAST(data_payload:TotalAmount AS NUMBER(18,2)) AS totalamount,
                CAST(data_payload:GSTAmount AS NUMBER(18,2)) AS gstamount,
                data_payload:CreateUserID::NUMBER AS createuserid,
                CAST(data_payload:PayeeABN::TEXT AS VARCHAR(20)) AS payeeabn,
                CAST(data_payload:ReimbursementCode::TEXT AS VARCHAR(20)) AS reimbursementcode,
                data_payload:BeanVersion::NUMBER AS beanversion,
                data_payload:ArchivePartition::NUMBER AS archivepartition,
                CAST(data_payload:SignedLineAmount AS NUMBER(18,2)) AS signedlineamount,
                data_payload:HeaderID::NUMBER AS headerid,
                data_payload:Retired::NUMBER AS retired,
                data_payload:Debit::NUMBER AS debit,
                data_payload:UpdateUserID::NUMBER AS updateuserid,
                TO_TIMESTAMP_TZ(data_payload:InvoiceProcessedDate::NUMBER/1000) AS invoiceprocesseddate,
                data_payload:PayCodeID::NUMBER AS paycodeid,
                CAST(data_payload:Status::TEXT AS VARCHAR(5)) AS status,
                CAST(data_payload:InvoiceNumber::TEXT AS VARCHAR(20)) AS invoicenumber,
                CAST(data_payload:LineItemPmtNumber::TEXT AS VARCHAR(4)) AS lineitempmtnumber,
                data_payload:Credit::NUMBER AS credit,
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
            FROM {{ source('gwcc', 'ccx_navigtransaction_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:loadcommandid::NUMBER AS loadcommandid,
                CAST($1:navclaimnumber::TEXT AS VARCHAR(19)) AS navclaimnumber,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:createtime::TIMESTAMP_TZ AS createtime,
                CAST($1:hicnumber::TEXT AS VARCHAR(20)) AS hicnumber,
                $1:costcategory::NUMBER AS costcategory,
                $1:recordtypeno::NUMBER AS recordtypeno,
                CAST($1:lineamount AS NUMBER(18,2)) AS lineamount,
                CAST($1:serviceprovabn::TEXT AS VARCHAR(20)) AS serviceprovabn,
                $1:dateofservice::TIMESTAMP_TZ AS dateofservice,
                CAST($1:accreditedwcproviderid::TEXT AS VARCHAR(20)) AS accreditedwcproviderid,
                $1:recordno::NUMBER AS recordno,
                $1:updatetime::TIMESTAMP_TZ AS updatetime,
                $1:claimid::NUMBER AS claimid,
                $1:id::NUMBER AS id,
                $1:invoicedate::TIMESTAMP_TZ AS invoicedate,
                CAST($1:totalamount AS NUMBER(18,2)) AS totalamount,
                CAST($1:gstamount AS NUMBER(18,2)) AS gstamount,
                $1:createuserid::NUMBER AS createuserid,
                CAST($1:payeeabn::TEXT AS VARCHAR(20)) AS payeeabn,
                CAST($1:reimbursementcode::TEXT AS VARCHAR(20)) AS reimbursementcode,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                CAST($1:signedlineamount AS NUMBER(18,2)) AS signedlineamount,
                $1:headerid::NUMBER AS headerid,
                $1:retired::NUMBER AS retired,
                $1:debit::NUMBER AS debit,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:invoiceprocesseddate::TIMESTAMP_TZ AS invoiceprocesseddate,
                $1:paycodeid::NUMBER AS paycodeid,
                CAST($1:status::TEXT AS VARCHAR(5)) AS status,
                CAST($1:invoicenumber::TEXT AS VARCHAR(20)) AS invoicenumber,
                CAST($1:lineitempmtnumber::TEXT AS VARCHAR(4)) AS lineitempmtnumber,
                $1:credit::NUMBER AS credit,
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
            FROM {{ source('gwcc', 'ccx_navigtransaction_icare') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),

cte_transformed AS (
    SELECT
        *,
        CAST({{ dbt_utils.generate_surrogate_key([
            'id'
        ]) }} AS VARCHAR(150)) AS navigtransaction_icare_sk,
        CAST({{ dbt_utils.generate_surrogate_key([
                        'loadcommandid',
                        'navclaimnumber',
                        'publicid',
                        'createtime',
                        'hicnumber',
                        'costcategory',
                        'recordtypeno',
                        'lineamount',
                        'serviceprovabn',
                        'dateofservice',
                        'accreditedwcproviderid',
                        'recordno',
                        'updatetime',
                        'claimid',
                        'invoicedate',
                        'totalamount',
                        'gstamount',
                        'createuserid',
                        'payeeabn',
                        'reimbursementcode',
                        'beanversion',
                        'archivepartition',
                        'signedlineamount',
                        'headerid',
                        'retired',
                        'debit',
                        'updateuserid',
                        'invoiceprocesseddate',
                        'paycodeid',
                        'status',
                        'invoicenumber',
                        'lineitempmtnumber',
                        'credit'
        ]) }} AS VARCHAR(150)) AS hash_key,
        COALESCE(gwcbi_payload_ts_ms, file_ingestion_timestamp) AS dbt_updated_at
    FROM cte_source_data
)

SELECT * FROM cte_transformed
{% if is_incremental() %}
WHERE file_ingestion_timestamp > (SELECT COALESCE(MAX(file_ingestion_timestamp), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
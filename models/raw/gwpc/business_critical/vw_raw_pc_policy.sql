{#-

Project: Data Uplift Progran 
Project Description/Purpose: Data Uplift Program 

Date            Version         Author          Description of Change           
2025-11-18      0.0                             This Converts Parquet or AVRO Data in Variant Column in the RAW DB into Flattend Views

-#}   

{{ config(
    tags=["raw_gwpc"]
) }}

{%- set source_system_code = 'GWPC' -%}

WITH source_data AS 
(

            SELECT
                data_payload:DONOTDESTROY::BOOLEAN AS DONOTDESTROY,
                data_payload:ISPORTALPOLICY_ICARE::BOOLEAN AS ISPORTALPOLICY_ICARE,
                CAST(data_payload:PUBLICID::TEXT AS VARCHAR(64)) AS PUBLICID,
                data_payload:PRIORPREMIUMS::NUMBER AS PRIORPREMIUMS,
                TO_TIMESTAMP_NTZ(data_payload:ISSUEDATE::NUMBER/1000) AS ISSUEDATE,
                data_payload:PRIORPREMIUMS_CUR::NUMBER AS PRIORPREMIUMS_CUR,
                data_payload:MOVEDPOLICYSOURCEACCOUNTID::NUMBER AS MOVEDPOLICYSOURCEACCOUNTID,
                data_payload:ACCOUNTID::NUMBER AS ACCOUNTID,
                TO_TIMESTAMP_NTZ(data_payload:CREATETIME::NUMBER/1000) AS CREATETIME,
                data_payload:LOSSHISTORYTYPE::NUMBER AS LOSSHISTORYTYPE,
                data_payload:EXCLUDEDFROMARCHIVE::BOOLEAN AS EXCLUDEDFROMARCHIVE,
                data_payload:ARCHIVESTATE::NUMBER AS ARCHIVESTATE,
                data_payload:ARCHIVESCHEMAINFO::NUMBER AS ARCHIVESCHEMAINFO,
                data_payload:ARCHIVEFAILUREDETAILSID::NUMBER AS ARCHIVEFAILUREDETAILSID,
                data_payload:PACKAGERISK::NUMBER AS PACKAGERISK,
                data_payload:NUMPRIORLOSSES::NUMBER AS NUMPRIORLOSSES,
                TO_TIMESTAMP_NTZ(data_payload:UPDATETIME::NUMBER/1000) AS UPDATETIME,
                data_payload:PRIMARYLANGUAGE::NUMBER AS PRIMARYLANGUAGE,
                data_payload:DONOTARCHIVE::BOOLEAN AS DONOTARCHIVE,
                data_payload:ID::NUMBER AS ID,
                data_payload:PRIMARYLOCALE::NUMBER AS PRIMARYLOCALE,
                CAST(data_payload:PRODUCTCODE::TEXT AS VARCHAR(64)) AS PRODUCTCODE,
                CAST(data_payload:EXCLUDEREASON::TEXT AS VARCHAR(255)) AS EXCLUDEREASON,
                data_payload:GROUPNUMBERFROMPORTAL::NUMBER AS GROUPNUMBERFROMPORTAL,
                data_payload:CREATEUSERID::NUMBER AS CREATEUSERID,
                data_payload:ARCHIVEFAILUREID::NUMBER AS ARCHIVEFAILUREID,
                CAST(data_payload:CRNNUMBER_ICARE::TEXT AS VARCHAR(60)) AS CRNNUMBER_ICARE,
                TO_TIMESTAMP_NTZ(data_payload:ORIGINALEFFECTIVEDATE::NUMBER/1000) AS ORIGINALEFFECTIVEDATE,
                data_payload:BEANVERSION::NUMBER AS BEANVERSION,
                data_payload:ARCHIVEPARTITION::NUMBER AS ARCHIVEPARTITION,
                data_payload:RETIRED::NUMBER AS RETIRED,
                data_payload:UPDATEUSERID::NUMBER AS UPDATEUSERID,
                data_payload:PRIORTOTALINCURRED::NUMBER AS PRIORTOTALINCURRED,
                TO_TIMESTAMP_NTZ(data_payload:ARCHIVEDATE::NUMBER/1000) AS ARCHIVEDATE,
                data_payload:PRIORTOTALINCURRED_CUR::NUMBER AS PRIORTOTALINCURRED_CUR,
                data_payload:PRODUCERCODEOFSERVICEID::NUMBER AS PRODUCERCODEOFSERVICEID,
                data_payload:NEWPRODUCERCODE_EXT::NUMBER AS NEWPRODUCERCODE_EXT,
                data_payload:NEWCLAIMSCHEMEAGENT_ICARE::NUMBER AS NEWCLAIMSCHEMEAGENT_ICARE,
                CAST(data_payload:MOVEDPOLSRCACCTPUBID::TEXT AS VARCHAR(64)) AS MOVEDPOLSRCACCTPUBID,
                CAST(data_payload:AGENCYCONTACTDETAILS_EXT::TEXT AS VARCHAR(255)) AS AGENCYCONTACTDETAILS_EXT,
                CAST(data_payload:AGENCYCONTACTEMAIL_EXT::TEXT AS VARCHAR(255)) AS AGENCYCONTACTEMAIL_EXT,
                CAST(data_payload:AGENCYCONTACTNUMBER_EXT::TEXT AS VARCHAR(255)) AS AGENCYCONTACTNUMBER_EXT,
                data_payload:INSURANCEBOOK_EXTID::NUMBER AS INSURANCEBOOK_EXTID,
                CAST(NULL AS VARCHAR(120)) as lsn,
                CAST(NULL AS VARCHAR(2)) as row_operation,
                CAST(NULL AS NUMBER) as txn_id,
                CAST(NULL AS  VARCHAR(120)) as seqval_hex,
                {{ dbt_utils.generate_surrogate_key([
                                'donotdestroy',
                        'isportalpolicy_icare',
                        'publicid',
                        'priorpremiums',
                        'issuedate',
                        'priorpremiums_cur',
                        'movedpolicysourceaccountid',
                        'accountid',
                        'createtime',
                        'losshistorytype',
                        'excludedfromarchive',
                        'archivestate',
                        'archiveschemainfo',
                        'archivefailuredetailsid',
                        'packagerisk',
                        'numpriorlosses',
                        'updatetime',
                        'primarylanguage',
                        'donotarchive',
                        'id',
                        'primarylocale',
                        'productcode',
                        'excludereason',
                        'groupnumberfromportal',
                        'createuserid',
                        'archivefailureid',
                        'crnnumber_icare',
                        'originaleffectivedate',
                        'beanversion',
                        'archivepartition',
                        'retired',
                        'updateuserid',
                        'priortotalincurred',
                        'archivedate',
                        'priortotalincurred_cur',
                        'producercodeofserviceid',
                        'newproducercode_ext',
                        'newclaimschemeagent_icare',
                        'movedpolsrcacctpubid',
                        'agencycontactdetails_ext',
                        'agencycontactemail_ext',
                        'agencycontactnumber_ext',
                        'insurancebook_extid'
                        ]) }} AS id_hash,
                metadata_file_name,
                file_ingestion_timestamp,
                CAST(NULL AS TIMESTAMP_NTZ) AS update_timestamp
            FROM {{ source('gwpc', 'pc_policy') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'avro'
            UNION ALL 
            SELECT
                $1:donotdestroy::BOOLEAN AS donotdestroy,
                $1:isportalpolicy_icare::BOOLEAN AS isportalpolicy_icare,
                CAST($1:publicid::TEXT AS VARCHAR(64)) AS publicid,
                $1:priorpremiums::NUMBER AS priorpremiums,
                $1:issuedate::TIMESTAMP_NTZ AS issuedate,
                $1:priorpremiums_cur::NUMBER AS priorpremiums_cur,
                $1:movedpolicysourceaccountid::NUMBER AS movedpolicysourceaccountid,
                $1:accountid::NUMBER AS accountid,
                $1:createtime::TIMESTAMP_NTZ AS createtime,
                $1:losshistorytype::NUMBER AS losshistorytype,
                $1:excludedfromarchive::BOOLEAN AS excludedfromarchive,
                $1:archivestate::NUMBER AS archivestate,
                $1:archiveschemainfo::NUMBER AS archiveschemainfo,
                $1:archivefailuredetailsid::NUMBER AS archivefailuredetailsid,
                $1:packagerisk::NUMBER AS packagerisk,
                $1:numpriorlosses::NUMBER AS numpriorlosses,
                $1:updatetime::TIMESTAMP_NTZ AS updatetime,
                $1:primarylanguage::NUMBER AS primarylanguage,
                $1:donotarchive::BOOLEAN AS donotarchive,
                $1:id::NUMBER AS id,
                $1:primarylocale::NUMBER AS primarylocale,
                CAST($1:productcode::TEXT AS VARCHAR(64)) AS productcode,
                CAST($1:excludereason::TEXT AS VARCHAR(255)) AS excludereason,
                $1:groupnumberfromportal::NUMBER AS groupnumberfromportal,
                $1:createuserid::NUMBER AS createuserid,
                $1:archivefailureid::NUMBER AS archivefailureid,
                CAST($1:crnnumber_icare::TEXT AS VARCHAR(60)) AS crnnumber_icare,
                $1:originaleffectivedate::TIMESTAMP_NTZ AS originaleffectivedate,
                $1:beanversion::NUMBER AS beanversion,
                $1:archivepartition::NUMBER AS archivepartition,
                $1:retired::NUMBER AS retired,
                $1:updateuserid::NUMBER AS updateuserid,
                $1:priortotalincurred::NUMBER AS priortotalincurred,
                $1:archivedate::TIMESTAMP_NTZ AS archivedate,
                $1:priortotalincurred_cur::NUMBER AS priortotalincurred_cur,
                $1:producercodeofserviceid::NUMBER AS producercodeofserviceid,
                $1:newproducercode_ext::NUMBER AS newproducercode_ext,
                $1:newclaimschemeagent_icare::NUMBER AS newclaimschemeagent_icare,
                CAST($1:movedpolsrcacctpubid::TEXT AS VARCHAR(64)) AS movedpolsrcacctpubid,
                CAST($1:agencycontactdetails_ext::TEXT AS VARCHAR(255)) AS agencycontactdetails_ext,
                CAST($1:agencycontactemail_ext::TEXT AS VARCHAR(255)) AS agencycontactemail_ext,
                CAST($1:agencycontactnumber_ext::TEXT AS VARCHAR(255)) AS agencycontactnumber_ext,
                $1:insurancebook_extid::NUMBER AS insurancebook_extid,
                CAST($1:gwcbi___lsn::STRING AS VARCHAR(120)) as lsn,
                CAST($1:gwcbi___operation::STRING AS VARCHAR(2)) as row_operation,
                $1:gwcbi___tx_id::NUMBER  as txn_id,
                CAST($1:gwcbi___seqval_hex::STRING AS VARCHAR(120))  as seqval_hex,
                {{ dbt_utils.generate_surrogate_key([
                                'ID',
                        'seqval_hex'
                        ]) }} AS id_hash,
                metadata_file_name,
                file_ingestion_timestamp,
                TO_TIMESTAMP($1:gwcbi___cdc_tx_date::NUMBER / 1000) as update_timestamp
            FROM {{ source('gwpc', 'pc_policy') }}
            WHERE REGEXP_SUBSTR(metadata_file_name, '[^.]+$') = 'parquet'
            
),
{#-
    Driving CTE Over 
-#}   
transformed AS (
    SELECT
        *
    FROM source_data
)
SELECT * FROM transformed
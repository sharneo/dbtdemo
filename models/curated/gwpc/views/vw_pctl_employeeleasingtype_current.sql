
{#-
Project: Data Uplift Program
Project Description/Purpose: Data Uplift Program

Date            Version         Author          Description of Change           
2026.01.11      0.0                             This Creates a Current View for the Curated Layer for the Table  
                                                pctl_employeeleasingtype . This view contains only the Latest Record Data in the Table
                                                This View is an Initial One and will need to be revisited. 
                                                1 and 0 Represent Initial Initialisation from CDA
-#}

{{ config(
    materialized='view',
    tags=["daily", "curated","curated_view", "hourly", "policy"]
) }}

SELECT
    {{ dbt_utils.star(from=ref('pctl_employeeleasingtype')  ) }}
FROM {{ ref('pctl_employeeleasingtype') }}
WHERE FILE_TYPE='PARQUET'
      AND GWCBI_OPERATION NOT IN ('0','1')
QUALIFY ROW_NUMBER() OVER (
     PARTITION BY ID 
     ORDER BY GWCBI_SEQVAL DESC 
) = 1

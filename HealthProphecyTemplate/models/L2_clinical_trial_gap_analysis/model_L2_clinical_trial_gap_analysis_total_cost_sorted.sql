{{
  config({    
    "materialized": "table",
    "alias": "prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__total_cost_sorted",
    "database": "product_dev",
    "schema": "default"
  })
}}

WITH total_cost_by_reason_code AS (

  SELECT * 
  
  FROM {{ source('prophecy_tmp_source__L2_clinical_trial_gap_analysis', 'prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__total_cost_by_reason_code') }}

),

total_cost_sorted AS (

  SELECT * 
  
  FROM total_cost_by_reason_code AS in0
  
  ORDER BY total_cost DESC

)

SELECT *

FROM total_cost_sorted

{{
  config({    
    "materialized": "table",
    "alias": "prophecy_tmp__mbh9lpvn__L2_clinical_trial_gap_analysis__total_cost_by_reason_code",
    "database": "product_dev",
    "schema": "default"
  })
}}

WITH L0_raw_encounters AS (

  SELECT * 
  
  FROM {{ source('analyst_samples.healthcare', 'L0_raw_encounters') }}

),

valid_encounters AS (

  SELECT * 
  
  FROM L0_raw_encounters AS in0
  
  WHERE baseEncounterCost > 0

),

total_cost_by_reason_code AS (

  SELECT 
    reasonCode AS snomed_diagnosis,
    sum(baseEncounterCost) AS total_cost
  
  FROM valid_encounters AS in0
  
  GROUP BY reasonCode

)

SELECT *

FROM total_cost_by_reason_code

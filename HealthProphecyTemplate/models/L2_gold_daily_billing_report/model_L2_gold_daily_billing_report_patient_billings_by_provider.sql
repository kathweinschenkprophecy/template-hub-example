{{
  config({    
    "materialized": "incremental",
    "alias": "L2_gold_billing_daily_report",
    "database": "playground",
    "incremental_strategy": "append",
    "schema": "playground_schema_11083_gd4"
  })
}}

WITH L0_raw_encounters AS (

  SELECT * 
  
  FROM {{ source('analyst_samples.healthcare', 'L0_raw_encounters') }}

),

encounter_details AS (

  SELECT 
    DATE(start) AS encounterDate,
    patient AS patient,
    provider AS provider,
    baseEncounterCost AS baseEncounterCost
  
  FROM L0_raw_encounters AS in0

),

encounter_by_date AS (

  SELECT * 
  
  FROM encounter_details AS in0
  
  WHERE encounterDate = {{ var('executionDate') }}

),

patient_billings_by_provider AS (

  SELECT 
    encounterDate AS day,
    provider AS provider,
    COUNT(patient) AS numPatients,
    SUM(baseEncounterCost) AS billings
  
  FROM encounter_by_date AS in0
  
  GROUP BY 
    encounterDate, provider

)

SELECT *

FROM patient_billings_by_provider

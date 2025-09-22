{{
  config({    
    "materialized": "table",
    "alias": "L1_silver_hipaa_compliant_patients",
    "database": "playground",
    "schema": "playground_schema_11083_gd4"
  })
}}

WITH L0_raw_patients AS (

  SELECT * 
  
  FROM {{ source('analyst_samples.healthcare', 'L0_raw_patients') }}

),

hashed_patient_ids AS (

  SELECT 
    sha2(id, 256) AS hashedId,
    birthDate AS birthDate,
    deathDate AS deathDate,
    marital AS marital,
    race AS race,
    ethnicity AS ethnicity,
    gender AS gender,
    state AS state,
    zip AS zip,
    income AS income
  
  FROM L0_raw_patients AS in0

),

clean_dates AS (

  SELECT 
    hashedId AS hashedId,
    CASE
      WHEN YEAR(CURRENT_DATE) - YEAR(birthDate) > 89
        THEN '90+'
      ELSE CAST(YEAR(CURRENT_DATE) - YEAR(birthDate) AS STRING)
    END AS age,
    CASE
      WHEN YEAR(current_date()) - YEAR(birthDate) > 89
        THEN '90+'
      ELSE CAST(YEAR(deathDate) AS STRING)
    END AS deathDate,
    marital AS marital,
    race AS race,
    ethnicity AS ethnicity,
    gender AS gender,
    state AS state,
    zip AS zip,
    income AS income
  
  FROM hashed_patient_ids AS in0

),

clean_geo AS (

  SELECT 
    hashedId AS hashedId,
    age AS age,
    marital AS marital,
    race AS race,
    ethnicity AS ethnicity,
    gender AS gender,
    state AS state,
    CASE
      WHEN SUBSTRING(zip, 1, 3) IN ('036', '059', '063', '102', '203', '556', '692', '790', '821', '823', '830', '831', '878', '879', '884', '890', '893')
        THEN '000'
      ELSE SUBSTRING(zip, 1, 3)
    END AS zip3,
    income AS income
  
  FROM clean_dates AS in0

)

SELECT *

FROM clean_geo

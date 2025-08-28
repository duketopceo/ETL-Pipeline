-- ===================================================================================
-- BigQuery Transformation Script for RoofLink Data (V2)
-- ===================================================================================
-- This script is designed to run in Google BigQuery. It mirrors the logic
-- from the Python ETL pipeline (etl_pipeline_logic.py) to transform raw
-- RoofLink CSV data into a cleaned, structured table suitable for Looker.

-- V2 Changes:
-- - More conservative missing data handling (fewer COALESCE calls).
-- - Added `is_complete` flag to identify high-quality records.
-- - Added calculated fields (`customer_full_name`, `is_insurance_claim`, `job_duration_days`).

-- Instructions:
-- 1. Load your raw CSV data into a BigQuery table. For this script, we assume
--    it's loaded into a table named `your_project.your_dataset.rooflink_raw_data`.
-- 2. Replace `your_project.your_dataset` with your actual BigQuery project and dataset IDs.
-- 3. Run the CREATE OR REPLACE TABLE statement below.

-- ===================================================================================
-- Step 1: Define the Cleaned Table Structure (V2)
-- ===================================================================================

CREATE OR REPLACE TABLE `your_project.your_dataset.rooflink_cleaned_data`
(
  -- Core Identifier
  job_id INT64 OPTIONS(description="The unique identifier for a job, extracted from the URL."),

  -- Customer Information
  customer_first_name STRING,
  customer_last_name STRING,
  customer_address STRING,
  customer_city STRING,
  customer_state STRING,
  customer_zipcode STRING,
  customer_phone STRING,
  customer_cell STRING,
  customer_email STRING,
  customer_region STRING,
  customer_rep STRING,
  customer_alt_rep STRING,
  customer_marketing_rep STRING,
  customer_lead_source STRING,

  -- Job/Address Information
  address STRING,
  city STRING,
  state STRING,
  zipcode STRING,

  -- Status & Dates
  status_label STRING,
  lead_status_label STRING,
  date_created DATETIME,
  date_approved DATETIME,
  date_signed DATETIME,
  date_rd_released DATETIME,
  date_completed DATETIME,
  date_roof_completed DATETIME,
  date_closed DATETIME,

  -- Estimate / Financials
  estimate_date_last_edited DATETIME,
  estimate_gt_margin FLOAT64,
  estimate_estimate_gross_margin FLOAT64,
  estimate_owes FLOAT64,
  estimate_total FLOAT64,

  -- Other Details
  roofing_crew STRING,
  last_note_message STRING,

  -- Insurance Information
  insurance_company_name STRING,
  insurance_company_claims_phone STRING,
  insurance_company_claims_extension STRING,
  insurance_company_claims_email STRING,
  claim_handler_name STRING,
  claim_handler_phone STRING,
  claim_handler_email STRING,

  -- V2 Calculated Fields for Looker
  is_complete BOOL OPTIONS(description="Flag to indicate if a row has all critical customer info (job_id, name, address)."),
  customer_full_name STRING OPTIONS(description="Combination of customer_first_name and customer_last_name."),
  is_insurance_claim BOOL OPTIONS(description="Flag to indicate if the job is an insurance claim."),
  job_duration_days INT64 OPTIONS(description="Number of days from job creation to completion.")
);

-- ===================================================================================
-- Step 2: Transform and Insert Data (V2)
-- ===================================================================================

INSERT INTO `your_project.your_dataset.rooflink_cleaned_data`
WITH
  -- CTE 1: Extract Job ID and perform initial data type casting
  RawDataCasted AS (
    SELECT
      -- Extract the number from the URL to create job_id
      SAFE_CAST(REGEXP_EXTRACT(`Full_url`, r'(\\d+)') AS INT64) AS job_id,

      -- Cast all other relevant columns, keeping them as close to raw as possible
      `Customer___First_name` AS customer_first_name,
      `Customer___Last_name` AS customer_last_name,
      `Customer___Address` AS customer_address,
      `Address` AS address, -- Note: There are two address fields, we keep both
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_created`) AS date_created,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_completed`) AS date_completed,
      `Insurance_company___Name` AS insurance_company_name,

      -- All other columns needed for the final table
      `Customer___City` AS customer_city,
      `Customer___State` AS customer_state,
      `Customer___Zipcode` AS customer_zipcode,
      `Customer___Phone` AS customer_phone,
      `Customer___Cell` AS customer_cell,
      `Customer___Email` AS customer_email,
      `Customer___Region` AS customer_region,
      `Customer___Rep` AS customer_rep,
      `Customer___Alt_rep` AS customer_alt_rep,
      `Customer___Marketing_rep` AS customer_marketing_rep,
      `Customer___Lead_source` AS customer_lead_source,
      `City` AS city,
      `State` AS state,
      `Zipcode` AS zipcode,
      `Status_label` AS status_label,
      `Lead_status_label` AS lead_status_label,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_approved`) AS date_approved,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_signed`) AS date_signed,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_rd_released`) AS date_rd_released,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_roof_completed`) AS date_roof_completed,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_closed`) AS date_closed,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Estimate___Date_last_edited`) AS estimate_date_last_edited,
      SAFE_CAST(`Estimate___Gt_margin` AS FLOAT64) AS estimate_gt_margin,
      SAFE_CAST(`Estimate___Estimate_Gross_Margin` AS FLOAT64) AS estimate_estimate_gross_margin,
      SAFE_CAST(`Estimate___Owes` AS FLOAT64) AS estimate_owes,
      SAFE_CAST(`Estimate___Total` AS FLOAT64) AS estimate_total,
      `Roofing_crew` AS roofing_crew,
      `Last_note_message` AS last_note_message,
      `Insurance_company___Claims_phone` AS insurance_company_claims_phone,
      `Insurance_company___Claims_extension` AS insurance_company_claims_extension,
      `Insurance_company___Claims_email` AS insurance_company_claims_email,
      `Claim_handler___Name` AS claim_handler_name,
      `Claim_handler___Phone` AS claim_handler_phone,
      `Claim_handler___Email` AS claim_handler_email,

      TO_JSON_STRING(t) AS original_row
    FROM
      `your_project.your_dataset.rooflink_raw_data` AS t
  ),

  -- CTE 2: Remove duplicates based on job_id
  Deduplicated AS (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY FARM_FINGERPRINT(original_row)) as rn
    FROM RawDataCasted
    WHERE job_id IS NOT NULL
  ),

  -- CTE 3: Apply final transformations and calculated fields
  FinalCalculations AS (
    SELECT
        *,
        -- is_complete Flag
        (job_id IS NOT NULL AND customer_first_name IS NOT NULL AND customer_last_name IS NOT NULL AND address IS NOT NULL) AS is_complete,

        -- customer_full_name
        TRIM(CONCAT(COALESCE(customer_first_name, ''), ' ', COALESCE(customer_last_name, ''))) AS customer_full_name,

        -- is_insurance_claim Flag
        (insurance_company_name IS NOT NULL AND insurance_company_name != 'Unknown') AS is_insurance_claim,

        -- job_duration_days
        DATE_DIFF(date_completed, date_created, DAY) AS job_duration_days

    FROM Deduplicated
    WHERE rn = 1
  )

-- Final SELECT statement with conservative COALESCE
SELECT
  job_id,
  customer_first_name,
  customer_last_name,
  customer_address,
  customer_city,
  customer_state,
  customer_zipcode,
  customer_phone,
  customer_cell,
  customer_email,
  COALESCE(customer_region, 'Unknown') AS customer_region,
  COALESCE(customer_rep, 'Unknown') AS customer_rep,
  customer_alt_rep,
  customer_marketing_rep,
  COALESCE(customer_lead_source, 'Unknown') AS customer_lead_source,
  address,
  city,
  state,
  zipcode,
  COALESCE(status_label, 'Unknown') AS status_label,
  COALESCE(lead_status_label, 'Unknown') AS lead_status_label,
  date_created,
  date_approved,
  date_signed,
  date_rd_released,
  date_completed,
  date_roof_completed,
  date_closed,
  estimate_date_last_edited,
  estimate_gt_margin,
  estimate_estimate_gross_margin,
  estimate_owes,
  estimate_total,
  roofing_crew,
  last_note_message,
  insurance_company_name,
  insurance_company_claims_phone,
  insurance_company_claims_extension,
  insurance_company_claims_email,
  claim_handler_name,
  claim_handler_phone,
  claim_handler_email,
  -- New V2 Fields
  is_complete,
  customer_full_name,
  is_insurance_claim,
  job_duration_days
FROM FinalCalculations;

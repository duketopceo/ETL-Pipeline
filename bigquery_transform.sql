-- ===================================================================================
-- BigQuery Transformation Script for RoofLink Data
-- ===================================================================================
-- This script is designed to run in Google BigQuery. It mirrors the logic
-- from the Python ETL pipeline (etl_pipeline_logic.py) to transform raw
-- RoofLink CSV data into a cleaned, structured table.

-- Instructions:
-- 1. Load your raw CSV data into a BigQuery table. For this script, we assume
--    it's loaded into a table named `your_project.your_dataset.rooflink_raw_data`.
-- 2. Replace `your_project.your_dataset` with your actual BigQuery project and dataset IDs.
-- 3. Run the CREATE OR REPLACE TABLE statement below.

-- ===================================================================================
-- Step 1: Define the Cleaned Table Structure
-- ===================================================================================
-- This statement creates the final table with a clean schema. Column names are
-- standardized to snake_case, and data types are explicitly set.
-- Note: Some columns from the raw data might be dropped if they are mostly empty.
-- This schema represents the expected final structure.

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

  -- Job/Address Information (can sometimes be duplicates of customer info)
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
  claim_handler_email STRING
);

-- ===================================================================================
-- Step 2: Transform and Insert Data
-- ===================================================================================
-- This statement reads from the raw table, applies all the cleaning logic,
-- and inserts the result into our new cleaned table.

INSERT INTO `your_project.your_dataset.rooflink_cleaned_data`
WITH
  -- CTE 1: Extract Job ID and perform initial data type casting
  -- Note: BigQuery replaces unsupported characters like '/' in column names with underscores.
  -- The names used here (`Customer___First_name`) assume the raw CSV was loaded directly.
  RawDataCasted AS (
    SELECT
      -- Extract the number from the URL to create job_id
      CAST(REGEXP_EXTRACT(`Full_url`, r'(\d+)') AS INT64) AS job_id,

      -- These are the raw column names from the CSV
      `Customer___First_name` AS customer_first_name,
      `Customer___Last_name` AS customer_last_name,
      `Customer___Address` AS customer_address,
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
      `Address` AS address,
      `City` AS city,
      `State` AS state,
      `Zipcode` AS zipcode,
      `Status_label` AS status_label,
      `Lead_status_label` AS lead_status_label,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_created`) AS date_created,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_approved`) AS date_approved,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_signed`) AS date_signed,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_rd_released`) AS date_rd_released,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_completed`) AS date_completed,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_roof_completed`) AS date_roof_completed,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Date_closed`) AS date_closed,
      SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M%p', `Estimate___Date_last_edited`) AS estimate_date_last_edited,
      SAFE_CAST(`Estimate___Gt_margin` AS FLOAT64) AS estimate_gt_margin,
      SAFE_CAST(`Estimate___Estimate_Gross_Margin` AS FLOAT64) AS estimate_estimate_gross_margin,
      SAFE_CAST(`Estimate___Owes` AS FLOAT64) AS estimate_owes,
      SAFE_CAST(`Estimate___Total` AS FLOAT64) AS estimate_total,
      `Roofing_crew` AS roofing_crew,
      `Last_note_message` AS last_note_message,
      `Insurance_company___Name` AS insurance_company_name,
      `Insurance_company___Claims_phone` AS insurance_company_claims_phone,
      `Insurance_company___Claims_extension` AS insurance_company_claims_extension,
      `Insurance_company___Claims_email` AS insurance_company_claims_email,
      `Claim_handler___Name` AS claim_handler_name,
      `Claim_handler___Phone` AS claim_handler_phone,
      `Claim_handler___Email` AS claim_handler_email,

      -- Keep the original row to calculate a unique hash for duplicate detection
      TO_JSON_STRING(t) AS original_row
    FROM
      `your_project.your_dataset.rooflink_raw_data` AS t
  ),

  -- CTE 2: Handle missing values and remove duplicates
  CleanedAndDeduplicated AS (
    SELECT
      *,
      -- Assign a row number to each row, partitioned by job_id.
      -- Order by the hash of the original row to ensure consistent tie-breaking.
      ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY FARM_FINGERPRINT(original_row)) as rn
    FROM RawDataCasted
    WHERE job_id IS NOT NULL -- Exclude rows where a job_id could not be extracted
  )

-- Final SELECT statement
-- Select only the first instance of each job_id (rn = 1)
SELECT
  job_id,
  -- Use COALESCE to fill missing values, similar to the Python script's fillna logic
  COALESCE(customer_first_name, 'Unknown') AS customer_first_name,
  COALESCE(customer_last_name, 'Unknown') AS customer_last_name,
  COALESCE(customer_address, 'Unknown') AS customer_address,
  COALESCE(customer_city, 'Unknown') AS customer_city,
  COALESCE(customer_state, 'Unknown') AS customer_state,
  COALESCE(customer_zipcode, 'Unknown') AS customer_zipcode,
  COALESCE(customer_phone, 'Unknown') AS customer_phone,
  COALESCE(customer_cell, 'Unknown') AS customer_cell,
  COALESCE(customer_email, 'Unknown') AS customer_email,
  COALESCE(customer_region, 'Unknown') AS customer_region,
  COALESCE(customer_rep, 'Unknown') AS customer_rep,
  COALESCE(customer_alt_rep, 'Unknown') AS customer_alt_rep,
  COALESCE(customer_marketing_rep, 'Unknown') AS customer_marketing_rep,
  COALESCE(customer_lead_source, 'Unknown') AS customer_lead_source,
  COALESCE(address, 'Unknown') AS address,
  COALESCE(city, 'Unknown') AS city,
  COALESCE(state, 'Unknown') AS state,
  COALESCE(zipcode, 'Unknown') AS zipcode,
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
  -- For numeric, we can fill with 0 or a median value if known. Here we use 0.
  COALESCE(estimate_gt_margin, 0) AS estimate_gt_margin,
  COALESCE(estimate_estimate_gross_margin, 0) AS estimate_estimate_gross_margin,
  COALESCE(estimate_owes, 0) AS estimate_owes,
  COALESCE(estimate_total, 0) AS estimate_total,
  COALESCE(roofing_crew, 'Unknown') AS roofing_crew,
  COALESCE(last_note_message, 'None') AS last_note_message,
  COALESCE(insurance_company_name, 'Unknown') AS insurance_company_name,
  COALESCE(insurance_company_claims_phone, 'Unknown') AS insurance_company_claims_phone,
  COALESCE(insurance_company_claims_extension, 'Unknown') AS insurance_company_claims_extension,
  COALESCE(insurance_company_claims_email, 'Unknown') AS insurance_company_claims_email,
  COALESCE(claim_handler_name, 'Unknown') AS claim_handler_name,
  COALESCE(claim_handler_phone, 'Unknown') AS claim_handler_phone,
  COALESCE(claim_handler_email, 'Unknown') AS claim_handler_email
FROM CleanedAndDeduplicated
WHERE rn = 1;

-- ================================================================
-- BigQuery ETL SQL Pipeline - Refined & Documented
-- Author: Jules
-- Version: 1.1
--
-- Purpose:
-- This script transforms raw job posting data into a cleaned, structured,
-- and deduplicated table. It mirrors the logic of the Python ETL
-- script, providing an in-database alternative for data processing.
--
-- Instructions:
-- 1. Replace `your_project.your_dataset.raw_data_table` with your
--    actual source table name.
-- 2. Adjust the column names in the `SourceData` CTE to match the
--    columns in your raw table (e.g., `job_url`, `company`, `salary`).
-- ================================================================

WITH
  -- Step 1: Source and Initial Deduplication
  -- Select data from the source table and remove any exact duplicate rows
  -- based on the job URL, keeping the most recent entry if a timestamp is available.
  SourceData AS (
    SELECT
      *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY job_url ORDER BY posted_date DESC) as rn
      FROM
        `your_project.your_dataset.raw_data_table`
    )
    WHERE rn = 1
  ),

  -- Step 2: Extract Job ID and Clean Strings
  -- This CTE extracts the numeric job_id from the URL, which is the primary key
  -- for joining datasets. It also trims whitespace from string fields.
  WithJobId AS (
    SELECT
      -- Extracts digits from a URL like '.../job/12345/...'
      SAFE_CAST(REGEXP_EXTRACT(job_url, r'/job/(\d+)') AS INT64) AS job_id,
      TRIM(company) AS company,
      TRIM(location) AS location,
      -- Cleans salary by removing currency symbols, commas, etc.
      SAFE_CAST(REGEXP_REPLACE(salary, r'[^0-9.]', '') AS FLOAT64) AS salary,
      posted_date
    FROM
      SourceData
  ),

  -- Step 3: Type Casting and Validation
  -- Robustly convert columns to their target data types using SAFE_CAST.
  -- This prevents errors from bad data and turns uncastable values into NULLs.
  -- We also filter out any rows where a job_id could not be extracted,
  -- as they cannot be used for merging.
  TypeCasted AS (
    SELECT
      job_id,
      company,
      location,
      salary,
      SAFE_CAST(posted_date AS DATETIME) AS posted_datetime
    FROM
      WithJobId
    WHERE
      job_id IS NOT NULL
  ),

  -- Step 4: Calculate Imputation Values
  -- To handle missing data, we pre-calculate the values to be used for imputation.
  -- For numeric columns (salary), we use the median.
  -- For categorical columns (company, location), we use the mode (most frequent value).
  ImputationValues AS (
    SELECT
      APPROX_QUANTILES(salary, 2 IGNORE NULLS)[OFFSET(1)] AS median_salary,
      APPROX_TOP_COUNT(company, 1 IGNORE NULLS)[OFFSET(0)].value AS mode_company,
      APPROX_TOP_COUNT(location, 1 IGNORE NULLS)[OFFSET(0)].value AS mode_location
    FROM
      TypeCasted
  ),

  -- Step 5: Impute Missing Values
  -- This CTE fills in the NULL values from the previous step.
  -- COALESCE returns the first non-null value in its argument list.
  -- The CROSS JOIN makes the single row of ImputationValues available to every
  -- row in TypeCasted, allowing us to fill NULLs with the calculated stats.
  Imputed AS (
    SELECT
      job_id,
      COALESCE(company, iv.mode_company, 'Unknown') AS company,
      COALESCE(location, iv.mode_location, 'Unknown') AS location,
      COALESCE(salary, iv.median_salary) AS salary,
      posted_datetime
    FROM
      TypeCasted
    CROSS JOIN
      ImputationValues iv
  ),

  -- Step 6: Final Deduplication
  -- Although initial duplicates were removed, the cleaning and imputation process
  -- might create new duplicates. This final step ensures every row is unique.
  Deduplicated AS (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY posted_datetime DESC) as final_rn
    FROM
      Imputed
  )

-- Final SELECT Statement
-- Select the final, cleaned data, ensuring only one record per job_id.
SELECT
  job_id,
  company,
  location,
  salary,
  posted_datetime
FROM
  Deduplicated
WHERE
  final_rn = 1;

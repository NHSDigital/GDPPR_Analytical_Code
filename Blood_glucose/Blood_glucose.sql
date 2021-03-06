-- Databricks notebook source
-- MAGIC %md # GDPPR Blood Glucose

---- Table of all Journal Entries with a blood glucose SNOMED code
---- Note that any SNOMED code for OGTT/Non-Fasting that states that the time frame is not 120mins/2hours is classed as "indeterminable" as these timeframes are not used for diagnosing diabetes
CREATE OR REPLACE TEMPORARY VIEW ALL_BLOOD_GLUCOSE_JOURNALS AS
SELECT
  a.*,
  b.ConceptId_Description,
  TYPE_OF_BLOOD_GLUCOSE_RECORD,
  FASTING_STATUS
FROM
  gdppr_database.gdppr_table AS a
  INNER JOIN REF_DATA.BLOOD_GLUCOSE_REF_DATA AS b ON a.CODE = b.ConceptID
WHERE
  a.CODE IN (
    SELECT
      DISTINCT ConceptID
    FROM
      REF_DATA.BLOOD_GLUCOSE_REF_DATA
  )

-- COMMAND ----------

-- MAGIC %md ## JOURNAL BREAKDOWN

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ALL_NON_FASTING_BLOOD_GLUCOSE_JOURNALS AS 
SELECT * 
FROM ALL_BLOOD_GLUCOSE_JOURNALS
WHERE FASTING_STATUS IN ("NON-FASTING","RANDOM")

-- COMMAND ----------

---- HbA1c will always have fasting status of "anytime" because HbA1c levels are reflective of the blood glucose levels from the previous 2/3 months 
---- Random tests can only be used to diagnose diabetes and cannot be used to refute the possibility of diabetes
---- Every single SNOMED code relating to blood glucose is an observable entity so we expect a numeric value from each journal to assign a blood glucose category

CREATE OR REPLACE TEMPORARY VIEW ALL_NON_FASTING_BLOOD_GLUCOSE_JOURNALS_WITH_CATEGORY AS 
SELECT * ,
CASE WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD = "HBA1C" AND VALUE1_CONDITION >= 9 AND VALUE1_CONDITION < 20
     THEN "LOW"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD = "HBA1C" AND VALUE1_CONDITION >= 20 AND VALUE1_CONDITION < 42
     THEN "NORMAL"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD = "HBA1C" AND VALUE1_CONDITION >= 42 AND VALUE1_CONDITION <= 48
     THEN "PRE-DIABETES"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD = "HBA1C" AND VALUE1_CONDITION > 48 AND VALUE1_CONDITION <= 162
     THEN "DIABETES"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD IN ("BLOOD", "PLASMA", "SERUM") AND FASTING_STATUS = "NON-FASTING" AND VALUE1_CONDITION >= 1 AND VALUE1_CONDITION < 4
     THEN "LOW"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD IN ("BLOOD", "PLASMA", "SERUM") AND FASTING_STATUS = "NON-FASTING" AND VALUE1_CONDITION >= 4 AND VALUE1_CONDITION < 7.8
     THEN "NORMAL"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD IN ("BLOOD", "PLASMA", "SERUM") AND FASTING_STATUS = "NON-FASTING" AND VALUE1_CONDITION >= 7.8 AND VALUE1_CONDITION < 11.1
     THEN "PRE-DIABETES"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD IN ("BLOOD", "PLASMA", "SERUM") AND FASTING_STATUS = "NON-FASTING" AND VALUE1_CONDITION >= 11.1 AND VALUE1_CONDITION <= 150
     THEN "DIABETES"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD IN ("BLOOD", "PLASMA", "SERUM") AND FASTING_STATUS = "RANDOM" AND VALUE1_CONDITION >= 1 AND VALUE1_CONDITION < 11.1
     THEN "MORE-TESTING-NEEDED"
     WHEN TYPE_OF_BLOOD_GLUCOSE_RECORD IN ("BLOOD", "PLASMA", "SERUM") AND FASTING_STATUS = "RANDOM" AND VALUE1_CONDITION >= 11.1 AND VALUE1_CONDITION <= 150
     THEN "DIABETES"
     ELSE "INDETERMINABLE"
END AS CATEGORISED_BLOOD_GLUCOSE
FROM ALL_NON_FASTING_BLOOD_GLUCOSE_JOURNALS     

-- COMMAND ----------

---- We can only assign a blood glucose status to OGTT journals if it is matched with a fasting journal value - OGTT results are allocated those statuses on the basis of a combination of the fasting and 2 hour levels:
---- We have no low blood glucose threshold for the combined OGTT and fasting journals because these tests take place in controlled conditions (so there should be no fasting blood glucose values that meet the criteria of hypoglycaemia)
---- We can still allocate blood glucose statuses on a single fasting value
CREATE OR REPLACE TEMPORARY VIEW ALL_FASTING_BLOOD_GLUCOSE_JOURNALS AS 
SELECT * 
FROM ALL_BLOOD_GLUCOSE_JOURNALS 
WHERE FASTING_STATUS = "FASTING"


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ALL_OGTT_BLOOD_GLUCOSE_JOURNALS 
AS SELECT * 
FROM ALL_BLOOD_GLUCOSE_JOURNALS 
WHERE FASTING_STATUS = "OGTT"

-- COMMAND ----------

---- Drop value 2 condition in the Fasting journals (THEY ARE ALL NULL) and replace the value 2 condition with the OGTT journal value
---- Join together fasting and OGTT journals with left join - so we can still allocate blood glucose statuses to the fasting journals that are not matched with an OGTT journal
---- We need the value1_condition (fasting value) to be NON-NULL
CREATE OR REPLACE TEMPORARY VIEW FASTING_OGTT_MATCHED_JOURNALS AS
SELECT a.YEAR_OF_BIRTH,
a.SEX,
a.LSOA,
a.YEAR_OF_DEATH,
a.NHS_NUMBER,
a.ETHNIC,
a.PRACTICE,
a.GP_SYSTEM_SUPPLIER,
a.PROCESSED_TIMESTAMP,
a.REPORTING_PERIOD_END_DATE,
a.JOURNAL_REPORTING_PERIOD_END_DATE,
a.DATE,
a.RECORD_DATE,
a.CODE,
a.SENSITIVE_CODE,
a.EPISODE_CONDITION,
a.EPISODE_PRESCRIPTION,
a.VALUE1_CONDITION,
a.VALUE1_PRESCRIPTION,
b.VALUE1_CONDITION AS VALUE2_CONDITION,
b.VALUE1_PRESCRIPTION AS VALUE2_PRESCRIPTION,
a.LINKS,
a.ConceptId_Description,
a.TYPE_OF_BLOOD_GLUCOSE_RECORD,
a.FASTING_STATUS 
FROM ALL_FASTING_BLOOD_GLUCOSE_JOURNALS AS a
LEFT JOIN ALL_OGTT_BLOOD_GLUCOSE_JOURNALS AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.DATE = b.DATE
AND a.PRACTICE = b.PRACTICE
AND a.REPORTING_PERIOD_END_DATE = b.REPORTING_PERIOD_END_DATE
AND a.JOURNAL_REPORTING_PERIOD_END_DATE = b.JOURNAL_REPORTING_PERIOD_END_DATE


-- COMMAND ----------

---- We need the value1_condition (fasting value) to be NON-NULL
CREATE OR REPLACE TEMPORARY VIEW FASTING_OGTT_MATCHED_JOURNALS_NON_NULL AS
SELECT * FROM FASTING_OGTT_MATCHED_JOURNALS
WHERE VALUE1_CONDITION IS NOT NULL

-- COMMAND ----------

---- Val 1 condition is the fasting blood glucose value and value 2 condition the OGTT blood glucose value (2hour)
---- Clinician - OGTT tests take place in controlled conditions (when the individuals blood glucose is not too low) - i.e. we set the lower bound for plausibility as the threshold for hypoglycaemia
---- By setting the plausible lower bound to < 4 we lose 0.2% of journals (5,000 out of 2,000,000) journal values
---- Wherever we have a "LESS THAN" ensure its greater than the lower bound for hypoglycaemia
CREATE OR REPLACE TEMPORARY VIEW FASTING_OGTT_MATCHED_JOURNALS_WITH_CATEGORY AS
SELECT * ,
CASE WHEN VALUE1_CONDITION >= 4 AND VALUE1_CONDITION <= 5.4 AND VALUE2_CONDITION >= 4 AND VALUE2_CONDITION < 7.8
     THEN "NORMAL"
     WHEN VALUE1_CONDITION >= 4 AND VALUE1_CONDITION < 7 AND VALUE2_CONDITION >= 7.8 AND VALUE2_CONDITION < 11.1
     THEN "PRE-DIABETES"
     WHEN VALUE1_CONDITION >= 5.5 AND VALUE1_CONDITION <= 6.9 AND VALUE2_CONDITION >= 4 AND VALUE2_CONDITION < 7.8 
     THEN "PRE-DIABETES"
     WHEN VALUE1_CONDITION >= 4 AND ((VALUE1_CONDITION >= 7 AND VALUE1_CONDITION <= 150) OR (VALUE2_CONDITION >= 11.1 AND VALUE2_CONDITION <= 150))
     THEN "DIABETES"
     WHEN VALUE1_CONDITION >= 3.9 AND VALUE1_CONDITION <= 5.4 AND VALUE2_CONDITION IS NULL
     THEN "NORMAL"
     WHEN VALUE1_CONDITION >= 5.5 AND VALUE1_CONDITION <= 6.9 AND VALUE2_CONDITION IS NULL
     THEN "PRE-DIABETES"
     WHEN VALUE1_CONDITION >= 7 AND VALUE1_CONDITION <= 150 AND VALUE2_CONDITION IS NULL
     THEN "DIABETES"
     ELSE "INDETERMINABLE"
END AS CATEGORISED_BLOOD_GLUCOSE
FROM FASTING_OGTT_MATCHED_JOURNALS_NON_NULL

-- COMMAND ----------

-- MAGIC %md ### COMBINE BOTH BREAKDOWNS AND REMOVE INDETERMINABLE

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ALL_JOURNALS_WITH_BLOOD_GLUCOSE_STATUS AS
SELECT * 
FROM ALL_NON_FASTING_BLOOD_GLUCOSE_JOURNALS_WITH_CATEGORY
WHERE CATEGORISED_BLOOD_GLUCOSE != "INDETERMINABLE"

UNION

SELECT * 
FROM FASTING_OGTT_MATCHED_JOURNALS_WITH_CATEGORY
WHERE CATEGORISED_BLOOD_GLUCOSE != "INDETERMINABLE"

-- COMMAND ----------

-- MAGIC %md ## FIND MOST RECENT

-- COMMAND ----------

---- Get the most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW MAX_DATE AS
SELECT NHS_NUMBER
, MAX(DATE) AS DATE
FROM ALL_JOURNALS_WITH_BLOOD_GLUCOSE_STATUS
GROUP BY NHS_NUMBER

-- COMMAND ----------

---- full table of most recent records
CREATE OR REPLACE TEMPORARY VIEW TABLE_MOST_RECENT_RECORDS AS
SELECT a.*
FROM  ALL_JOURNALS_WITH_BLOOD_GLUCOSE_STATUS AS a
INNER JOIN MAX_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.DATE = b.DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REMOVE CONFLICTS

-- COMMAND ----------

---- Need to remove those with conflicting blood glucose status at their most recent record
CREATE OR REPLACE TEMPORARY VIEW TABLE_JOURNAL_COUNTS AS
SELECT NHS_NUMBER
, COUNT(DISTINCT CATEGORISED_BLOOD_GLUCOSE) AS COUNT_CATEGORIES
FROM TABLE_MOST_RECENT_RECORDS
GROUP BY NHS_NUMBER

-- COMMAND ----------

---- Any individual with conflicting blood glucose statuses at their most recent record is removed
CREATE OR REPLACE TEMPORARY VIEW BLOOD_GLUCOSE_MOST_RECENT_NO_CONFLICTS AS
SELECT *
FROM TABLE_MOST_RECENT_RECORDS
WHERE NHS_NUMBER IN (SELECT DISTINCT NHS_NUMBER FROM TABLE_JOURNAL_COUNTS WHERE COUNT_CATEGORIES = 1)
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 5 ORIGINAL TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## GDPPR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC JOURNALS

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # create spark df of dataset
-- MAGIC data = spark.table('gdppr_database.gdppr_table')
-- MAGIC 
-- MAGIC # create spark df of snomed ethnicity reference data
-- MAGIC ethnicity_ref_data = spark.table('reference_database.gdppr_ethnicity_mappings_table')
-- MAGIC 
-- MAGIC # join nhs data dictionary ethnic groups onto ethnicity snomed journals
-- MAGIC data_join = data.join(ethnicity_ref_data, data.CODE == ethnicity_ref_data.ConceptId,how='left') 
-- MAGIC 
-- MAGIC # import functions
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC ## remove non ethnicity snomed codes i.e. nulls
-- MAGIC snomed_no_null = data_join.where(col("PrimaryCode").isNotNull())
-- MAGIC 
-- MAGIC # remove unknown snomed ethnicity - need to change to is in list but can't work it out in pyspark yet
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "Z")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "z")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "X")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "x")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "99")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "9")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != "")
-- MAGIC snomed_no_null = snomed_no_null.filter(data_join.PrimaryCode != " ")
-- MAGIC 
-- MAGIC # turn into temp view
-- MAGIC snomed_no_null.createOrReplaceTempView('gdppr_ethnicity_journals')

-- COMMAND ----------

-- Group by nhs number, record date, and ethnicity to get one journal per ethnicity, per date, per patient (removes duplicate journals)
CREATE OR REPLACE TEMPORARY VIEW JOURNAL_GROUPBY AS
SELECT NHS_NUMBER
, RECORD_DATE
, PrimaryCode AS ETHNICITY
, COUNT(*) AS COUNT_RECORDS
FROM gdppr_ethnicity_journals
GROUP BY NHS_NUMBER
, RECORD_DATE
, PrimaryCode

-- COMMAND ----------

-- get most recent record date per patient
CREATE OR REPLACE TEMPORARY VIEW RECENT_JOURNAL_DATE AS
SELECT NHS_NUMBER
, MAX(RECORD_DATE) AS RECENT_ETHNICITY_DATE
FROM JOURNAL_GROUPBY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get most recent ethnicity journal for each patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_JOURNAL_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORD_DATE AS RECORDED_DATE
, "GDPPR_JOURNAL" AS SOURCE
, 1 AS PRIORITY
FROM JOURNAL_GROUPBY AS a
INNER JOIN RECENT_JOURNAL_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.RECORD_DATE = b.RECENT_ETHNICITY_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PATIENT

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # import functions
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC ## remove blank ethnic field
-- MAGIC field_no_null = data_join.filter(data_join.ETHNIC != "")
-- MAGIC 
-- MAGIC # remove unknowns - not stated
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "Z")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "z")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "X")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "x")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "99")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "9")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != "")
-- MAGIC field_no_null = field_no_null.filter(data_join.ETHNIC != " ")
-- MAGIC 
-- MAGIC ## remove null ethnic field
-- MAGIC field_no_null = field_no_null.where(col("ETHNIC").isNotNull())
-- MAGIC 
-- MAGIC # turn into temp view
-- MAGIC field_no_null.createOrReplaceTempView('gdppr_ethnicity_patients')

-- COMMAND ----------

-- Group by nhs number, reporting period end date, and ethnic field to get one ethnicity per date, per patient (removes duplicates)
CREATE OR REPLACE TEMPORARY VIEW PATIENT_GROUPBY AS
SELECT NHS_NUMBER
, REPORTING_PERIOD_END_DATE
, ETHNIC AS ETHNICITY
, COUNT(*) AS COUNT_RECORDS
FROM gdppr_ethnicity_patients
GROUP BY NHS_NUMBER
, REPORTING_PERIOD_END_DATE
, ETHNIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PATIENT_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(REPORTING_PERIOD_END_DATE) AS RECENT_ETHNICITY_DATE
FROM PATIENT_GROUPBY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get most recent ethnicity for each patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_PATIENT_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.REPORTING_PERIOD_END_DATE AS RECORDED_DATE
, "GDPPR_PATIENT" AS SOURCE
, 2 AS PRIORITY
FROM PATIENT_GROUPBY AS a
INNER JOIN PATIENT_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.REPORTING_PERIOD_END_DATE = b.RECENT_ETHNICITY_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # GDPPR ONLY

-- COMMAND ----------

-- append gdppr patient and gdppr journal records together
CREATE OR REPLACE TEMPORARY VIEW GDPPR_RECENT_ETHNICITY_JOINED AS

SELECT NHS_NUMBER
, ETHNICITY
, RECORDED_DATE
, SOURCE
, PRIORITY
FROM GDPPR_JOURNAL_ETHNICITY

UNION

SELECT NHS_NUMBER
, ETHNICITY
, RECORDED_DATE
, SOURCE 
, PRIORITY
FROM GDPPR_PATIENT_ETHNICITY

-- COMMAND ----------

-- get most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(RECORDED_DATE) AS RECENT_ETHNICITY_DATE
FROM GDPPR_RECENT_ETHNICITY_JOINED
GROUP BY NHS_NUMBER


-- COMMAND ----------

-- get most recent ethnicity record for each patient
CREATE OR REPLACE TEMPORARY VIEW GDPPR_ETHNICITY_RECENT AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORDED_DATE
, a.SOURCE
, a.PRIORITY
FROM GDPPR_RECENT_ETHNICITY_JOINED AS a
INNER JOIN GDPPR_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.RECORDED_DATE = b.RECENT_ETHNICITY_DATE 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REMOVE CONFLICTS - GDPPR

-- COMMAND ----------

-- find highest priority source for each patient (this is the minimum as GDPPR journal is priority 1, GDPPR patient is priority 2 etc.)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_MIN_PRIORITY AS
SELECT NHS_NUMBER
, MIN(PRIORITY) AS MIN_PRIORITY
FROM GDPPR_ETHNICITY_RECENT
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get prioritised ethnicity record for each patient (remove lower priority source records)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_PRIORITISED_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORDED_DATE
, a.SOURCE
, a.PRIORITY
FROM GDPPR_ETHNICITY_RECENT AS a
INNER JOIN GDPPR_MIN_PRIORITY AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.PRIORITY = b.MIN_PRIORITY

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## NULLIFY DUPLICATES - GDPPR

-- COMMAND ----------

-- count ethnicities per NHS number as any with more than one need to be nulled and removed (for now)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_DUPLICATE_ETHNICITY_NHSNUM AS
SELECT NHS_NUMBER
, COUNT(*) AS COUNT_ETHNICITIES
FROM GDPPR_PRIORITISED_ETHNICITY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- select only NHS numbers with one ethnicity i.e. remove patients who have more than one

CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_GDPPR AS
SELECT NHS_NUMBER
, ETHNICITY AS ETHNIC_CATEGORY_CODE
, RECORDED_DATE AS DATE_OF_ATTRIBUTION
, SOURCE AS DATA_SOURCE
FROM GDPPR_PRIORITISED_ETHNICITY
WHERE NHS_NUMBER IN (SELECT NHS_NUMBER FROM GDPPR_DUPLICATE_ETHNICITY_NHSNUM WHERE COUNT_ETHNICITIES = 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REMOVE DECEASED PATIENTS AND OPT OUTS - GDPPR

-- COMMAND ----------

-- create list of gdppr patients who are not deceased (gdppr does not include opt outs)
CREATE OR REPLACE TEMPORARY VIEW GDPPR_ALIVE_PATIENTS AS
SELECT DISTINCT NHS_NUMBER
FROM gdppr_database.gdppr_table
WHERE DATE_OF_DEATH IS null

-- COMMAND ----------

/* ##########################################################
###########  THIS IS THE FINAL TABLE  #######################
############################################################# */

-- pull only living patients

CREATE OR REPLACE TEMPORARY VIEW GDPPR_ETHNICITY_ASSET_V1 AS
SELECT NHS_NUMBER
, ETHNIC_CATEGORY_CODE
, DATE_OF_ATTRIBUTION
, DATA_SOURCE
FROM ETHNICITY_ASSET_GDPPR
WHERE NHS_NUMBER IN (SELECT NHS_NUMBER FROM GDPPR_ALIVE_PATIENTS)
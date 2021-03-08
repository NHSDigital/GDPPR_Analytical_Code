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
-- MAGIC ## HES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC ## GET THE CURRENT YEAR OF HES AND PREVIOUS 5 YEARS
-- MAGIC 
-- MAGIC # import necessary packages/functions
-- MAGIC from pyspark.sql.functions import expr, col, lit, concat, regexp_replace, upper, split, regexp_extract
-- MAGIC 
-- MAGIC # select years
-- MAGIC years = [row.year for row in spark.sql("SHOW TABLES IN sensitive_hes").filter(col("isTemporary")==False).withColumn("yeary", split(col("tableName"), '_')[2]).withColumn("year", regexp_extract(col("yeary"),"([0-2][0-9]{3})", 1)).select("year").filter(col("year") != '').distinct().sort(col("year").desc()).limit(6).collect()]
-- MAGIC 
-- MAGIC # print years for visual check
-- MAGIC print(years)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC OP

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # create 5 years of HES OP ethnicity records
-- MAGIC stmt = []
-- MAGIC for year in years:
-- MAGIC   stmt.append(f"""SELECT a.NEWNHSNO, b.APPTDATE, b.ETHNOS FROM sensitive_hes.hes_op_{year} AS a INNER JOIN hes.hes_op_{year} AS b ON a.ATTENDKEY = b.ATTENDKEY WHERE b.CR_GP_PRACTICE NOT IN ('Y','Q99')""")
-- MAGIC 
-- MAGIC # create temp table
-- MAGIC spark.sql('CREATE OR REPLACE TEMPORARY VIEW OP_5_YEAR AS ' + ' UNION '.join(stmt))

-- COMMAND ----------

-- remove unknown/blank/null ethnicities
CREATE OR REPLACE TEMPORARY VIEW HES_OP_NO_UNKNOWN AS
SELECT NEWNHSNO AS NHS_NUMBER
, APPTDATE
, ETHNOS AS ETHNICITY
FROM OP_5_YEAR
WHERE ETHNOS NOT IN ("Z", "z", "X", "x", "99", "9", "", " ")
AND ETHNOS IS NOT NULL

-- COMMAND ----------

-- Group by nhs number, appt date, and ethnos field to get one ethnicity per date, per patient (removes duplicates e.g. several appts in one day)
CREATE OR REPLACE TEMPORARY VIEW OP_GROUPBY AS
SELECT NHS_NUMBER
, APPTDATE
, ETHNICITY
, COUNT(*) AS COUNT_RECORDS
FROM HES_OP_NO_UNKNOWN
GROUP BY NHS_NUMBER
, APPTDATE
, ETHNICITY

-- COMMAND ----------

-- get most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW OP_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(APPTDATE) AS RECENT_ETHNICITY_DATE
FROM OP_GROUPBY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get most recent ethnicity for each patient
CREATE OR REPLACE TEMPORARY VIEW HES_OP_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.APPTDATE AS RECORDED_DATE
, "HES_OP" AS SOURCE
, 5 AS PRIORITY
FROM OP_GROUPBY AS a
INNER JOIN OP_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.APPTDATE = b.RECENT_ETHNICITY_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC AE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # create 5 years of HES AE ethnicity records
-- MAGIC stmt = []
-- MAGIC for year in years:
-- MAGIC   stmt.append(f"""SELECT a.NEWNHSNO
-- MAGIC , b.ARRIVALDATE
-- MAGIC , b.ETHNOS
-- MAGIC FROM sensitive_hes.hes_ae_{year} AS a
-- MAGIC LEFT JOIN  hes.hes_ae_{year} AS b 
-- MAGIC ON a.AEKEY = b.AEKEY
-- MAGIC WHERE b.CR_GP_PRACTICE NOT IN ('Y','Q99')""")
-- MAGIC 
-- MAGIC # create temp table
-- MAGIC spark.sql('CREATE OR REPLACE TEMPORARY VIEW AE_5_YEAR AS ' + ' UNION '.join(stmt))

-- COMMAND ----------

-- remove unknown/blank/null ethnicities
CREATE OR REPLACE TEMPORARY VIEW HES_AE_NO_UNKNOWN AS
SELECT NEWNHSNO AS NHS_NUMBER
, ARRIVALDATE
, ETHNOS AS ETHNICITY
FROM AE_5_YEAR
WHERE ETHNOS NOT IN ("Z", "z", "X", "x", "99", "9", "", " ")
AND ETHNOS IS NOT NULL

-- COMMAND ----------

-- Group by nhs number, arrival date, and ethnos field to get one ethnicity per date, per patient (removes duplicates e.g. several a+e trips in one day - unlikely?)
CREATE OR REPLACE TEMPORARY VIEW AE_GROUPBY AS
SELECT NHS_NUMBER
, ARRIVALDATE
, ETHNICITY
, COUNT(*) AS COUNT_RECORDS
FROM HES_AE_NO_UNKNOWN
GROUP BY NHS_NUMBER
, ARRIVALDATE
, ETHNICITY

-- COMMAND ----------

-- get most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW AE_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(ARRIVALDATE) AS RECENT_ETHNICITY_DATE
FROM AE_GROUPBY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get most recent ethnicity for each patient
CREATE OR REPLACE TEMPORARY VIEW HES_AE_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.ARRIVALDATE AS RECORDED_DATE
, "HES_AE" AS SOURCE
, 4 AS PRIORITY
FROM AE_GROUPBY AS a
INNER JOIN AE_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.ARRIVALDATE = b.RECENT_ETHNICITY_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC APC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # create 5 years of HES APC ethnicity records
-- MAGIC stmt = []
-- MAGIC for year in years:
-- MAGIC   stmt.append(f"""SELECT a.NEWNHSNO
-- MAGIC , b.EPIEND
-- MAGIC , b.ETHNOS
-- MAGIC FROM sensitive_hes.hes_apc_{year} AS a
-- MAGIC LEFT JOIN  hes.hes_apc_{year} AS b 
-- MAGIC ON a.EPIKEY = b.EPIKEY
-- MAGIC WHERE b.CR_GP_PRACTICE NOT IN ('Y','Q99')""")
-- MAGIC 
-- MAGIC # create temp table
-- MAGIC spark.sql('CREATE OR REPLACE TEMPORARY VIEW APC_5_YEAR AS ' + ' UNION '.join(stmt))

-- COMMAND ----------

-- remove unknown/blank/null ethnicities
CREATE OR REPLACE TEMPORARY VIEW HES_APC_NO_UNKNOWN AS
SELECT NEWNHSNO AS NHS_NUMBER
, EPIEND
, ETHNOS AS ETHNICITY
FROM APC_5_YEAR
WHERE ETHNOS NOT IN ("Z", "z", "X", "x", "99", "9", "", " ")
AND ETHNOS IS NOT NULL

-- COMMAND ----------

-- Group by nhs number, episode end date, and ethnos field to get one ethnicity per date, per patient (removes duplicates e.g. several admittals in one day - unlikely?)
CREATE OR REPLACE TEMPORARY VIEW APC_GROUPBY AS
SELECT NHS_NUMBER
, EPIEND
, ETHNICITY
, COUNT(*) AS COUNT_RECORDS
FROM HES_APC_NO_UNKNOWN
GROUP BY NHS_NUMBER
, EPIEND
, ETHNICITY

-- COMMAND ----------

-- get most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW APC_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(EPIEND) AS RECENT_ETHNICITY_DATE
FROM APC_GROUPBY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get most recent ethnicity for each patient
CREATE OR REPLACE TEMPORARY VIEW HES_APC_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.EPIEND AS RECORDED_DATE
, "HES_APC" AS SOURCE
, 3 AS PRIORITY
FROM APC_GROUPBY AS a
INNER JOIN APC_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.EPIEND = b.RECENT_ETHNICITY_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # APPEND AND FIND MOST RECENT OF ALL

-- COMMAND ----------

-- append gdppr and hes ethnicity records together
CREATE OR REPLACE TEMPORARY VIEW ALL_SOURCES_RECENT_ETHNICITY_JOINED AS

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

UNION

SELECT NHS_NUMBER
, ETHNICITY
, RECORDED_DATE
, SOURCE
, PRIORITY
FROM HES_OP_ETHNICITY

UNION

SELECT NHS_NUMBER
, ETHNICITY
, RECORDED_DATE
, SOURCE 
, PRIORITY
FROM HES_AE_ETHNICITY

UNION

SELECT NHS_NUMBER
, ETHNICITY
, RECORDED_DATE
, SOURCE 
, PRIORITY
FROM HES_APC_ETHNICITY


-- COMMAND ----------

-- get most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW ALL_SOURCES_RECENT_DATE AS
SELECT NHS_NUMBER
, MAX(RECORDED_DATE) AS RECENT_ETHNICITY_DATE
FROM ALL_SOURCES_RECENT_ETHNICITY_JOINED
GROUP BY NHS_NUMBER


-- COMMAND ----------

-- get most recent ethnicity record for each patient
CREATE OR REPLACE TEMPORARY VIEW ALL_ETHNICITY_RECENT AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORDED_DATE
, a.SOURCE
, a.PRIORITY
FROM ALL_SOURCES_RECENT_ETHNICITY_JOINED AS a
INNER JOIN ALL_SOURCES_RECENT_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.RECORDED_DATE = b.RECENT_ETHNICITY_DATE 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # REMOVE CONFLICTS

-- COMMAND ----------

-- find highest priority source for each patient (this is the minimum as GDPPR journal is priority 1, GDPPR patient is priority 2 etc.)
CREATE OR REPLACE TEMPORARY VIEW MIN_PRIORITY AS
SELECT NHS_NUMBER
, MIN(PRIORITY) AS MIN_PRIORITY
FROM ALL_ETHNICITY_RECENT
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get prioritised ethnicity record for each patient (remove lower priority source records)
CREATE OR REPLACE TEMPORARY VIEW PRIORITISED_ETHNICITY AS
SELECT a.NHS_NUMBER
, a.ETHNICITY
, a.RECORDED_DATE
, a.SOURCE
, a.PRIORITY
FROM ALL_ETHNICITY_RECENT AS a
INNER JOIN MIN_PRIORITY AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.PRIORITY = b.MIN_PRIORITY

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # NULLIFY DUPLICATES

-- COMMAND ----------

-- count ethnicities per NHS number as any with more than one need to be nulled and removed (for now)
CREATE OR REPLACE TEMPORARY VIEW DUPLICATE_ETHNICITY_NHSNUM AS
SELECT NHS_NUMBER
, COUNT(*) AS COUNT_ETHNICITIES
FROM PRIORITISED_ETHNICITY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- select only NHS numbers with one ethnicity i.e. remove patients who have more than one

CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_ALL AS
SELECT NHS_NUMBER
, ETHNICITY AS ETHNIC_CATEGORY_CODE
, RECORDED_DATE AS DATE_OF_ATTRIBUTION
, SOURCE AS DATA_SOURCE
FROM PRIORITISED_ETHNICITY
WHERE NHS_NUMBER IN (SELECT NHS_NUMBER FROM DUPLICATE_ETHNICITY_NHSNUM WHERE COUNT_ETHNICITIES = 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # REMOVE DECEASED PATIENTS AND OPT OUTS

-- COMMAND ----------

-- create list of gdppr patients who are not deceased (gdppr does not include opt outs so they will be removed where they came in from HES)
CREATE OR REPLACE TEMPORARY VIEW ALIVE_PATIENTS AS
SELECT DISTINCT NHS_NUMBER
FROM gdppr_database.gdppr_table
WHERE DATE_OF_DEATH IS null

-- COMMAND ----------

/* ##########################################################
###########  THIS IS THE FINAL TABLE  #######################
############################################################# */

-- pull only living patients

CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_V1 AS
SELECT NHS_NUMBER
, ETHNIC_CATEGORY_CODE
, DATE_OF_ATTRIBUTION
, DATA_SOURCE
FROM ETHNICITY_ASSET_ALL
WHERE NHS_NUMBER IN (SELECT NHS_NUMBER FROM ALIVE_PATIENTS)


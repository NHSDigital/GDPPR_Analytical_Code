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

-- MAGIC %md
-- MAGIC # APPEND AND FIND MOST RECENT OF ALL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # MODAL

-- COMMAND ----------

-- append gdppr and hes ethnicity records together
CREATE OR REPLACE TEMPORARY VIEW ALL_SOURCES_ETHNICITY_JOINED AS

SELECT NHS_NUMBER
, ETHNICITY
, "Journal" AS SOURCE
FROM JOURNAL_GROUPBY

UNION

SELECT NHS_NUMBER
, ETHNICITY
, "Patient" AS SOURCE
FROM PATIENT_GROUPBY

UNION

SELECT NEWNHSNO AS NHS_NUMBER
, ETHNOS AS ETHNICITY
, "OP" AS SOURCE
FROM OP_5_YEAR
WHERE ETHNOS NOT IN ("Z", "z", "X", "x", "99", "9", "", " ")
AND ETHNOS IS NOT NULL

UNION

SELECT NEWNHSNO AS NHS_NUMBER
, ETHNOS AS ETHNICITY
, "AE" AS SOURCE
FROM AE_5_YEAR
WHERE ETHNOS NOT IN ("Z", "z", "X", "x", "99", "9", "", " ")
AND ETHNOS IS NOT NULL

UNION

SELECT NEWNHSNO AS NHS_NUMBER
, ETHNOS AS ETHNICITY
, "APC" AS SOURCE
FROM APC_5_YEAR
WHERE ETHNOS NOT IN ("Z", "z", "X", "x", "99", "9", "", " ")
AND ETHNOS IS NOT NULL

-- COMMAND ----------

-- count journals per ethnicity per patient
CREATE OR REPLACE TEMPORARY VIEW JOURNALS_PER_ETHNICITY AS
SELECT NHS_NUMBER
, ETHNICITY
, COUNT(*) AS COUNT
FROM ALL_SOURCES_ETHNICITY_JOINED
GROUP BY NHS_NUMBER
, ETHNICITY

-- COMMAND ----------

-- get max count per patient
CREATE OR REPLACE TEMPORARY VIEW MAX_COUNT_ETHNICITY AS
SELECT NHS_NUMBER
, MAX(COUNT) AS MAX_COUNT
FROM JOURNALS_PER_ETHNICITY
GROUP BY NHS_NUMBER

-- COMMAND ----------

-- get the modal ethnicity per patient (may still include duplicates for those with more than one modal ethnic cat)
CREATE OR REPLACE TEMPORARY VIEW MODAL_ETHNICITY AS
SELECT a.* 
FROM JOURNALS_PER_ETHNICITY AS a
INNER JOIN MAX_COUNT_ETHNICITY AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.COUNT = b.MAX_COUNT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### REMOVE DUPLICATES

-- COMMAND ----------

-- count ethnicities per NHS number as any with more than one need to be nulled and removed (for now)
CREATE OR REPLACE TEMPORARY VIEW DUPLICATE_ETHNICITY_NHSNUM_MODAL AS
SELECT NHS_NUMBER
, COUNT(DISTINCT ETHNICITY) AS COUNT_ETHNICITIES
FROM MODAL_ETHNICITY
GROUP BY NHS_NUMBER

-- COMMAND ----------

/* ##########################################################
###########  THIS IS THE FINAL TABLE  #######################
############################################################# */

-- select only NHS numbers with one modal ethnicity i.e. remove patients who have more than one
CREATE OR REPLACE TEMPORARY VIEW ETHNICITY_ASSET_MODAL AS
SELECT NHS_NUMBER
, ETHNICITY AS ETHNIC_CATEGORY_CODE
FROM MODAL_ETHNICITY
WHERE NHS_NUMBER IN (SELECT NHS_NUMBER FROM DUPLICATE_ETHNICITY_NHSNUM_MODAL WHERE COUNT_ETHNICITIES = 1)
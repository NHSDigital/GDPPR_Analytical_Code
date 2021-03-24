-- Databricks notebook source
-- MAGIC %md # SMOKING STATUS GDPPR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SMOKING JOURNALS

-- COMMAND ----------

-- create view of only smoking journals
-- add in code description and category which allows us to assign smoking status
CREATE OR REPLACE TEMPORARY VIEW all_smoking_journals AS
SELECT a.* 
, b.Code_description
, b.CATEGORY
FROM gdppr_database.gdppr_table as a
LEFT JOIN reference_database.smoking_ref_data AS b
ON a.CODE = b.SNOMED_concept_ID
WHERE a.CODE IN (SELECT DISTINCT SNOMED_concept_ID FROM reference_database.smoking_ref_data)

-- COMMAND ----------

-- MAGIC %md # CATEGORISE

-- COMMAND ----------

-- categorise journals based on values or snomed codes (based on previous analyses)
CREATE OR REPLACE TEMPORARY VIEW categorise_smoking_journals AS
SELECT *
, CASE WHEN CATEGORY = 'non_smoker_findings_codes' AND VALUE1_CONDITION IS NULL THEN "Non-Smoker"
       WHEN CATEGORY = 'smoker_findings_codes' AND VALUE1_CONDITION IS NULL THEN "Smoker" 
       WHEN CATEGORY = 'Microtest_Error' AND VALUE1_CONDITION IS NULL THEN "Non-Smoker"
       WHEN CATEGORY = 'Microtest_Error' AND VALUE1_CONDITION = 0 THEN "Non-Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND VALUE1_CONDITION IS NULL THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '160603005' AND VALUE1_CONDITION >= 1 AND VALUE1_CONDITION <= 9 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '160604004' AND VALUE1_CONDITION >= 10 AND VALUE1_CONDITION <= 19 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '160605003' AND VALUE1_CONDITION >= 20 AND VALUE1_CONDITION <= 39 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '160606002' AND VALUE1_CONDITION >= 40 AND VALUE1_CONDITION <= 100 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '266920004' AND VALUE1_CONDITION >= 0 AND VALUE1_CONDITION <= 1 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '56578002' AND VALUE1_CONDITION >=1 AND VALUE1_CONDITION <=20 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '56771006' AND VALUE1_CONDITION >= 20 AND VALUE1_CONDITION <= 100 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '59978006' AND VALUE1_CONDITION >=1 AND VALUE1_CONDITION <= 20 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '65568007' AND VALUE1_CONDITION >= 1 AND VALUE1_CONDITION <= 100 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '77176002' AND VALUE1_CONDITION >= 1 AND VALUE1_CONDITION <= 100 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Smoker' AND CODE = '82302008' AND VALUE1_CONDITION >= 1 AND VALUE1_CONDITION <= 100 THEN "Smoker"
       WHEN CATEGORY = 'Value_Finding_Non_Smoker' AND VALUE1_CONDITION IS NULL THEN "Non-Smoker"
       WHEN CATEGORY = 'Observable_Entities' AND CODE IN ('160625004', '230056004', '230057008', '230058003', '266918002', '401201003', '413173009', '836001000000109', '228486009', '735112005') AND VALUE1_CONDITION IS NULL THEN "Non-Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE IN ('160625004', '735112005') AND VALUE1_CONDITION >= 0 THEN "Non-Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE IN ('230056004', '266918002', '401201003') AND VALUE1_CONDITION = 0 THEN "Non-Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE = '230056004' AND VALUE1_CONDITION > 0 AND VALUE1_CONDITION <= 100 THEN "Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE = '230057008' AND VALUE1_CONDITION > 0 AND VALUE1_CONDITION <= 20 THEN "Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE = '230058003' AND VALUE1_CONDITION > 0 AND VALUE1_CONDITION <= 100 THEN "Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE = '266918002' AND VALUE1_CONDITION > 0 AND VALUE1_CONDITION <= 100 THEN "Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE = '401201003' AND VALUE1_CONDITION > 0 AND VALUE1_CONDITION <= 240 THEN "Smoker" 
       WHEN CATEGORY = 'Observable_Entities' AND CODE = '413173009' AND VALUE1_CONDITION >= 0 AND VALUE1_CONDITION <= 1440 THEN "Smoker"
       WHEN CATEGORY = 'Observable_Entities' AND CODE = '836001000000109' AND VALUE1_CONDITION > 0 AND VALUE1_CONDITION <= 100 THEN "Smoker"
       WHEN CATEGORY = 'Observable_Entities' AND CODE IN ('401159003') AND VALUE1_CONDITION IS NULL THEN "Smoker"    
ELSE 'Indeterminable'
END AS SMOKING_CATS
FROM all_smoking_journals

-- COMMAND ----------

-- MAGIC %md # ADD DATA DICTIONARY SMOKING CATEGORIES

-- COMMAND ----------

---- Add in nhs data dictionary categories : current smoker; ex-smoker; non-smoker history unknown; never smoker and not stated
CREATE OR REPLACE TEMPORARY VIEW smoking_categories_2 AS
SELECT *,
CASE WHEN SMOKING_CATS = "Smoker" THEN "Current-Smoker"
WHEN SMOKING_CATS = "Indeterminable" THEN "Indeterminable"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "221000119102" THEN "Never-Smoker"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "266919005" THEN "Never-Smoker"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "401201003" AND VALUE1_CONDITION IS NULL THEN "Never-Smoker"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "401201003" AND VALUE1_CONDITION = 0 THEN "Never-Smoker"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "105539002" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "105540000" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "105541001" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "160618006" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "360918006" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "360929005" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "405746006" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "8392000" THEN "Non-Smoker History Unknown"
WHEN SMOKING_CATS = "Non-Smoker" AND CODE = "87739003" THEN "Non-Smoker History Unknown"
ELSE 'Ex-Smoker'
END AS DATA_DICTIONARY_CATS
FROM categorise_smoking_journals

-- COMMAND ----------

-- MAGIC %md # FIND MOST RECENT RECORD

-- COMMAND ----------

---- Need to remove those that are indeterminable - most recent "known" record
CREATE OR REPLACE TEMPORARY VIEW smoking_no_unknowns AS
SELECT * 
FROM smoking_categories_2
WHERE DATA_DICTIONARY_CATS != "Indeterminable"

-- COMMAND ----------

---- Create table of most recent date per patient
CREATE OR REPLACE TEMPORARY VIEW MAX_DATE AS
SELECT NHS_NUMBER
, MAX(DATE) AS DATE
FROM smoking_no_unknowns
GROUP BY NHS_NUMBER

-- COMMAND ----------

---- create table with only most recent records
CREATE OR REPLACE TEMPORARY VIEW smoking_Most_Recent_Records AS
SELECT a.*
FROM smoking_no_unknowns AS a
INNER JOIN MAX_DATE AS b
ON a.NHS_NUMBER = b.NHS_NUMBER
AND a.DATE = b.DATE

-- COMMAND ----------

---- Count number of categories per patient from most recent records - patients may have more than one recent record so some may have conflicting smoking statuses
CREATE OR REPLACE TEMPORARY VIEW smoking_Journal_Counts AS
SELECT NHS_NUMBER
, COUNT(DISTINCT DATA_DICTIONARY_CATS) AS COUNT_CATEGORIES
FROM smoking_Most_Recent_Records
GROUP BY NHS_NUMBER

-- COMMAND ----------

---- Remove individual with conflicting smoking statuses 
-- add in age at time of journal
CREATE OR REPLACE TEMPORARY VIEW recent_smoking_journals_no_conflicts AS
SELECT * 
, FLOOR(DATEDIFF(DATE, DATE_OF_BIRTH) / 365.25) AS AGE_AT_TIME_OF_JOURNAL
FROM smoking_Most_Recent_Records
WHERE NHS_NUMBER IN (SELECT DISTINCT NHS_NUMBER FROM smoking_Journal_Counts WHERE COUNT_CATEGORIES = 1)

-- COMMAND ----------
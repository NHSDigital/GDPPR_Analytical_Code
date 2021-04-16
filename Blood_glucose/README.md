# BACKGROUND
Blood glucose status may be of interest in relation to the COVID-19 pandemic therefore NHS Digital have written some code to understand the quality, coverage, and distribution of blood glucose status recording within the GDPPR dataset.

Blood glucose status can be recorded via several methods within GDPPR, all of which use SNOMED codes within patient journal tables. These snomed codes can initially be categorised into the following categories:
1. Blood
2. HbA1c
3. Plasma
4. Serum
5. Tolerance

Some of these categories can then be further classified into the following categories:
1. Fasting
2. Non-fasting
3. OGTT
4. Random

These classifications are required to categorise the associated blood glucose reading accordingly and are therefore included within the reference data. *Please note that cut off points for outliers were determined through initial analyses and were agreed with internal clinicians.*

# METHODOLOGY

a) All blood glucose SNOMED codes are categorised in the reference data based on previous analyses. The Cluster ID's used to create this reference data are 'GLUC_COD', 'FASPLASGLUC_COD' and 'IFCCHBAM_COD'.  _Please note it is possible that further blood pressure clusters may be created and/or blood glucose codes may be added to the GDPPR extract and these would need categorising._

b) Journals categorised as HbA1c are categorised according to [these thresholds](https://www.diabetes.co.uk/what-is-hba1c.html) and discussions with internal clinicians

c) Journals initially categorised as 'Blood', 'Plasma' or 'Serum', and have further sub-classifications of 'Non-Fasting' or 'Random' are categorised according to [these thresholds](https://www.diabetes.co.uk/diabetes_care/blood-sugar-level-ranges.html) and discussions with internal clinicians

d) Journals with a sub-classification of 'OGTT' can only be categorised if it is joined to an associated 'Fasting' journal as these statuses are determined on the basis of the fasting and 2 hour levels. These journals are matched according to NHS_Number/date of recording etc. and are then categorised according to [these thresholds](https://www.diabetes.co.uk/diabetes_care/blood-sugar-level-ranges.html) and discussions with internal clinicians

   -  Journals with a sub-classification of 'Fasting' can be categorised alone without the OGTT journal and are therefore categorised alone if no OGTT journal is also present. They are categorised according to [these thresholds](https://www.diabetes.co.uk/diabetes_care/blood-sugar-level-ranges.html) and discussions with internal clinicians.

e) Journals without a blood glucose reading (value) or journals with indeterminable statuses are removed and the most recent blood glucose reading for each patient is selected.

# NOTES

GDPPR data = ```gdppr_database.gdppr_table```

blood glucose snomed code reference data = ```ref_data.blood_glucose_ref_data```

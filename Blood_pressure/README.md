# BACKGROUND
Blood pressure status may be of interest in relation to the COVID-19 pandemic therefore NHS Digital have written some code to understand the quality, coverage, and distribution of blood pressure status recording within the GDPPR dataset.

Blood pressure status can be recorded via several methods within GDPPR, all of which use SNOMED codes within patient journal tables:

1.	SNOMED codes for blood pressure status (no values) e.g. code with description of ‘On examination - blood pressure reading very high (finding)’
2.	SNOMED codes for either systolic or diastolic blood pressure status (no values) e.g. code with description of ‘Normal diastolic arterial pressure (finding)’ - these need joining to calculate blood pressure status
3.	SNOMED codes for blood pressure with **2** associated values e.g. code for ‘Blood pressure (observable entity)’ with associated values of 120 and 80
4.	SNOMED codes for either systolic or diastolic blood pressure with **1** associate value e.g. 'Diastolic blood pressure (observable entity)' with an associated value of 80 - these need joining to calculate blood pressure status

# METHODOLOGY

a) All blood pressure SNOMED codes are categorised in the reference data based on previous analyses. The Cluster ID's used to create this reference data are 'BP_COD', 'ABPM_COD' and 'NDABP_COD'.  _Please note it is possible that further blood pressure clusters may be created and/or blood pressure codes may be added to the GDPPR extract and these would need categorising._

b) For method 1, journals are assigned the approproate blood pressure status according to the SNOMED code description

c) For method 2, journals are joined based on NHS number, date etc. to ensure readings were taken on the same day and are then assigned an [NHS data dictionary blood pressure status](https://www.nhs.uk/conditions/high-blood-pressure-hypertension/)

d) For method 3, journals assigned an [NHS data dictionary blood pressure status](https://www.nhs.uk/conditions/high-blood-pressure-hypertension/)

e) For method 4, journals are joined based on NHS number, date etc. to ensure readings were taken on the same day and are then assigned an [NHS data dictionary blood pressure status](https://www.nhs.uk/conditions/high-blood-pressure-hypertension/)

# NOTES

GDPPR data = ```gdppr_database.gdppr_table```

blood pressure snomed code reference data = ```ref_data.blood_pressure_ref_data```

# BACKGROUND
Smoking status may be of interest in relation to the COVID-19 pandemic therefore NHS Digital have written some code to understand the quality, coverage, and distribution of smoking status recording within the GDPPR dataset.

Smoking status can be recorded via several methods within GDPPR, all of which use SNOMED codes within patient journal tables:

1.	SNOMED codes for smoking status (no values) e.g. code with description of ‘Non-smoker (finding)’
2.	SNOMED codes for smoking status with associated values e.g. code for ‘Cigarette consumption (observable entity)’ with an associated value of 10
3.	SNOMED codes for non-smokers which need categorising in terms of their smoking history e.g. never smoker or ex smoker

# METHODOLOGY

a) All smoking status SNOMED codes are categorised in the reference data based on previous analyses. _Please note it is possible that further smoking status codes may be added to the GDPPR extract and these would need categorising._

b) For methods 1 and 3, journals are assigned an [NHS data dictionary smoking status](https://datadictionary.nhs.uk/attributes/smoking_status.html)

c) For method 2, journals are assigned an NHS data dictionary smoking status based on the associated value. Categorisations are based on initial analyses and discussions with clinicians.

# NOTES

GDPPR data = ```gdppr_database.gdppr_table```

smoking status snomed code reference data = ```ref_data.BMI_CODES```

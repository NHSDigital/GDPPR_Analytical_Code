# BACKGROUND

Ethnicity is of particular interest in relation to the COVID-19 pandemic therefore NHS Digital have produced an ethnicity asset to understand the quality, coverage, and distribution of ethnic category recording within certain datasets held by NHS Digital. The [2011 Census](https://www.ons.gov.uk/census/2011census), published by the Office for National Statistics (ONS), is the gold standard for ethnicity recording in England and Wales, however as this data is nearly 10 years old and may no longer reflect the ethnic breakdown of the current population NHS Digital have amalgamated ethnic category data from [Hospital Episode Statistics (HES)](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics) and [GPES Data for Pandemic Planning and Research COVID (GDPPR-COVID)](https://digital.nhs.uk/coronavirus/gpes-data-for-pandemic-planning-and-research) to provide a near population (England only) level view of ethnic category. Ethnic category coverage management information using this asset is published on the [NHS Digital website](https://digital.nhs.uk/data-and-information/areas-of-interest/ethnicity).

The HES datasets included within this code are HES Admitted Patient Care (APC), HES Accident and Emergency (AE) and HES Outpatients (OP), from the current year and previous 5 years. Ethnic category can only be recorded via the ETHNOS field in the HES datasets. 

Ethnic category can be recorded via two methods within GDPPR: an ETHNIC field within patient information tables, or a SNOMED code for ethnicity within patient journal tables. [SNOMED codes](https://digital.nhs.uk/services/terminology-and-classifications/snomed-ct) are the clinical coding standards used with GP records.

# METHODOLOGY

## Recency 

__*Note: Almost identical methodology is used for GDPPR only, GDPPR + HES, and GDPPR with HES only when ethnicity is not available in GDPPR, therefore it is only explained once here.*__

**a)** Patients and their recorded ethnic categories from each of the 5 data sources (GDPPR-Journal, GDPPR-Patient, HES-APC, HES-AE, HES-OP) are amalgamated.

**b)** Records where the ethnic category is unknown are removed, and the data source providing the most recent ethnic category recording for each patient is selected to de-duplicate patients with multiple recordings.

**c)** Where conflicts exist between data sources priority is given in the following order: GDPPR-Journal, GDPPR-Patient, HES-APC, HES-AE, HES-OP. 

**d)** Where conflicts still exist (i.e. where the highest priority data source gives different ethnic categories on the latest date of attribution) ethnic category is set to null and the patient is not counted as having a known ethnic category.

**e)** Records are removed for patients who have a recorded date of death and/or their postcode is not in England.

## Modal

**a)** Patients and their recorded ethnic categories from each of the 5 data sources (GDPPR-Journal, GDPPR-Patient, HES-APC, HES-AE, HES-OP) are amalgamated and records where ethnic category is unknown are removed.

**b)** The modal ethnic category recording for each patient is selected to de-duplicate patients with multiple recordings.

**c)** Where conflicts exist between data sources i.e. a patient has more than one modal ethnic category, ethnic category is set to null and the patient is not counted as having a known ethnic category.

**d)** Records are removed for patients who have a recorded date of death and/or their postcode is not in England.

# NOTES

GDPPR data = ```gdppr_database.gdppr_table```

Ethnicity reference data = ```reference_database.gdppr_ethnicity_mappings_table```

HES data is stored in two tables - sensitive and non-sensitive; they need joining to get the required data:

HES OP data = ```sensitive_hes.hes_op_{year}``` and ```hes.hes_op_{year}```

HES AE data = ```sensitive_hes.hes_ae_{year}``` and ```hes.hes_ae_{year}```

HES APC data = ```sensitive_hes.hes_apc_{year}``` and ```hes.hes_apc_{year}```


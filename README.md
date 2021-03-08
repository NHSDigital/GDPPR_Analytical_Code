[GDPPR](https://digital.nhs.uk/coronavirus/gpes-data-for-pandemic-planning-and-research) subject matter experts have completed various analyses using the GDPPR dataset and are sharing code to:

* prevent duplication of work
* allow peer review of code and methodology used in analysis
* increase consistency of methodology across users
* increase general knowledge sharing

Useful code can be found in the folders within this repository. Each folder will include a README.md file which explains the basic methodology behind the analysis/code, a code script, and reference data (where necessary).

Please ensure that you read the information in the read me as there may be several scripts for different methodologies. 

Due to the analytical environment NHS Digital uses (databricks) it is possible for python, SQL and markdown code to be used within the same script. 
- SQL code will appear as usual - please note that databricks uses spark SQL rather than T-SQL
- Python code will have ```--MAGIC``` prior to it and ```%py``` to signal that this 'chunk' of code is written in python
- Markdown code will have ```--MAGIC``` prior to it and ```%md``` to signal that this 'chunk' of code is written in markdown. Markdown code is minimal and used in databricks to separate code as you would with commenting in SQL.

Code is included for the following pieces of analysis:
* Ethnicity
* BMI

Please note that the majority of this code has been written as part of exploratory analyses so care should be taken to ensure it is appropriate for your own analysis and that relevant clinical advice is sought where appropriate. All code and methodology is subject to change following further analysis.  

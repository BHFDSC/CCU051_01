# Databricks notebook source
# MAGIC %md # CCU051_D00_master
# MAGIC 
# MAGIC **Description** This notebook runs all notebooks in the correct order.
# MAGIC 
# MAGIC **Author(s)** Genevieve Cezard
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **Created on** 2022.10.04
# MAGIC 
# MAGIC **Last updated on** 2023.05.23

# COMMAND ----------

# Step 1: Parameters (last run on March 28th)

# dbutils.notebook.run("CCU051_D01_parameters", 3600)

# Checks for data coverage are done here: data_checks / CCU051-D00-check_coverage

# COMMAND ----------

# Step 2: Codelists (last run on Dec 6th for 2a+2c and on March 2nd for 2b+2d)

# dbutils.notebook.run("CCU051_D02a_codelist_covid", 3600)
# dbutils.notebook.run("CCU051_D02b_codelist_Qcovid", 3600)
# dbutils.notebook.run("CCU051_D02c_codelist_quality_assurance", 3600)
# dbutils.notebook.run("CCU051_D02d_codelist_covariates", 3600)

# Checks for QCovid codelists are done here: data_checks / CCU051-D02b-codelist_check

# COMMAND ----------

# Step 3a-b : Include data curation for LSOA and to use in skinny table or outcome definition
# dbutils.notebook.run("CCU051_D03a_curated_data", 3600) # (last run on March 30th)
# dbutils.notebook.run("CCU051_D03b_curated_data_ruc", 3600) # (last run on Jan 24th)

# Step 3c-d: Include data curation for vaccination data to be use in vaccination notebook (last run on Jan 25th)
# dbutils.notebook.run("CCU051_D03c_curated_data_vacc_tb2", 3600)
# dbutils.notebook.run("CCU051_D03d_curated_vacc_reshape_tb2", 3600)

# Step 3e: Include data curation for PCR tests (last run on March 28th)
# dbutils.notebook.run("CCU051_D03e_curated_data_PCR_test", 3600)


# COMMAND ----------

# Step 4: Create patient skinny table with basic information (last run on Jan 25th)
# This contains ID, month/year of birth, sex, ethnicity, LSOA for all individuals
# NOTE:
# -> Use the skinny functions developed by Tom in CCU018 i.e. skinny_unassembled() and skinny_unassembled() (see in CCU018_01-D08-skinny notebook)
# -> It prioritises primary care records over hospitalisation records for example

# dbutils.notebook.run("CCU051_D04_skinny", 3600)

# COMMAND ----------

# Step 5: Perform quality assurance checks (last run on Jan 26th)

# dbutils.notebook.run("CCU051_D05_quality_assurance", 3600)

# COMMAND ----------

# Step 6: Apply Inclusion/Exclusion criteria (last run on Jan 26th /May 3rd just to check something)
# It also creates a temporary Cohort used in step 7 

# dbutils.notebook.run("CCU051_D06_inclusion_exclusion", 3600)

# COMMAND ----------

# Step 7: Add a couple of variables in the cohort (last run on Jan 26th)

# dbutils.notebook.run("CCU051_D07_cohort", 3600)

# COMMAND ----------

# Step 8: Define weights (last run on Jan 27th)
# Use cohort created in step 7.

# dbutils.notebook.run("CCU051_D08_sample_weights", 3600)

# COMMAND ----------

# Step 9a-b: Define covariates and non-Covid hospitalisation (last run on Feb 12th)

# dbutils.notebook.run("CCU051_D09a_covariates", 3600)
# dbutils.notebook.run("CCU051_D09b_covariate_noncovid_hosp", 3600)

# Step 9c: Get last positivie PCR test (last run on March 28th /April 26th just to check something)
# dbutils.notebook.run("CCU051_D09c_covariate_PCR_tests", 3600)

# COMMAND ----------

# Step 10: Define exposure/vaccination (last run on Feb 7th)

# dbutils.notebook.run("CCU051_D10_vaccination", 3600)

# COMMAND ----------

# Step 11: Define outcomes (Covid hospitalisations and deaths) (last run on March 30th)

# dbutils.notebook.run("CCU051_D11_outcome", 3600)

# COMMAND ----------

# Step 12: Combine all and create analysis cohort (final table) + QCovid items in separate dataset (last run on March 31st)

# dbutils.notebook.run("CCU051_D12_combine", 3600)

# Databricks notebook source
# MAGIC %md # CCU051_D03e_curated_data_PCR_test
# MAGIC 
# MAGIC **Description** This notebook creates the covid phenotypes table of CCU051.
# MAGIC 
# MAGIC **Author(s)** Alexia Sampri
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2023.03.16 (Based on code developed by Alexia Sampri and Tom Bolton for CCU002_07)
# MAGIC 
# MAGIC **Date last updated** 2023.03.28
# MAGIC 
# MAGIC **Date last run** 2023.03.28
# MAGIC  
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC ccu051_out_codelist_covid - ccu051_out_cohort - sgss 
# MAGIC 
# MAGIC **Data output**    
# MAGIC ccu051_cur_covid
# MAGIC 
# MAGIC 
# MAGIC **Acknowledgements** Based on previous work by Alexia Sampri and Tom Bolton for CCU002_07, CCU018_01 and the earlier CCU002 sub-projects.

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU051_D01_parameters"

# COMMAND ----------

start_date = '2020-01-01'
end_date = '2022-05-31' 
print(start_date, end_date)

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

codelist = spark.table(path_out_codelist_covid)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

sgss     = extract_batch_from_archive(parameters_df_datasets, 'sgss')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# sgss
_sgss = sgss\
  .select(['PERSON_ID_DEID', 'Reporting_Lab_ID', 'Specimen_Date'])\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('Specimen_Date', 'DATE')\
  .where((f.col('DATE') >= start_date) & (f.col('DATE') <= end_date))\
  .dropDuplicates()


# COMMAND ----------

# MAGIC %md
# MAGIC # 3 Covid positive

# COMMAND ----------

# sgss
# note: all records are included as every record is a "positive test"
# -- TODO: wranglers please clarify whether LAB ID 840 is still the best means of identifying pillar 1 vs 2
# -- CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as description,
#   .withColumn('description', f.when(f.col('Reporting_Lab_ID') == '840', 'pillar_2').otherwise('pillar_1'))\
_sgss_pos = _sgss\
  .withColumn('covid_phenotype', f.lit('Covid_positive_test'))\
  .withColumn('clinical_code', f.lit(''))\
  .withColumn('description', f.lit(''))\
  .withColumn('covid_status', f.lit(''))\
  .withColumn('code', f.lit(''))\
  .withColumn('source', f.lit('sgss'))\
  .select('PERSON_ID', 'DATE', 'covid_phenotype', 'clinical_code', 'description', 'covid_status', 'code', 'source')

# COMMAND ----------

display(_sgss_pos)

# COMMAND ----------

tmp = _sgss_pos

# COMMAND ----------

# check combined
count_var(tmp, 'PERSON_ID'); print()
tmpt = tab(tmp, 'covid_phenotype', 'source', var2_unstyled=1); print()
tmp1 = tmp.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

# check individual
count_var(tmp, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save name
outName = f'{proj}_cur_covid'.lower()

# save
tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

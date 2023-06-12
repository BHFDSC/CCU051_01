# Databricks notebook source
# MAGIC %md # CCU051_D09c_covariate_PCR_tests
# MAGIC  
# MAGIC **Description** This notebook creates - the last PCR test before baseline -  notebook of CCU051.
# MAGIC 
# MAGIC **Author(s)** Alexia Sampri
# MAGIC 
# MAGIC **Date last updated** 2023.04.26
# MAGIC 
# MAGIC **Date last run** 2023.04.26
# MAGIC  
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC ccu051_out_cohort - ccu051_cur_covid 
# MAGIC 
# MAGIC **Data output** ccu051_out_PCR_test
# MAGIC 
# MAGIC **Acknowledgements** Based on previous work by Alexia Sampri and Tom Bolton for CCU002_07, CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC 
# MAGIC **Notes** Data: +ve PCR test Pillar 1 and/or Pillar 2 COVID-19 infection laboratory testing data 

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

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

# MAGIC %md # 1 Data

# COMMAND ----------

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_cur_covid')
covid = spark.table(f'{dbc}.{proj}_cur_covid')

# COMMAND ----------

display(cohort)

# COMMAND ----------

display(covid)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# check
count_var(cohort, 'PERSON_ID'); print()
count_var(covid, 'PERSON_ID'); print()

# restrict to cohort
_covid = merge(covid, cohort.select('PERSON_ID', 'baseline_date'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()



# COMMAND ----------

# check
count_var(_covid, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 3 Infection

# COMMAND ----------

# MAGIC %md ## 3.1 Check

# COMMAND ----------

# check infection
count_var(_covid, 'PERSON_ID'); print()
tmpt = tabstat(_covid, 'DATE', date=1); print()
tmp1 = _covid.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
tmpt = tab(_covid, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(_covid, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Prepare

# COMMAND ----------

print('------------------------------------------------------------------------------')
print('confirmed COVID-19 (as defined for CCU002_01)')
print('------------------------------------------------------------------------------')

covid_confirmed = _covid\
  .where(\
    (f.col('covid_phenotype').isin([
      'Covid_positive_test'
    ]))\
    & (f.col('source').isin(['sgss']))\
    & (f.col('covid_status').isin(['confirmed', '']))\
  )

# check
count_var(covid_confirmed, 'PERSON_ID'); print()
print(covid_confirmed.limit(10).toPandas().to_string(max_colwidth=50)); print()
tmpt = tabstat(covid_confirmed, 'DATE', date=1); print()
tmp1 = covid_confirmed.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
tmpt = tab(covid_confirmed, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(covid_confirmed, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Create

# COMMAND ----------

print('------------------------------------------------------------------------------')
print('latest confirmed covid infection through PCR test')
print('------------------------------------------------------------------------------')
# window for row number
_win_rownum = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('date'), 'covid_phenotype', 'source')

# filter to latest confirmed covid infection
covid_confirmed_last = covid_confirmed\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .where(f.col('_rownum') == 1)\
  .withColumnRenamed('DATE', 'PCR_last_date')\
  .orderBy('PERSON_ID')

# check
count_var(covid_confirmed_last, 'PERSON_ID'); print()
tmpt = tabstat(covid_confirmed_last, 'PCR_last_date', date=1); print()
tmpt = tab(covid_confirmed_last, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(covid_confirmed_last, 'covid_phenotype', 'source', var2_unstyled=1); print()

# reduce
covid_confirmed_last = covid_confirmed_last\
  .select('PERSON_ID', 'PCR_last_date', f.col('covid_phenotype').alias('covid_last_phenotype'))

 
# check
count_var(covid_confirmed_last, 'PERSON_ID'); print()
print(covid_confirmed_last.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 3.4 Check

# COMMAND ----------

# check 
display(covid_confirmed_last)

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# merge 
tmp1 = merge(covid_confirmed_last, cohort.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# check
count_var(tmp1, 'PERSON_ID'); print()

# COMMAND ----------

# check final
display(tmp1)

# check dates
_check = tmp1\
  .where((f.col('exp_covid_last_date') < '2020-01-01')|(f.col('exp_covid_last_date') >= '2022-06-01'))

# check
count_var(_check, 'PERSON_ID'); print()

# COMMAND ----------

# save name
outName = f'{proj}_out_PCR_test'.lower()

# save
tmp1.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

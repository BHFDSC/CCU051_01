# Databricks notebook source
# %md # X. Any other hospitalisation
# Date of first hospitalisation that is not Covid between June 1st 2022 and September 30th 2022

# COMMAND ----------

# MAGIC %md # CCU051_D09a_covariates_noncovid_hosp
# MAGIC 
# MAGIC **Description** This notebook creates the covariates for CCU051.
# MAGIC  * Any other hospitalisations (that is not Covid)
# MAGIC   
# MAGIC **Author(s)** Tom Bolton, Genevieve Cezard
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2022.11.29 (from CCU002_07)
# MAGIC 
# MAGIC **Date last updated** 2023.03.12
# MAGIC 
# MAGIC **Date last run** 2023.03.12
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC codelist_qcovid - codelist_covariates\
# MAGIC 'ccu051_tmp_cohort'\
# MAGIC gdppr - hes_apc_long - hes_apc_oper_long
# MAGIC 
# MAGIC **Data output** - 'CCU051_out_covariates'
# MAGIC 
# MAGIC **Acknowledgements** Adapted from previous work by Tom Bolton (John Nolan, Elena Raffetti, Alexia Sampri) from CCU002_07, CCU018_01 and earlier CCU002 sub-projects.

# COMMAND ----------

spark.sql('CLEAR CACHE')

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

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU051/CCU051_D01_parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

# codelists
# see below

# cohort
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

# data sources
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')

# COMMAND ----------

# MAGIC %md # 2. Check

# COMMAND ----------

# see D09_covariates

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# MAGIC %md ## 3.1. Codelist

# COMMAND ----------

# Codelists for Covid-19

# note: copied from outcomes notebook

# https://www.who.int/standards/classifications/classification-of-diseases/emergency-use-icd-codes-for-covid-19-disease-outbreak
codelist_covid = spark.createDataFrame(
  [
    ("covid", "ICD10", "U071", "COVID-19, virus identified"),
    ("covid", "ICD10", "U072", "COVID-19, Virus not identified")  
  ],
  ['name', 'terminology', 'code', 'term']  
)
display(codelist_covid)

# COMMAND ----------

# MAGIC %md ## 3.2. Cohort

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
# check
print(f'proj_end_date = {proj_end_date}'); print()
assert proj_end_date == '2022-09-30'

individual_censor_dates = (
  cohort
  .select('PERSON_ID', f.col('baseline_date').alias('CENSOR_DATE_START'))
  .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{proj_end_date}'))))

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
tmpt = tabstat(individual_censor_dates, 'CENSOR_DATE_START', date=1); print()
tmpt = tabstat(individual_censor_dates, 'CENSOR_DATE_END', date=1); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 3.3. Source data

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print(f'hes_apc') 
print('---------------------------------------------------------------------------------')
# reduce and rename columns
# remove nulls
hes_apc_1 = (
  hes_apc
  .select(f.col('PERSON_ID_DEID').alias('PERSON_ID'), 'EPIKEY', f.col('EPISTART').alias('DATE'), 'DIAG_4_CONCAT')
  .where(f.col('PERSON_ID').isNotNull())  
  .where(f.col('DATE').isNotNull())
)

# add individual censor dates
# hes_apc_long_2 = merge(hes_apc_long_1, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_2 = (
  hes_apc_1
  .join(individual_censor_dates, on=['PERSON_ID'], how='inner')
)

# # check
# count_var(hes_apc_long_2, 'PERSON_ID'); print()  
  
# filter
hes_apc_3 = (
  hes_apc_2
  .where(f.col('DATE') >= f.col('CENSOR_DATE_START')) 
  .where(f.col('DATE') <= f.col('CENSOR_DATE_END'))
  .withColumn('_diff', f.datediff(f.col('CENSOR_DATE_END'), f.col('DATE')))
)

# temp save
hes_apc_prepared = temp_save(df=hes_apc_3, out_name=f'{proj}_tmp_covariates_supp_hes_apc'); print() 

# check
count_var(hes_apc_prepared, 'PERSON_ID'); print()
count_var(hes_apc_prepared, 'EPIKEY'); print()
tmpt = tabstat(hes_apc_prepared, 'DATE', date=1); print()  
tmpt = tabstat(hes_apc_prepared, '_diff'); print()  
  
# tidy
hes_apc_prepared = (
  hes_apc_prepared
  .drop('_diff')
)

# check
print(hes_apc_prepared.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 4. Codelist ANTI-match

# COMMAND ----------

# MAGIC %md ## 4.1. Create

# COMMAND ----------

hes_apc_prepared_1 = (
  hes_apc_prepared
  .withColumn('covid_flag', f.when(f.col('DIAG_4_CONCAT').rlike('U071|U072'), 1).otherwise(0))
)

# check
tmpt = tab(hes_apc_prepared_1, 'covid_flag'); print()

tmpf = (
  hes_apc_prepared_1
  .where(f.col('covid_flag') == 1)  
)
count_var(tmpf, 'PERSON_ID'); print()
# mataches outcomes covid_hes_apc_any (n_id = 106,360)

# COMMAND ----------

# MAGIC %md ## 4.2. Check

# COMMAND ----------

# check
display(hes_apc_prepared_1)

# COMMAND ----------

# MAGIC %md # 5. Prepare

# COMMAND ----------

# filter
first_1 = (
  hes_apc_prepared_1
  .where(f.col('covid_flag') == 0)
)

# check
tmpt = tab(first_1, 'covid_flag'); print()
count_var(first_1, 'PERSON_ID'); print()
count_var(first_1, 'EPIKEY'); print()

# COMMAND ----------

# MAGIC %md # 6. Selection

# COMMAND ----------

# select
# first event
win_first_id_ord = (
  Window
  .partitionBy('PERSON_ID')
  .orderBy('DATE', 'EPIKEY')  
)
first_2 = (
  first_1
  .withColumn('rownum_id', f.row_number().over(win_first_id_ord))
  .where(f.col('rownum_id') == 1)
  .drop('rownum_id', 'EPIKEY', 'DIAG_4_CONCAT', 'covid_flag')
)

# reformat
first_3 = (
  first_2
  .withColumn('noncovid_hosp_flag', f.lit(1))
  .withColumnRenamed('DATE', 'noncovid_hosp_date')
  .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'noncovid_hosp_flag', 'noncovid_hosp_date')
)
# temp save
first_3 = temp_save(df=first_3, out_name=f'{proj}_tmp_covariates_supp_first_3'); print() 

# check
count_varlist(first_3, ['PERSON_ID'])
tmpt = tabstat(first_3, 'noncovid_hosp_date', date=1); print()

# COMMAND ----------

# MAGIC %md # 7. Additional

# COMMAND ----------

# merge cov_1 and cohort ID
cov_supp_1 = merge(first_3, individual_censor_dates, ['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# check
count_var(cov_supp_1, 'PERSON_ID'); print()
tmpt = tab(cov_supp_1, 'noncovid_hosp_flag'); print()

# fill zero
cov_supp_2 = (
  cov_supp_1
  .na.fill(value=0, subset='noncovid_hosp_flag')
)

# check
tmpt = tab(cov_supp_2, 'noncovid_hosp_flag'); print()

# COMMAND ----------

# MAGIC %md # 8. Check

# COMMAND ----------

# check
count_var(cov_supp_2, 'PERSON_ID'); print()
print(len(cov_supp_2.columns)); print()
print(pd.DataFrame({f'_cols': cov_supp_2.columns}).to_string()); print()

# COMMAND ----------

# check
display(cov_supp_2)

# COMMAND ----------

# MAGIC %md # 9. Save

# COMMAND ----------

# save name
outName = f'{proj}_out_covariates_supp'.lower()

# save previous version for comparison purposes
tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
  .select('tableName')\
  .where(f.col('tableName') == outName)\
  .collect()
if(len(tmpt)>0):
  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
  print(outName_pre)
  spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
  spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
cov_supp_2.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
cov_supp_2 = spark.table(f'{dbc}.{outName}')

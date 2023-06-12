# Databricks notebook source
# MAGIC %md # CCU051_D11_outcomes
# MAGIC 
# MAGIC **Description** This notebook creates the outcomes for CCU051.
# MAGIC  * Covid hospitalisations
# MAGIC  * Deaths
# MAGIC   
# MAGIC **Author(s)** Alexia Sampri, Genevieve Cezard
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2023.01.18 (from CCU002_07)
# MAGIC 
# MAGIC **Date last updated** 2023.03.30
# MAGIC 
# MAGIC **Date last run** 2023.03.30
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters - manual codelist input\
# MAGIC 'ccu051_tmp_cohort'
# MAGIC 
# MAGIC **Data output** - 'CCU051_out_outcomes' - 'CCU051_tmp_outcomes_hes_apc' - 'CCU051_tmp_outcomes_deaths'
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

from pyspark.sql.functions import col,lit

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

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU051/CCU051_D01_parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_cohort""")
cohort       = spark.table(f'{dbc}.{proj}_out_cohort')

spark.sql(f"""REFRESH TABLE {path_cur_hes_apc_long}""")
hes_apc_long = spark.table(path_cur_hes_apc_long)

spark.sql(f"""REFRESH TABLE {path_cur_deaths_long}""")
deaths_long  = spark.table(path_cur_deaths_long)


# COMMAND ----------

# https://www.who.int/standards/classifications/classification-of-diseases/emergency-use-icd-codes-for-covid-19-disease-outbreak
codelist_covid = spark.createDataFrame(
  [
    ("covid", "ICD10", "U07.1", "COVID-19, virus identified", "", ""),
    ("covid", "ICD10", "U07.2", "COVID-19, Virus not identified", "", "")  
  ],
  ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']  
)

display(codelist_covid)

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
codelist_covid = codelist_covid\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', 'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', '[\.\-\s]', '')).otherwise(f.col('code')))
display(codelist_covid)

# COMMAND ----------

for terminology in ['ICD10']:
  print(terminology)
  ctmp = codelist_covid\
    .where(f.col('terminology') == terminology)
  count_var(ctmp, 'code'); print()

# COMMAND ----------

tmpt = tab(hes_apc_long, 'ADMIMETH')

# COMMAND ----------

win1 = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('DATE', f.desc('flag'))
win2 = Window\
  .partitionBy('PERSON_ID')

tmp1 = (
  hes_apc_long
  .join(cohort.select('PERSON_ID'), on='PERSON_ID', how='inner')
  .withColumnRenamed('EPISTART', 'DATE')
  .where((f.col('DATE') >= '2022-06-01') & (f.col('DATE') <= '2022-09-30'))
  .where(f.col('DIAG_POSITION') == 1)
  .where(f.col('CODE').isin(['U071', 'U072']))
  .withColumn('flag', f.when(f.col('ADMIMETH').rlike(r'^2'), 1).otherwise(0))
  .withColumn('rownum', f.row_number().over(win1))
  .withColumn('rownummax', f.count('PERSON_ID').over(win2))
)

# check
# tmpt = tab(tmp1.where(f.col('rownum') == 1), 'rownummax')

# COMMAND ----------

display(tmp1)

# COMMAND ----------

tmp2 = (
  tmp1
  .where(f.col('rownum') == 1)
)
tmpt = tab(tmp2, 'ADMIMETH')

# COMMAND ----------

tmp2a = (
  tmp1
  .where(f.col('flag') == 1)
  .withColumn('rownum1', f.row_number().over(win1))  
  .where(f.col('rownum1') == 1)
)
tmpt = tab(tmp2a, 'ADMIMETH')

# COMMAND ----------

count_var(tmp2, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

display(cohort)

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
# check
print(f'proj_end_date = {proj_end_date}')
assert proj_end_date == '2022-09-30'

individual_censor_dates = (
  cohort
  .select('PERSON_ID', f.col('baseline_date').alias('CENSOR_DATE_START'))
  .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{proj_end_date}'))))

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()


# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# check
tmpt = tab(hes_apc_long, 'DIAG_POSITION'); print()

# filter to primary diagnosis position
# reduce and rename columns 
hes_apc_long_prepared = hes_apc_long\
  .select(['PERSON_ID', 'EPISTART', 'CODE', 'DIAG_POSITION', 'ADMIMETH'])\
  .withColumnRenamed('EPISTART', 'DATE')

# # check
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()
# tmpt = tab(hes_apc_long_prepared, 'DIAG_POSITION'); print()

hes_apc_long_prepared = (
  hes_apc_long_prepared
  .join(individual_censor_dates, on=['PERSON_ID'], how='inner')
)

# # check
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

hes_apc_long_prepared = hes_apc_long_prepared\
  .where(\
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )
  
# # check
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# save 
# outName = f'{proj}_tmp_outcomes_hes_apc'.lower()  
# hes_apc_long_prepared.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
# hes_apc_long_prepared = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('deaths_long')
print('--------------------------------------------------------------------------------------')
# check
tmpt = tab(deaths_long, 'DIAG_POSITION'); print()

# filter to underlying diagnosis position
# reduce and rename columns
deaths_long_prepared = deaths_long\
  .select(['PERSON_ID', 'DATE', 'CODE', 'DIAG_POSITION'])

# check
# count_var(deaths_long_prepared, 'PERSON_ID'); print()
# tmpt = tab(deaths_long_prepared, 'DIAG_POSITION'); print()

# add individual censor dates
deaths_long_prepared = deaths_long_prepared\
  .join(individual_censor_dates, on='PERSON_ID', how='inner')

# check
# count_var(deaths_long_prepared, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
deaths_long_prepared = deaths_long_prepared\
  .where(\
    (f.col('DATE') >= f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

deaths_long_prepared = deaths_long_prepared.withColumn('ADMIMETH', lit(None))

# check
# count_var(deaths_long_prepared, 'PERSON_ID'); print()

# save 
# outName = f'{proj}_tmp_outcomes_deaths'.lower()  
# deaths_long_prepared.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
# deaths_long_prepared = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

display(hes_apc_long_prepared)

# COMMAND ----------

display(deaths_long_prepared)

# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------

# create extra outcome: COVID hospitalisation = emergency hospitalisation with covid as the MAIN cause of admission

# # Elective Admission, when the decision to admit could be separated in time from the actual admission: 
# 11 = Waiting list. . A Patient admitted electively from a waiting list having been given no date of admission at a time a decision was made to admit
# 12 = Booked. A Patient admitted having been given a date at the time the decision to admit was made, determined mainly on the grounds of resource availability
#  13 = Planned. A Patient admitted, having been given a date or approximate date at the time that the decision to admit was made. This is usually part of a planned sequence of clinical care determined mainly on social or clinical criteria (e.g. check cystoscopy)". A planned admission is one where the date of admission is determined by the needs of the treatment, rather than by the availability of resources.

# Note that this does not include a transfer from another Hospital Provider (see 81 below).

# Emergency Admission, when admission is unpredictable and at short notice because of clinical need: 

# 21 = Accident and emergency or dental casualty department of the Health Care Provider  
# 22 = General Practitioner: after a request for immediate admission has been made direct to a Hospital Provider, i.e. not through a Bed bureau, by a General Practitioner: or deputy 
# 23 = Bed bureau 
# 24 = Consultant Clinic, of this or another Health Care Provider  
# 25 = Admission via Mental Health Crisis Resolution Team (available from 2013/14)
# 2A = Accident and Emergency Department of another provider where the patient had not been admitted (available from 2013/14)
# 2B = Transfer of an admitted patient from another Hospital Provider in an emergency (available from 2013/14)
# 2C = Baby born at home as intended (available from 2013/14)
# 2D = Other emergency admission (available from 2013/14)
# 28 = Other means, examples are: 
# - Admitted from the Accident and Emergency Department of another provider where they had not been admitted
# - Transfer of an admitted patient from another Hospital Provider in an emergency
# - Baby born at home as intended 

# Maternity Admission, of a pregnant or recently pregnant woman to a maternity ward (including delivery facilities) except when the intention is to terminate the pregnancy:

# 31 = Admitted ante-partum 
# 32 = Admitted post-partum 

# Other Admission not specified above:

# 82 = The birth of a baby in this Health Care Provider  
# 83 = Baby born outside the Health Care Provider except when born at home as intended. 
# 81 = Transfer of any admitted patient from other Hospital Provider other than in an emergency
# 84 = Admission by Admissions Panel of a High Security Psychiatric Hospital, patient not entered on the HSPH Admissions Waiting List (available between 1999 and 2006)
# 89 = HSPH Admissions Waiting List of a High Security Psychiatric Hospital (available between 1999 and 2006)
# 98 = Not applicable (available from 1996/97)
# 99 = Not known: a validation error


hes_apc_long_prepared_emergency_pri = hes_apc_long_prepared.where(f.col('ADMIMETH').isin(['21', '22', '23','24', '25', '2A', '2B', '2C', '2D', '28'])) 

hes_apc_long_prepared_emergency_pri = (
  hes_apc_long_prepared_emergency_pri
  .where(f.col('DIAG_POSITION') == 1)   
)

# COMMAND ----------

hes_apc_long_prepared_pri = (
  hes_apc_long_prepared
  .where(f.col('DIAG_POSITION') == 1)  
)

deaths_long_prepared_pri = (
  deaths_long_prepared
  .where(f.col('DIAG_POSITION') == 'UNDERLYING')  
)

# COMMAND ----------

display(hes_apc_long_prepared_emergency_pri)

# COMMAND ----------

display(hes_apc_long_prepared_pri)

# COMMAND ----------

display(deaths_long_prepared_pri)

# COMMAND ----------

codelist_covid_hes_apc_emergency_pri = (
  codelist_covid
  .withColumn('name', f.lit('covid_hes_apc_emergency_pri'))
)

codelist_covid_hes_apc_pri = (
  codelist_covid
  .withColumn('name', f.lit('covid_hes_apc_pri'))
)

codelist_covid_hes_apc_any = (
  codelist_covid
  .withColumn('name', f.lit('covid_hes_apc_any'))
)

codelist_covid_deaths_pri = (
  codelist_covid
  .withColumn('name', f.lit('covid_deaths_pri'))
)

codelist_covid_deaths_any = (
  codelist_covid
  .withColumn('name', f.lit('covid_deaths_any'))
)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_out = {
    'hes_apc_emergency_pri': ['hes_apc_long_prepared_emergency_pri',     'codelist_covid_hes_apc_emergency_pri', 1]
  , 'hes_apc_pri': ['hes_apc_long_prepared_pri', 'codelist_covid_hes_apc_pri', 1]
  , 'hes_apc_any': ['hes_apc_long_prepared',     'codelist_covid_hes_apc_any', 1]
  , 'deaths_pri':  ['deaths_long_prepared_pri',  'codelist_covid_deaths_pri',  1]
  , 'deaths_any':  ['deaths_long_prepared',      'codelist_covid_deaths_any',  1]
}

# run codelist match and codelist match summary functions
#_out, _out_1st, _out_1st_wide = codelist_match(_out_hes, _name_prefix=f'out_pri_')
out, out_1st, out_1st_wide = codelist_match(dict_out, _name_prefix=f'out_')

#_out_summ_name, _out_summ_name_code = codelist_match_summ(_out_hes, _out)
out_summ_name, out_summ_name_code = codelist_match_summ(dict_out, out)

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

# MAGIC %md ## 4.0 Display

# COMMAND ----------

# check result
display(out_1st_wide)

# COMMAND ----------

# display(out_summ_name)
out_all = out['all']
tmpg1 = (
  out_all
  .groupBy('name')
  .agg(
    f.count(f.lit(1)).alias('n')
    , f.countDistinct(f.col('PERSON_ID')).alias('n_id')
  )
)
display(tmpg1)

# COMMAND ----------

display(out_summ_name_code)

# COMMAND ----------

out_all = out['all']
tmp1 = out_all.where(f.col('source') == 'deaths_any')
display(tmp1)

# COMMAND ----------

# MAGIC %md ## 4.1 Plots - First event - Over follow-up time (years) by data source (stacked)

# COMMAND ----------

# DBTITLE 1,Independent y-axes
# _tmp = _out_1st\
#   .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
# _tmpp = _tmp\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.4*rows_of_2), sharex=True) # , sharey=True , dpi=100) # 
 
# colors = sns.color_palette("tab10", 2)
# names = ['hes_apc', 'deaths']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) 
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
#   tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
#   s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
#   ax.hist([s1, s2], bins = list(np.linspace(0,0.4,100)), stacked=True, color=colors, label=names) # normed=True
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nFollow-up (years)\n')
#   ax.xaxis.set_tick_params(labelbottom=True)
#   if(i==0): ax.legend(loc='upper right')
    
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
# plt.tight_layout();
# display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
# _tmp = _out_1st\
#   .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
# _tmpp = _tmp\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.4*rows_of_2), sharex=True, sharey=True) #  , dpi=100) # 
 
# colors = sns.color_palette("tab10", 2)
# names = ['hes_apc', 'deaths']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) 
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
#   tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
#   s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
#   ax.hist([s1, s2], bins = list(np.linspace(0,3,100)), stacked=True, color=colors, label=names) # normed=True
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nFollow-up (years)\n')
#   ax.xaxis.set_tick_params(labelbottom=True)
#   if(i==0): ax.legend(loc='upper right')
    
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
# plt.tight_layout();
# display(fig)

# COMMAND ----------

# MAGIC %md ## 4.2 Plots - First event - Over calendar time by data source (stacked)

# COMMAND ----------

# DBTITLE 1,Independent y-axes
# _tmpp = _out_1st\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.8*rows_of_2), sharex=True) # , sharey=True , dpi=100) # 
 
# colors = sns.color_palette("tab10", 2)
# names = ['hes_apc', 'deaths']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
  
#   tmp2d1 = _tmpp[(_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'DATE'])
#   s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'DATE'])
#   ax.hist([s1, s2], bins=100, stacked=True, color=colors, label=names) # normed=True # bins = list(np.linspace(0,3,100))
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nDate\n')
#   if(i==0): ax.legend(loc='upper right')
  
#   # plt.draw()
#   # ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
#   ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)
  
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)


# plt.tight_layout();
# display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
# _tmpp = _out_1st\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.8*rows_of_2), sharex=True, sharey=True) # , sharey=True , dpi=100) # 
 
# colors = sns.color_palette("tab10", 2)
# names = ['hes_apc', 'deaths']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
  
#   tmp2d1 = _tmpp[(_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'DATE'])
#   s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'DATE'])
#   ax.hist([s1, s2], bins=100, stacked=True, color=colors, label=names) # normed=True # bins = list(np.linspace(0,3,100))
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nDate\n')
#   if(i==0): ax.legend(loc='upper right')
  
#   # plt.draw()
#   # ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
#   ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)
  
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)


# plt.tight_layout();
# display(fig)

# COMMAND ----------

# MAGIC %md ## 4.3 Plots - First event - Over age at event (years) by data source (stacked)

# COMMAND ----------

# DBTITLE 1,Independent y-axes
# # plot age instead of diff
# _tmp = (merge(_out_1st, cohort.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
#   .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
# _tmpp = _tmp\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.4*rows_of_2), sharex=True) # , sharey=True , dpi=100) # 
 
# colors = sns.color_palette("tab10", 2)
# names = ['hes_apc', 'deaths']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
#   tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
#   s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
#   ax.hist([s1, s2], bins = list(np.linspace(0,20,100)), stacked=True, color=colors, label=names) # normed=True
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nAge (years)\n')
#   ax.xaxis.set_tick_params(labelbottom=True)
#   if(i==0): ax.legend(loc='upper right')
    
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)

# plt.tight_layout();
# display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
# # plot age instead of diff
# _tmp = (merge(_out_1st, cohort.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
#   .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
# _tmpp = _tmp\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.4*rows_of_2), sharex=True) # , sharey=True , dpi=100) #  
 
# colors = sns.color_palette("tab10", 2)
# names = ['hes_apc', 'deaths']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
#   tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
#   s2 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
#   ax.hist([s1, s2], bins = list(np.linspace(0,20,100)), stacked=True, color=colors, label=names) # normed=True
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nAge (years)\n')
#   ax.xaxis.set_tick_params(labelbottom=True)
#   if(i==0): ax.legend(loc='upper right')
    
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)

# plt.tight_layout();
# display(fig)

# COMMAND ----------

# MAGIC %md ## 4.4 Plots - First event - Over age at event (years) by sex (overlapping)

# COMMAND ----------

# DBTITLE 1,Independent y-axes 
# # plot age instead of diff
# _tmp = (merge(_out_1st, cohort.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
#   .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
# _tmpp = _tmp\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.4*rows_of_2), sharex=True) # , sharey=True , dpi=100) # 
 
# colors = sns.color_palette("tab10", 2)
# names = ['Male', 'Female']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
#   tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'SEX'] == '1'][f'diff'])
#   s2 = list(tmp2d1[tmp2d1[f'SEX'] == '2'][f'diff'])
#   # ax.hist([s1, s2], bins = list(np.linspace(0,20,100)), stacked=True, color=colors, label=names) # normed=True 
#   ax.hist(s1, bins = list(np.linspace(0,110,100)), color=colors[0], label=names[0], alpha=0.5) # normed=True 
#   ax.hist(s2, bins = list(np.linspace(0,110,100)), color=colors[1], label=names[1], alpha=0.5) # normed=True 
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nAge (years)\n')
#   ax.xaxis.set_tick_params(labelbottom=True)
#   if(i==0): ax.legend(loc='upper right')
    
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
# plt.tight_layout();
# display(fig)

# COMMAND ----------

# DBTITLE 1,Shared y-axes
# # plot age instead of diff
# _tmp = (merge(_out_1st, cohort.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
#   .withColumn('diff', f.datediff(f.col('DATE'), f.col('DOB'))/365.25))
# _tmpp = _tmp\
#   .toPandas()

# plt.rcParams.update({'font.size': 8})
# rows_of_2 = np.ceil(len(_tmpp['name'].drop_duplicates())/2).astype(int)
# fig, axes = plt.subplots(rows_of_2, 2, figsize=(13,2.4*rows_of_2), sharex=True) # , sharey=True , dpi=100) # 
 
# colors = sns.color_palette("tab10", 2)
# names = ['Male', 'Female']  
  
# vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
# for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
#   print(i, ax, v)
#   tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
#   s1 = list(tmp2d1[tmp2d1[f'SEX'] == '1'][f'diff'])
#   s2 = list(tmp2d1[tmp2d1[f'SEX'] == '2'][f'diff'])
#   # ax.hist([s1, s2], bins = list(np.linspace(0,20,100)), stacked=True, color=colors, label=names) # normed=True
#   ax.hist(s1, bins = list(np.linspace(0,110,100)), color=colors[0], label=names[0], alpha=0.5) # normed=True 
#   ax.hist(s2, bins = list(np.linspace(0,110,100)), color=colors[1], label=names[1], alpha=0.5) # normed=True 
#   ax.set_title(f'{v}')
#   ax.set(xlabel='\nAge (years)\n')
#   ax.xaxis.set_tick_params(labelbottom=True)
#   if(i==0): ax.legend(loc='upper right')
    
# # axes[3,1].set_axis_off()
# # axes[3,2].set_axis_off()
# # axes[3,3].set_axis_off()
# # axes[3,4].set_axis_off()
# # for i in range(0,3):
# #   for j in range(0, 5):
# #     axes[i,j].xaxis.set_tick_params(labelbottom=True)
    
# plt.tight_layout();
# display(fig)

# COMMAND ----------

# MAGIC %md ## 4.5 Numerical summaries of plots

# COMMAND ----------

# check numerical summaries 
tmpf = (merge(out_1st_wide, cohort.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'])
        .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
        .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25)
        .withColumn('name_source', f.concat_ws('_', 'name', 'source'))
        .withColumn('name_sex', f.concat_ws('_', 'name', 'SEX'))); print()
tmpt = tabstat(tmpf, 'diff', byvar='name_source'); print()
tmpt = tabstat(tmpf, 'DATE', byvar='name_source', date=1); print()
tmpt = tabstat(tmpf, 'age',  byvar='name_source'); print()
tmpt = tabstat(tmpf, 'age',  byvar='name_sex'); print()

# COMMAND ----------

display(out_1st_wide)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_outcomes'.lower()

# save
out_1st_wide.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

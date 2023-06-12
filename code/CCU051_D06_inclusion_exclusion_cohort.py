# Databricks notebook source
# MAGIC %md # CCU051_D06_inclusion_exclusion_cohort
# MAGIC  
# MAGIC **Description** This notebook creates the temporary **cohort** and **inclusion exclusion** table.
# MAGIC  
# MAGIC **Authors** Genevieve Cezard, Alexia Sampri
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2022.12.06 (from CCU002_07)
# MAGIC 
# MAGIC **Date last updated** 2023.01.17
# MAGIC 
# MAGIC **Date last run** 2023.01.26 (data batch 2022.12.31) / (run on 2023.05.03 just to check something)
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC 'ccu051_tmp_skinny'\
# MAGIC 'ccu051_tmp_quality_assurance'\
# MAGIC deaths ('ccu051_cur_deaths_dars_nic_391419_j3w9t_archive_sing')
# MAGIC 
# MAGIC **Data output** - 'ccu051_tmp_cohort' - 'ccu051_tmp_flow_inc_exc'\
# MAGIC intermediate dataset: 'ccu051_tmp_inclusion_exclusion_merged'
# MAGIC 
# MAGIC **Notes** \
# MAGIC Inclusion criteria are as follows:
# MAGIC 1. Has a record in the primary care extract (GDPPR) 
# MAGIC 2. Has a record of sex, age, year of birth
# MAGIC 3. Has a record of local authority (LSOA)
# MAGIC 4. Has a record of a region in England
# MAGIC 5. Recorded as alive on the study start date
# MAGIC 6. Age 5 years old and over on the study start date
# MAGIC 
# MAGIC In addition, we may want to exclude:\
# MAGIC 7. Patients who failed quality assurance rules (1-8) as defined in `CCU051_D05_quality_assurance`\
# MAGIC 8. Patients aged over 110 years old on the study start date
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

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_tmp_skinny""")
spark.sql(f"""REFRESH TABLE {dbc}.{proj}_tmp_quality_assurance""")

skinny  = spark.table(f'{dbc}.{proj}_tmp_skinny')
deaths  = spark.table(path_cur_deaths_sing)
qa      = spark.table(f'{dbc}.{proj}_tmp_quality_assurance')

# COMMAND ----------

display(qa)

# COMMAND ----------

display(skinny)

# COMMAND ----------

display(deaths)

# COMMAND ----------

display(qa)

# COMMAND ----------

#display(sgss)

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('skinny')
print('---------------------------------------------------------------------------------')
# reduce
_skinny = skinny.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'LSOA', 'in_gdppr','region','IMD_2019_deciles','IMD_2019_quintiles','RUC11_bin','RUC11_code','RUC11_name')

# check
count_var(_skinny, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('deaths')
print('---------------------------------------------------------------------------------')
# reduce
_deaths = deaths.select('PERSON_ID', f.col('REG_DATE_OF_DEATH').alias('DOD'))

# check
count_var(_deaths, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('quality assurance')
print('---------------------------------------------------------------------------------')
_qa = qa

# check
count_var(_qa, 'PERSON_ID'); print()
tmpt = tab(_qa, '_rule_total'); print()


print('---------------------------------------------------------------------------------')
print('merged')
print('---------------------------------------------------------------------------------')
# merge skinny and deaths
_merged = (
  merge(_skinny, _deaths, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'])
  .withColumn('in_deaths', f.when(f.col('_merge') == 'both', 1).otherwise(0))
  .drop('_merge')); print()

# merge in qa
_merged = (
  merge(_merged, _qa, ['PERSON_ID'], validate='1:1', assert_results=['both'])
  .withColumn('in_qa', f.when(f.col('_merge') == 'both', 1).otherwise(0))
  .drop('_merge')); print()

# check
count_var(_merged, 'PERSON_ID'); print()

# temp save
outName = f'{proj}_tmp_inclusion_exclusion_merged'.lower()
_merged.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
_merged = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

# check
tmpt = tab(_merged, 'in_gdppr'); print()
tmpt = tab(_merged, 'in_deaths'); print()
tmpt = tab(_merged, 'in_qa'); print()
tmpt = tab(_merged, '_rule_total'); print()

# COMMAND ----------

# MAGIC %md # 3. Inclusion / exclusion

# COMMAND ----------

tmp0 = _merged
tmpc = count_var(tmp0, 'PERSON_ID', ret=1, df_desc='original', indx=0); print()

# COMMAND ----------

# MAGIC %md ## 3.1. Exclude patients not in GDPPR

# COMMAND ----------

# 1. Has a record in the primary care extract (GDPPR)

# filter out patients not in GDPPR
tmp1 = tmp0.where(f.col('in_gdppr') == 1)

# check
tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='Exclusion of patients not in GDPPR', indx=1); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.2. Exclude patients without sex, age or date of birth

# COMMAND ----------

#2. Has a record of sex, age, year of birth
# If no date of birth then no age, so we just need to apply the rule on age
# Note: take the opposite of (f.col('SEX').isNull()) | (~f.col('SEX').isin([1,2])) | (f.col('DOB').isNull())

# filter out patients without sex - age - year of birth ()
tmp2 = tmp1.where( (f.col('SEX').isin([1,2])) &  (~f.col('DOB').isNull()))

# check
tmpt = count_var(tmp2, 'PERSON_ID', ret=1, df_desc='Exclusion of patients without sex, age or date of birth', indx=2); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.3. Exclude patients without LSOA

# COMMAND ----------

#3. Has a record of local authority (LSOA)

# filter out patients without LSOA
tmp3 = tmp2.where(~f.col('LSOA').isNull())

# check
tmpt = count_var(tmp3, 'PERSON_ID', ret=1, df_desc='Exclusion of patients without LSOA', indx=3); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.4. Exclude patients with region outside of England

# COMMAND ----------

# 4. Has a record of region in England

# filter out patients without a region in England
tmp4 = tmp3.where(~f.col('region').isin(['None','Scotland','Wales']))

# check
tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='Exclusion of patients with region outside England', indx=4); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.5. Exclude patients who died before baseline

# COMMAND ----------

# 5. Recorded as alive on the study start date

# filter out patients who died before baseline (2022-06-01)
tmp5 = tmp4.where( (f.col('DOD').isNull()) | (f.col('DOD')> f.to_date(f.lit('2022-06-01'))) )

# check
tmpt = count_var(tmp5, 'PERSON_ID', ret=1, df_desc='Exclusion of patients who died before baseline (2022-06-01)', indx=5); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.6. Exclude patients aged 0-4 years old

# COMMAND ----------

# 6. Age 5 years old and over on the study start date
# Note : Someone who turn 5 on 2022-06-01 is born on 2017-06-01. Anyone born on or before that should be included.

# filter out patients who were born after 2017-06-01
tmp6 = tmp5.where(f.col('DOB') <= f.to_date(f.lit('2017-06-01')))

# check
tmpt = count_var(tmp6, 'PERSON_ID', ret=1, df_desc='Exclusion of patients aged < 5 at baseline (2022-06-01)', indx=6); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.7. Exclude patients who failed the quality assurance

# COMMAND ----------

# 7. Filter out patients who failed the qaulity assurance
tmp7 = tmp6.where(f.col('_rule_total') != 1)
 
# check
tmpt = count_var(tmp7, 'PERSON_ID', ret=1, df_desc='Exclusion of patients who failed the quality assurance', indx=7); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.8. Exclude patients aged over 110 years old on the study start date

# COMMAND ----------

# 8. Filter out patients who are aged over 110 years old and over on the study start date
# Note : Someone who turns 110 on 2022-06-01 is born on 1912-06-01. Anyone born before that should be excluded.

# filter out patients who were born after 2017-06-01
tmp8 = tmp7.where(f.col('DOB') >= f.to_date(f.lit('1912-06-01')))

# check
tmpt = count_var(tmp8, 'PERSON_ID', ret=1, df_desc='Exclusion of patients who are aged over 110 years old at baseline', indx=8); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md # 4. Flow diagram

# COMMAND ----------

# check flow table
tmpp = (
  tmpc
  .orderBy('indx')
  .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
  .withColumnRenamed('df_desc', 'stage')
  .toPandas())
tmpp['n'] = tmpp['n'].astype(int)
tmpp['n_id'] = tmpp['n_id'].astype(int)
tmpp['n_id_distinct'] = tmpp['n_id_distinct'].astype(int)
tmpp['n_diff'] = (tmpp['n'] - tmpp['n'].shift(1)).fillna(0).astype(int)
tmpp['n_id_diff'] = (tmpp['n_id'] - tmpp['n_id'].shift(1)).fillna(0).astype(int)
tmpp['n_id_distinct_diff'] = (tmpp['n_id_distinct'] - tmpp['n_id_distinct'].shift(1)).fillna(0).astype(int)
for v in [col for col in tmpp.columns if re.search("^n.*", col)]:
  tmpp.loc[:, v] = tmpp[v].map('{:,.0f}'.format)
for v in [col for col in tmpp.columns if re.search(".*_diff$", col)]:  
  tmpp.loc[tmpp['stage'] == 'original', v] = ''
# tmpp = tmpp.drop('indx', axis=1)
print(tmpp.to_string()); print()

# COMMAND ----------

# MAGIC %md # 5. Save

# COMMAND ----------

# MAGIC %md ## 5.1. Cohort

# COMMAND ----------

tmpf = tmp7.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'DOD', 'LSOA','region','IMD_2019_deciles','IMD_2019_quintiles','RUC11_bin','RUC11_code','RUC11_name')

# check
count_var(tmpf, 'PERSON_ID')

# COMMAND ----------

# check 
display(tmpf)

# COMMAND ----------

# save name
outName = f'{proj}_tmp_cohort'.lower()

# save
tmpf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 5.2. Flow

# COMMAND ----------

# save name
outName = f'{proj}_tmp_flow_inc_exc'.lower()

# save
tmpc.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# suppress cols in tmp1 by creating a new dataframe tmpp2 - to be able to export
tmpp2 = tmpc
cols = ['n', 'n_id', 'n_id_distinct']
for i, var in enumerate(cols):
  tmpp2 = tmpp2.withColumn(var, f.col(var).cast(t.IntegerType()))
  typ = dict(tmpp2.dtypes)[var]
  print(i, var, typ)  
  assert str(typ) in('bigint')
  assert tmpp2.where(f.col(var)<0).count() == 0
  tmpp2 = (tmpp2
         .withColumn(var,
                     f.when(f.col(var) == 0, 0)
                     .when(f.col(var) < 10, 10)
                     .when(f.col(var) >= 10, 5*f.round(f.col(var)/5))
                    )
        )

# COMMAND ----------

display(tmpp2)

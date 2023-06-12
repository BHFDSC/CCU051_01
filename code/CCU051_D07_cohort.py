# Databricks notebook source
# MAGIC %md
# MAGIC # CCU051_D07_cohort
# MAGIC  
# MAGIC **Description** This notebook creates the final **cohort** table.
# MAGIC  
# MAGIC **Authors** Genevieve Cezard, Alexia Sampri, Tom Bolton
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2022.12.21 (from CCU002_07)
# MAGIC 
# MAGIC **Date last updated** 2022.12.21
# MAGIC 
# MAGIC **Date last run** 2023.01.26 (data batch 2022.12.31)
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC 'ccu051_tmp_cohort'
# MAGIC 
# MAGIC **Data output** - 'ccu051_out_cohort' 
# MAGIC 
# MAGIC **Notes** \
# MAGIC Addition of baseline date and age at baseline
# MAGIC 
# MAGIC **Acknowledgements** Adapted from previous work by Tom Bolton (John Nolan, Elena Raffetti, Alexia Sampri) from CCU002_07, CCU018_01 and earlier CCU002 sub-projects.

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

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU051_D01_parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_tmp_cohort""")
cohort = spark.table(f'{dbc}.{proj}_tmp_cohort')

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

display(cohort)

# COMMAND ----------

# MAGIC %md # 3. Create

# COMMAND ----------

tmp = (cohort
        .withColumn('baseline_date', f.to_date(f.lit(proj_start_date)))
        .withColumn('AGE', f.round(f.datediff(f.lit(proj_start_date), f.col('DOB'))/365.25, 0)))
#        .withColumn('baseline_age', f.round(f.datediff(f.lit(proj_start_date), f.col('DOB'))/365.25, 2)))

# COMMAND ----------

# MAGIC %md # 4. Check

# COMMAND ----------

display(tmp)

# COMMAND ----------

count_var(tmp, 'PERSON_ID'); print()
tmpt = tab(tmp, 'SEX'); print()
tmpt = tab(tmp, 'ETHNIC_CAT'); print()
tmpt = tabstat(tmp, 'AGE'); print()
tmpt = tabstat(tmp, 'DOB', date=1); print()

# COMMAND ----------

tmpt = tab(tmp, 'DOB'); print()

# COMMAND ----------

# DBTITLE 1,Age at baseline
tmpp = (tmp
        .groupBy('AGE')
        .agg(f.count(f.lit(1)).alias('n'))
        .toPandas()
       )

plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['AGE'],tmpp['n'], width = 1/12, edgecolor = "black")
axes.set(xticks=np.arange(0, 110, step=5))
axes.set_xlim(0,110)
axes.set(xlabel="Age at baseline (years)")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
display(fig)

# COMMAND ----------

# DBTITLE 1,Date of birth
tmpp = (tmp
        .groupBy('DOB')
        .agg(f.count(f.lit(1)).alias('n'))
        .toPandas()
       )
tmpp['date_formatted'] = pd.to_datetime(tmpp['DOB'], errors='coerce')

# plot
plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['date_formatted'], tmpp['n'], width = 28, edgecolor = "black")
# axes.set(xticks=np.arange(0, 19, step=1))
axes.set_xlim(datetime.datetime(1912, 1, 1), datetime.datetime(2022, 11, 1))
axes.set(xlabel="Date of birth")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
display(fig)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_cohort'.lower()

# save
tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

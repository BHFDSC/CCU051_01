# Databricks notebook source
# MAGIC %md
# MAGIC # CCU051_D10_vaccination
# MAGIC  
# MAGIC **Description** This notebook creates the finalise the vaccination variables needed for this project.
# MAGIC  
# MAGIC **Authors** Genevieve Cezard
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First created** 2023.01.24
# MAGIC 
# MAGIC **Date last updated** 2023.02.07
# MAGIC 
# MAGIC **Date last run** 2023.02.07
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC 'ccu051_tmp_cohort'
# MAGIC 
# MAGIC **Data output** - 'ccu051_out_vaccination' 
# MAGIC 
# MAGIC **Notes** \ created 1 extra vaccination variable and renamed date and type variables as per Scotland code for COALESCE project

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

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_cur_vacc_reshaped""")
vaccination = spark.table(f'{dbc}.{proj}_cur_vacc_reshaped')

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

display(vaccination)

# COMMAND ----------

# MAGIC %md # 3. Create

# COMMAND ----------

vacc1 = vaccination\
    .withColumn('num_doses_recent', f.when(f.col('dose_sequence_max') > 5, 5).otherwise(f.col('dose_sequence_max')))\



#.withColumnRenamed('date_1', 'date_vacc_1')
#.withColumnRenamed('type_1', 'vacc_type_1')
# -> Rename in a loop (see cmd 13-14)   
  
# Other vaccination variable such as 'fully_vaccinated' (depends on age group) or 'num_dose_start' (N dose at study start) will be created in R on the combined dataset

# COMMAND ----------

# rename columns as per Scotland code for COALESCE project i.e. date_1 -> date_vacc_1
dict_rename = {}
for col in vacc1.columns:
  col_rematch_date = re.match(r'(date)\_(\d)', col)
  if(col_rematch_date):
    dict_rename[col] = col_rematch_date.group(1) + '_vacc_' + col_rematch_date.group(2)
    
vacc2 = rename_columns(vacc1, dict_rename); print()

# COMMAND ----------

# rename columns as per Scotland code for COALESCE project i.e. type_1 -> vacc_type_1
dict_rename = {}
for col in vacc2.columns:
  col_rematch_type = re.match(r'(type)\_(\d)', col)
  if(col_rematch_type):
    dict_rename[col] = 'vacc_' + col_rematch_type.group(1) + '_' + col_rematch_type.group(2)
tmp = rename_columns(vacc2, dict_rename); print()

# COMMAND ----------

#tmpt = tab(tmp,'date_vacc_1'); print()

# COMMAND ----------

# MAGIC %md # 4. Check

# COMMAND ----------

display(tmp)

# COMMAND ----------

count_var(tmp, 'PERSON_ID'); print()
tmpt = tab(tmp, 'num_doses_recent'); print()
tmpt = tab(tmp, 'dose_sequence_max'); print()
tmpt = tab(tmp, 'num_doses_recent','dose_sequence_max'); print()

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_vaccination'.lower()

# save
tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

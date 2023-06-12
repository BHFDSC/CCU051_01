# Databricks notebook source
# MAGIC %md # CCU051_D04_skinny
# MAGIC  
# MAGIC **Description** This notebook creates the skinny table for CCU051.
# MAGIC  
# MAGIC **Authors** Genevieve Cezard, Tom Bolton
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2022.10.04 (from CCU002_07/CCU018 adapted with addition of rural/urban classification)
# MAGIC 
# MAGIC **Date last updated** 2022.12.12
# MAGIC 
# MAGIC **Date last run** 2023.01.25 (run in about 4h - data batch 2022.12.31)
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC ccu051_cur_hes_apc_all_years_archive_long - ccu051_cur_hes_apc_all_years_archive_oper_long\
# MAGIC ccu051_cur_deaths_dars_nic_391419_j3w9t_archive_long - ccu051_cur_deaths_dars_nic_391419_j3w9t_archive_sing\
# MAGIC ccu051_cur_lsoa_region_lookup - ccu051_cur_lsoa_imd_lookup - ccu051_cur_lsoa_ruc_lookup\
# MAGIC ccu051_cur_ethnic_desc_cat_lookup\
# MAGIC deaths - gdppr - hes_apc - hes_op - hes_ae
# MAGIC  
# MAGIC **Data output** - 'ccu051_tmp_skinny' (with age - sex - ethinicity - ethnic cat - LSOA - region - IMD - RUC)\
# MAGIC (and intermediate datasets 'ccu051_tmp_skinny_assembled' and 'ccu051_tmp_skinny_unassembled')
# MAGIC 
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects. 

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

# DBTITLE 1,Functions_skinny
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/skinny"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU051/CCU051_D01_parameters" 

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')
gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
hes_op  = extract_batch_from_archive(parameters_df_datasets, 'hes_op')
hes_ae  = extract_batch_from_archive(parameters_df_datasets, 'hes_ae')

spark.sql(f"""REFRESH TABLE {path_cur_ethnic_desc_cat}""")
spark.sql(f"""REFRESH TABLE {path_cur_lsoa_region}""")
spark.sql(f"""REFRESH TABLE {path_cur_lsoa_imd}""")
spark.sql(f"""REFRESH TABLE {path_cur_lsoa_ruc}""")

ethnic_desc_cat = spark.table(path_cur_ethnic_desc_cat)
lsoa_region     = spark.table(path_cur_lsoa_region)
lsoa_imd        = spark.table(path_cur_lsoa_imd)
lsoa_ruc        = spark.table(path_cur_lsoa_ruc)

# COMMAND ----------

# MAGIC %md # 2. Skinny

# COMMAND ----------

# # skinny
# # Note: individual censor dates option used
# #       prioritise primary care records turned off - happy to take the most recent HES (delivery) record
# # unassembled = skinny_unassembled(_hes_apc=hes_apc, _hes_ae=hes_ae, _hes_op=hes_op, _gdppr=gdppr, _deaths=deaths)
# # assembled = skinny_assembled(_unassembled=unassembled, _overall_censor_date='2022-06-01')


# we remove records in the future for hes_ae because the batch is not equal to 2022-11-30
# hes_ae_filtered = (hes_ae.where(f.col('ARRIVALDATE') <= f.to_date(f.lit('2022-10-31'))))

# Using the date of the latest batch
# unassembled = skinny_unassembled(_hes_apc=hes_apc, _hes_ae=hes_ae_filtered, _hes_op=hes_op, _gdppr=gdppr, _deaths=deaths)
unassembled = skinny_unassembled(_hes_apc=hes_apc, _hes_ae=hes_ae, _hes_op=hes_op, _gdppr=gdppr, _deaths=deaths)

# temp save
outName = f'{proj}_tmp_skinny_unassembled'.lower()
unassembled.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
unassembled = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

assembled = skinny_assembled(_unassembled=unassembled, _overall_censor_date='2022-12-31')

# temp save
outName = f'{proj}_tmp_skinny_assembled'.lower()
assembled.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
assembled = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# check
# count_var(unassembled, 'PERSON_ID'); print()
count_var(assembled, 'PERSON_ID'); print()

# COMMAND ----------

# check
display(assembled)

# COMMAND ----------

# MAGIC %md # 3. Ethnic desc cat

# COMMAND ----------

# add ethnicity description and category

# check
count_var(ethnic_desc_cat, 'ETHNIC'); print()
count_var(assembled, 'ETHNIC'); print()

# merge (right_only) - equivalent to left join
assembled = merge(assembled, ethnic_desc_cat, ['ETHNIC'], validate='m:1', keep_results=['both', 'left_only']); print()

# check
tmpt = tab(assembled, 'ETHNIC', '_merge', var2_unstyled=1); print()
count_var(assembled, 'PERSON_ID'); print()

# check
tmpt = tab(assembled, 'ETHNIC_DESC', 'ETHNIC_CAT', var2_unstyled=1); print()
tmpt = tab(assembled, 'ETHNIC_CAT'); print()

# edit
assembled = (
  assembled
  .withColumn('ETHNIC_CAT', f.when(f.col('ETHNIC_CAT').isNull(), 'Unknown').otherwise(f.col('ETHNIC_CAT')))
)

# check
tmpt = tab(assembled, 'ETHNIC_CAT'); print()

# tidy
assembled = assembled.drop('_merge')

# COMMAND ----------

# MAGIC %md # 4. Region

# COMMAND ----------

# prepare
lsoa_region = lsoa_region.select('LSOA', 'region')

# check
count_var(lsoa_region, 'LSOA'); print()
count_var(assembled, 'LSOA'); print()
tmpt = tab(lsoa_region, 'region'); print()

# merge
assembled = merge(assembled, lsoa_region, ['LSOA'], validate='m:1', keep_results=['both', 'left_only'])\
  .withColumn('LSOA_1', f.substring(f.col('LSOA'), 1, 1)); print()

# check
tmpt = tab(assembled, 'LSOA_1', '_merge', var2_unstyled=1); print()
count_var(assembled, 'PERSON_ID'); print()

# edit
assembled = (
  assembled
  .withColumn('region',
    f.when(f.col('LSOA_1') == 'W', 'Wales')
     .when(f.col('LSOA_1') == 'S', 'Scotland')
     .otherwise(f.col('region'))
  )
)

# check
tmpt = tab(assembled, 'region'); print()

# tidy
assembled = assembled.drop('_merge', 'LSOA_1')

# COMMAND ----------

# MAGIC %md # 5. IMD

# COMMAND ----------

# check
count_var(lsoa_imd, 'LSOA'); print()
count_var(assembled, 'LSOA'); print()
tmpt = tab(lsoa_imd, 'IMD_2019_DECILES', 'IMD_2019_QUINTILES', var2_unstyled=1); print()

# merge
assembled = merge(assembled, lsoa_imd, ['LSOA'], validate='m:1', keep_results=['both', 'left_only'])\
  .withColumn('LSOA_1', f.substring(f.col('LSOA'), 1, 1)); print()

# check
tmpt = tab(assembled, 'LSOA_1', '_merge', var2_unstyled=1); print()
count_var(assembled, 'PERSON_ID'); print()

# check
tmpt = tab(assembled, 'IMD_2019_QUINTILES'); print()

# tidy
assembled = assembled.drop('_merge', 'LSOA_1')

# COMMAND ----------

# MAGIC %md # 6. Rural/Urban Classification (RUC)

# COMMAND ----------

# check
count_var(lsoa_ruc, 'LSOA'); print()
count_var(assembled, 'LSOA'); print()
tmpt = tab(lsoa_ruc, 'RUC11_name', 'RUC11_bin', var2_unstyled=1); print()

# merge
assembled = merge(assembled, lsoa_ruc, ['LSOA'], validate='m:1', keep_results=['both', 'left_only'])\
  .withColumn('LSOA_1', f.substring(f.col('LSOA'), 1, 1)); print()

# check
tmpt = tab(assembled, 'LSOA_1', '_merge', var2_unstyled=1); print()
count_var(assembled, 'PERSON_ID'); print()

# check
tmpt = tab(assembled, 'RUC11_name'); print()

# tidy
assembled = assembled.drop('_merge', 'LSOA_1')

# COMMAND ----------

# MAGIC %md # 7. Checks

# COMMAND ----------

# check
count_var(assembled, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 8. Quick assembly

# COMMAND ----------

# Quicker assembly
assembled_test = (
  spark.table(f'{dbc}.{proj}_tmp_skinny_assembled')
  .join(ethnic_desc_cat, on='ETHNIC', how='left')
  .withColumn('ETHNIC_CAT', f.when(f.col('ETHNIC_CAT').isNull(), 'Unknown').otherwise(f.col('ETHNIC_CAT')))
  .join(lsoa_region.select('LSOA', 'region'), on='LSOA', how='left')
  .withColumn('region',
    f.when(f.substring(f.col('LSOA'), 1, 1) == 'W', 'Wales')
    .when(f.substring(f.col('LSOA'), 1, 1) == 'S', 'Scotland')
    .otherwise(f.col('region'))
  )
  .join(lsoa_imd, on='LSOA', how='left')
  .join(lsoa_ruc, on='LSOA', how='left')
)

# reorder columns
vlist_ordered = [
  'PERSON_ID'
  , 'DOB'
  , 'SEX'
  , 'ETHNIC'
  , 'ETHNIC_DESC'
  , 'ETHNIC_CAT'
  , 'LSOA'
  , 'region'
  , 'IMD_2019_DECILES'
  , 'IMD_2019_QUINTILES'
  , 'RUC11_code'
  , 'RUC11_name'
  , 'RUC11_bin'
  , 'DOD'
  , 'in_gdppr'
  , '_date_DOB'
  , '_date_SEX'
  , '_date_ETHNIC'
  , '_date_LSOA'
  , '_date_DOD'  
  , '_source_DOB'
  , '_source_SEX'
  , '_source_ETHNIC'
  , '_source_LSOA'
  , '_source_DOD'   
  , '_tie_DOB'
  , '_tie_SEX'
  , '_tie_ETHNIC'
  , '_tie_LSOA'
  , '_tie_DOD'
]
vlist_unordered = [v for v in assembled_test.columns if v not in vlist_ordered]
vlist_all = vlist_ordered + vlist_unordered
assembled_test = assembled_test.select(*vlist_all)

# COMMAND ----------

# check assembled_test
display(assembled_test)

# COMMAND ----------

# check
count_var(assembled_test, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 9. Save

# COMMAND ----------

# save name
outName = f'{proj}_tmp_skinny'.lower()

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
assembled_test.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

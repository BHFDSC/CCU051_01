# Databricks notebook source
# MAGIC %md # CCU051_D03a_curated_data
# MAGIC 
# MAGIC **Description** This notebook produces the curated tables.
# MAGIC 
# MAGIC **Author(s)** Genevieve Cezard, Tom Bolton, Alexia Sampri
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2022.11.07 (from CCU018)
# MAGIC 
# MAGIC **Date last updated** 2023.03.30
# MAGIC 
# MAGIC **Date last run** 2023.03.30
# MAGIC 
# MAGIC **Data input** functions - libraries - parameters\
# MAGIC hes_apc - deaths\
# MAGIC look-up tables in dss_corporate : geog & imd ref - hes & gdppr ethnicity cat ref
# MAGIC 
# MAGIC **Data output** \
# MAGIC 'ccu051_cur_hes_apc_all_years_archive_long' - 'ccu051_cur_hes_apc_all_years_archive_oper_long'\
# MAGIC 'ccu051_cur_deaths_dars_nic_391419_j3w9t_archive_long' - 'ccu051_cur_deaths_dars_nic_391419_j3w9t_archive_sing'\
# MAGIC 'ccu051_cur_lsoa_region_lookup' - 'ccu051_cur_lsoa_imd_lookup'\
# MAGIC 'ccu051_cur_ethnic_desc_cat_lookup'\
# MAGIC 'ccu051_cur_ecds_all_years_archive_long'
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

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU051/CCU051_D01_parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')
#gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
#hes_op  = extract_batch_from_archive(parameters_df_datasets, 'hes_op')
#hes_ae  = extract_batch_from_archive(parameters_df_datasets, 'hes_ae')
ecds = extract_batch_from_archive(parameters_df_datasets, 'ecds')

#hes_apc      = spark.table(path_in_hes_apc)
# hes_apc_otr  = spark.table(path_hes_apc_otr)
#deaths       = spark.table(path_in_deaths)

geog         = spark.table(path_ref_geog)
imd          = spark.table(path_ref_imd)
ethnic_hes   = spark.table(path_ref_ethnic_hes)
ethnic_gdppr = spark.table(path_ref_ethnic_gdppr)

# COMMAND ----------

# MAGIC %md # 2. HES_APC

# COMMAND ----------

_hes_apc = hes_apc\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIMETH'] + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'EPIKEY')  
display(_hes_apc)

# COMMAND ----------

# check
count_var(hes_apc, 'PERSON_ID_DEID'); print()
count_var(hes_apc, 'EPIKEY'); print()
# count_var(hes_apc_otr, 'PERSON_ID_DEID'); print()
# count_var(hes_apc_otr, 'EPIKEY'); print()

# COMMAND ----------

# MAGIC %md ## 2.1. Diag

# COMMAND ----------

# MAGIC %md ### 2.1.1. Create

# COMMAND ----------

# check null EPISTART and potential of using ADMIDATE to supplement
tmp1 = hes_apc\
  .select('EPISTART', 'ADMIDATE', 'ADMIMETH')\
  .withColumn('_EPISTART', f.when(f.col('EPISTART').isNotNull(), 1))\
  .withColumn('_ADMIDATE', f.when(f.col('ADMIDATE').isNotNull(), 1))\
  .withColumn('_EPISTART_dummy', f.when(f.col('EPISTART').isin(['1800-01-01', '1801-01-01']), 1))\
  .withColumn('_ADMIDATE_dummy', f.when(f.col('ADMIDATE').isin(['1800-01-01', '1801-01-01']), 1))
tmpt = tab(tmp1, '_EPISTART', '_ADMIDATE', var2_unstyled=1); print()
tmpt = tab(tmp1, '_EPISTART_dummy', '_ADMIDATE_dummy', var2_unstyled=1); print()
# display(hes_apc.where(f.col('EPISTART').isNull()))

# little to be gained from using ADMIDATE and no other date variables 

# COMMAND ----------

_hes_apc = hes_apc\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIMETH'] + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'EPIKEY')  

# reshape twice, tidy, and remove records with missing code
hes_apc_long = reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIMETH'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
hes_apc_long = reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'ADMIMETH', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

# MAGIC %md ### 2.1.2. Check

# COMMAND ----------

# check
count_var(hes_apc_long, 'PERSON_ID'); print()
count_var(hes_apc_long, 'EPIKEY'); print()

# check removal of trailing X
tmpt = hes_apc_long\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpt, 'flag'); print()

# COMMAND ----------

display(hes_apc_long)

# COMMAND ----------

# MAGIC %md ### 2.1.3. Save

# COMMAND ----------

# save name
outName = f'{proj}_cur_hes_apc_all_years_archive_long'.lower() 

# save
hes_apc_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 2.2. Oper

# COMMAND ----------

# MAGIC %md ### 2.2.1. Create

# COMMAND ----------

# TODO use OPDATE (c.f., EPISTART), which can be found in the OTR files

oper_out = hes_apc\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID', 'EPIKEY', 'EPISTART'] + [col for col in list(hes_apc.columns) if re.match(r'^OPERTN_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'EPIKEY')
  
# reshape twice, tidy, and remove records with missing code
oper_out_long = reshape_wide_to_long_multi(oper_out, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='DIAG_POSITION', stubnames=['OPERTN_4_', 'OPERTN_3_'])
oper_out_long = reshape_wide_to_long_multi(oper_out_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'DIAG_POSITION'], j='DIAG_DIGITS', stubnames=['OPERTN_'])\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('OPERTN_', f.regexp_replace('OPERTN_', r'[.,\-\s]', ''))\
  .withColumnRenamed('OPERTN_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))

# COMMAND ----------

# MAGIC %md ### 2.2.2. Check

# COMMAND ----------

# check
count_var(oper_out_long, 'PERSON_ID'); print()
count_var(oper_out_long, 'EPIKEY'); print()

tmpt = tab(oper_out_long, 'DIAG_DIGITS'); print()
tmpt = tab(oper_out_long, 'DIAG_POSITION'); print() 
tmpt = tab(oper_out_long, 'CODE'); print() 
# TODO - add valid OPCS-4 code checker...

# COMMAND ----------

display(oper_out_long)

# COMMAND ----------

# MAGIC %md ### 2.2.3. Save

# COMMAND ----------

# save name
outName = f'{proj}_cur_hes_apc_all_years_archive_oper_long'.lower() 

# save
oper_out_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 3. Deaths

# COMMAND ----------

# MAGIC %md ## 3.1. Create

# COMMAND ----------

# check
count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID')
assert dict(deaths.dtypes)['REG_DATE'] == 'string'
assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# define window for the purpose of creating a row number below as per the skinny patient table
_win = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))

# rename ID
# remove records with missing IDs
# reformat dates
# reduce to a single row per individual as per the skinny patient table
# select columns required
# rename column ahead of reshape below
# sort by ID
deaths_out = deaths\
  .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')\
  .where(f.col('PERSON_ID').isNotNull())\
  .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))\
  .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])\
  .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')\
  .orderBy('PERSON_ID')

# check
count_var(deaths_out, 'PERSON_ID')
count_var(deaths_out, 'REG_DATE_OF_DEATH')
count_var(deaths_out, 'S_COD_CODE_UNDERLYING')

# single row deaths 
deaths_out_sing = deaths_out

# remove records with missing DOD
deaths_out = deaths_out\
  .where(f.col('REG_DATE_OF_DEATH').isNotNull())\
  .drop('REG_DATE')

# check
count_var(deaths_out, 'PERSON_ID')

# reshape
# add 1 to diagnosis position to start at 1 (c.f., 0) - will avoid confusion with HES long, which start at 1
# rename 
# remove records with missing cause of death
deaths_out_long = reshape_wide_to_long(deaths_out, i=['PERSON_ID', 'REG_DATE_OF_DEATH'], j='DIAG_POSITION', stubname='S_COD_CODE_')\
  .withColumn('DIAG_POSITION', f.when(f.col('DIAG_POSITION') != 'UNDERLYING', f.concat(f.lit('SECONDARY_'), f.col('DIAG_POSITION'))).otherwise(f.col('DIAG_POSITION')))\
  .withColumnRenamed('S_COD_CODE_', 'CODE4')\
  .where(f.col('CODE4').isNotNull())\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')\
  .withColumn('CODE3', f.substring(f.col('CODE4'), 1, 3))
deaths_out_long = reshape_wide_to_long(deaths_out_long, i=['PERSON_ID', 'DATE', 'DIAG_POSITION'], j='DIAG_DIGITS', stubname='CODE')\
  .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))
  
# check
count_var(deaths_out_long, 'PERSON_ID')  
tmpt = tab(deaths_out_long, 'DIAG_POSITION', 'DIAG_DIGITS', var2_unstyled=1) 
tmpt = tab(deaths_out_long, 'CODE')  
# TODO - add valid ICD-10 code checker...

# COMMAND ----------

# MAGIC %md ## 3.2. Check

# COMMAND ----------

display(deaths_out_sing)

# COMMAND ----------

display(deaths_out_long)

# COMMAND ----------

# MAGIC %md ## 3.3. Save

# COMMAND ----------

# save name
outName = f'{proj}_cur_deaths_{db}_archive_sing'.lower()

# save
deaths_out_sing.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# save name
outName = f'{proj}_cur_deaths_{db}_archive_long'.lower()

# save
deaths_out_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 4. LSOA region lookup

# COMMAND ----------

# MAGIC %md ## 4.1. Create

# COMMAND ----------

spark.sql(f"""
  CREATE or replace global temporary view {proj}_lsoa_region_lookup AS
  with curren_chd_geo_listings as (
    SELECT * 
    FROM {path_ref_geog}
    --WHERE IS_CURRENT = 1
  ),
  lsoa_auth as (
    SELECT e01.geography_code as lsoa_code, e01.geography_name lsoa_name, 
      e02.geography_code as msoa_code, e02.geography_name as msoa_name, 
      e0789.geography_code as authority_code, e0789.geography_name as authority_name,
      e0789.parent_geography_code as authority_parent_geography
    FROM curren_chd_geo_listings e01
    LEFT JOIN curren_chd_geo_listings e02 on e02.geography_code = e01.parent_geography_code
    LEFT JOIN curren_chd_geo_listings e0789 on e0789.geography_code = e02.parent_geography_code
    WHERE e01.geography_code like 'E01%' and e02.geography_code like 'E02%'
  ),
  auth_county as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           e10.geography_code as county_code, e10.geography_name as county_name,
           e10.parent_geography_code as parent_geography
    FROM lsoa_auth
    LEFT JOIN dss_corporate.ons_chd_geo_listings e10 on e10.geography_code = lsoa_auth.authority_parent_geography
    WHERE LEFT(authority_parent_geography,3) = 'E10'
  ),
  auth_met_county as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           NULL as county_code, NULL as county_name,           
           lsoa_auth.authority_parent_geography as region_code
    FROM lsoa_auth
    WHERE LEFT(authority_parent_geography,3) = 'E12'
  ),
  lsoa_region_code as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           auth_county.parent_geography as region_code
    FROM auth_county
    UNION ALL
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           region_code 
    FROM auth_met_county
  ),
  lsoa_region as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           region_code, e12.geography_name as region_name 
    FROM lsoa_region_code
    LEFT JOIN dss_corporate.ons_chd_geo_listings e12 on lsoa_region_code.region_code = e12.geography_code
  )
  SELECT * FROM lsoa_region
""")

# COMMAND ----------

tmp1 = spark.table(f'global_temp.{proj}_lsoa_region_lookup')

# COMMAND ----------

# MAGIC %md ## 4.2. Check

# COMMAND ----------

display(tmp1)

# COMMAND ----------

count_var(tmp1, 'lsoa_code')

# COMMAND ----------

# check duplicates
w1 = Window\
  .partitionBy('lsoa_code')\
  .orderBy('lsoa_name')
w2 = Window\
  .partitionBy('lsoa_code')
tmp2 = tmp1\
  .withColumn('_rownum', f.row_number().over(w1))\
  .withColumn('_rownummax', f.count('lsoa_code').over(w2))\
  .where(f.col('_rownummax') > 1)
display(tmp2)
# duplicates are a result of an authority name change - not relevant for this project

# COMMAND ----------

tmp2 = tmp1\
  .withColumn('_rownum', f.row_number().over(w1))\
  .where(f.col('_rownum') == 1)\
  .select('lsoa_code', 'lsoa_name', 'region_code', 'region_name')\
  .withColumnRenamed('lsoa_code', 'LSOA')\
  .withColumnRenamed('region_name', 'region')

count_var(tmp2, 'LSOA'); print()
tmpt = tab(tmp2, 'region'); print()

# COMMAND ----------

# MAGIC %md ## 4.3. Save 

# COMMAND ----------

# save name
outName = f'{proj}_cur_lsoa_region_lookup'.lower()

# save
tmp2.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 5. LSOA IMD lookup

# COMMAND ----------

# check
print(imd.toPandas().head(5)); print()
count_var(imd, 'LSOA_CODE_2011'); print()
tmpt = tab(imd, 'DECI_IMD', 'IMD_YEAR', var2_unstyled=1); print()

# tidy
tmp1 = imd\
  .where(f.col('IMD_YEAR') == 2019)\
  .select('LSOA_CODE_2011', 'DECI_IMD')\
  .withColumnRenamed('LSOA_CODE_2011', 'LSOA')\
  .withColumn('IMD_2019_QUINTILES',
    f.when(f.col('DECI_IMD').isin([1,2]), 1)\
     .when(f.col('DECI_IMD').isin([3,4]), 2)\
     .when(f.col('DECI_IMD').isin([5,6]), 3)\
     .when(f.col('DECI_IMD').isin([7,8]), 4)\
     .when(f.col('DECI_IMD').isin([9,10]), 5)\
     .otherwise(None)\
  )\
  .withColumnRenamed('DECI_IMD', 'IMD_2019_DECILES')

# check
tmpt = tab(tmp1, 'IMD_2019_DECILES', 'IMD_2019_QUINTILES', var2_unstyled=1); print()
print(tmp1.toPandas().head(5)); print()

# COMMAND ----------

# save name
outName = f'{proj}_cur_lsoa_imd_lookup'.lower()
  
# save
tmp1.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 6. Ethnic desc cat lookup

# COMMAND ----------

print('------------------------------------------------------------------------------')
print('ethnic_hes')
print('------------------------------------------------------------------------------')
# check
print(ethnic_hes.toPandas().head(5)); print()

# prepare
tmp1 = ethnic_hes\
  .select('ETHNICITY_CODE', 'ETHNICITY_DESCRIPTION')\
  .withColumnRenamed('ETHNICITY_CODE', 'ETHNIC')\
  .withColumnRenamed('ETHNICITY_DESCRIPTION', 'ETHNIC_DESC_HES')

# check
count_var(tmp1, 'ETHNIC'); print()
print(tmp1.toPandas().head(5)); print()


print('------------------------------------------------------------------------------')
print('ethnic_gdppr')
print('------------------------------------------------------------------------------')
# check
print(ethnic_gdppr.toPandas().head(5)); print()

# prepare
tmp2 = ethnic_gdppr\
  .select('Value', 'Label')\
  .withColumnRenamed('Value', 'ETHNIC')\
  .withColumnRenamed('Label', 'ETHNIC_DESC_GDPPR')

# check
count_var(tmp2, 'ETHNIC'); print()
print(tmp2.toPandas().head(5)); print()


print('------------------------------------------------------------------------------')
print('merge')
print('------------------------------------------------------------------------------')
# merge
tmp3 = merge(tmp1, tmp2, ['ETHNIC']); print()
tmp3 = tmp3\
  .withColumn('ETHNIC_DESC', f.coalesce(f.col('ETHNIC_DESC_HES'), f.col('ETHNIC_DESC_GDPPR')))\
  .orderBy('ETHNIC')
# .withColumn('ETHNIC_DESC', f.when(f.col('_merge') == 'both', f.col('ETHNIC_DESC_GDPPR')).otherwise(f.col('ETHNIC_DESC')))\
# .withColumn('ETHNIC_DESCx', f.concat(f.col('ETHNIC'), f.lit(' '), f.col('ETHNIC_DESC')))\

# check
# with pd.option_context('expand_frame_repr', False):
print(tmp3.toPandas().to_string()); print()
count_var(tmp3, 'ETHNIC'); print()

# tidy and add ETHNIC_CAT (CCU002_01-D04)
tmp4 = tmp3\
  .select('ETHNIC', 'ETHNIC_DESC')\
  .withColumn('ETHNIC_CAT',\
   f.when(f.col('ETHNIC').isin(['0','A','B','C']), f.lit('White'))\
    .when(f.col('ETHNIC').isin(['1','2','3','N','M','P']), f.lit('Black or Black British'))\
    .when(f.col('ETHNIC').isin(['4','5','6','L','K','J','H']), f.lit('Asian or Asian British'))\
    .when(f.col('ETHNIC').isin(['D','E','F','G']), f.lit('Mixed'))\
    .when(f.col('ETHNIC').isin(['7','8','W','T','S','R']), f.lit('Other'))\
    .when(f.col('ETHNIC').isin(['9','Z','X']), f.lit('Unknown'))\
    .otherwise('Unknown')\
  )

# check
tmpt = tab(tmp4, 'ETHNIC_DESC', 'ETHNIC_CAT', var2_unstyled=1); print()

# COMMAND ----------

# save name
outName = f'{proj}_cur_ethnic_desc_cat_lookup'.lower()
  
# save
tmp4.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 7. ECDS

# COMMAND ----------

# check
count_var(ecds, 'PERSON_ID_DEID'); print()
count_var(ecds, 'ECDSKEY'); print()
count_var(ecds, 'ARRIVAL_DATE'); print()

# select columns (PERSON_ID, RECORD_ID, DATE, Diagnostic and Diagnostic qualifier columns)
# rename PERSON_ID
ecds_1 = (
  ecds  
  .where(f.col('PERSON_ID_DEID').isNotNull())
  .select(
    ['PERSON_ID_DEID', 'ECDSKEY', 'ARRIVAL_DATE'] 
    + [col for col in list(ecds.columns) if re.match(r'^DIAGNOSIS_CODE_\d+$', col)]
    + [col for col in list(ecds.columns) if re.match(r'^DIAGNOSIS_QUALIFIER_\d+$', col)]
  )
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
  .orderBy('PERSON_ID', 'ECDSKEY')
)

# check
count_var(ecds_1, 'PERSON_ID'); print()
count_var(ecds_1, 'ECDSKEY'); print()
count_var(ecds_1, 'ARRIVAL_DATE'); print()
print(len(ecds_1.columns)); print()
print(pd.DataFrame({f'_cols': ecds_1.columns}).to_string()); print()

# check date (any dates that can use to supplement?)
tmpf = (
  ecds_1
  .select('ARRIVAL_DATE')
  .withColumn('flag_ARRIVAL_DATE', 
              f.when(f.col('ARRIVAL_DATE').isNotNull(), 
                     f.when(f.col('ARRIVAL_DATE') == '1800-01-01', -1)
                     .when(f.col('ARRIVAL_DATE') == '1801-01-01', -2)
                     .otherwise(1)
                    )
              .otherwise(0)
             )
)
tmpt = tab(tmpf, 'flag_ARRIVAL_DATE'); print()
tmpt = tabstat(tmpf, 'ARRIVAL_DATE', date=1); print()

# check records with code
tmpf1 = (
  ecds_1
  .withColumn('sum_CODE', sum([f.when(f.col(col).isNotNull(), 1).otherwise(0) for col in list(ecds.columns) if re.match(r'^DIAGNOSIS_CODE_\d+$', col)]))
  .withColumn('flag_CODE', f.when(f.col('sum_CODE') >= 1, 1).otherwise(0))
)
tmpt = tab(tmpf1, 'sum_CODE', 'flag_CODE'); print()

# COMMAND ----------

display(ecds_1.orderBy('PERSON_ID', 'ECDSKEY', 'ARRIVAL_DATE'))

# COMMAND ----------

# MAGIC %md ## 7.1. Diag

# COMMAND ----------

# MAGIC %md ### 7.1.1. Create

# COMMAND ----------

ecds_long_1 = (
  reshape_wide_to_long_multi(ecds_1, i=['PERSON_ID', 'ECDSKEY', 'ARRIVAL_DATE'], j='POSITION', stubnames=['DIAGNOSIS_CODE_', 'DIAGNOSIS_QUALIFIER_'])
  .withColumnRenamed('POSITION', 'DIAGNOSIS_POSITION')
  .withColumnRenamed('DIAGNOSIS_CODE_', 'DIAGNOSIS_CODE')
  .withColumnRenamed('DIAGNOSIS_QUALIFIER_', 'DIAGNOSIS_QUALIFIER')
)

# check
tmpt = tab(ecds_long_1, 'DIAGNOSIS_POSITION'); print()

# reformat
ecds_long_2 = (
  ecds_long_1
  .withColumn('DIAGNOSIS_POSITION', f.col('DIAGNOSIS_POSITION').cast(t.IntegerType()))
)

# check
tmpt = tab(ecds_long_2, 'DIAGNOSIS_POSITION'); print()

# check
count_var(ecds_long_2, 'ECDSKEY'); print()

# filter
ecds_long_3 = (
  ecds_long_2
  .where((f.col('DIAGNOSIS_CODE').isNotNull()) & (f.col('DIAGNOSIS_CODE') != ''))\
)

# check
count_var(ecds_long_3, 'ECDSKEY'); print()

# check
tmpt = tab(ecds_long_3, 'DIAGNOSIS_QUALIFIER'); print()

# check
# print(ecds_long.orderBy('PERSON_ID', 'ECDSKEY', 'ARRIVAL_DATE', 'POSITION').limit(20).toPandas().to_string()); print()

# COMMAND ----------

# check
display(ecds_long_3.orderBy('PERSON_ID', 'ECDSKEY', 'ARRIVAL_DATE', 'DIAGNOSIS_POSITION'))

# COMMAND ----------

# MAGIC %md ### 2.1.2. Check

# COMMAND ----------

tmpt = tab(ecds_long_3, 'DIAGNOSIS_POSITION'); print()

# COMMAND ----------

# # check
# count_var(hes_apc_long, 'PERSON_ID'); print()
# count_var(hes_apc_long, 'EPIKEY'); print()

# # check removal of trailing X
# tmpt = hes_apc_long\
#   .where(f.col('CODE').rlike('X'))\
#   .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
# tmpt = tab(tmpt, 'flag'); print()

# COMMAND ----------

# display(hes_apc_long)

# COMMAND ----------

# MAGIC %md ### 2.1.3. Save

# COMMAND ----------

# save name
outName = f'{proj}_cur_ecds_all_years_archive_long'.lower() 

# save
ecds_long_3.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

sus = spark.table(f'{dbc}.sus_{db}_archive')

# COMMAND ----------

tmpt = tab(sus, 'archived_on')

# COMMAND ----------

gdppr = spark.table(f'{dbc}.gdppr_{db}_archive')

# COMMAND ----------

tmpt = tab(gdppr, 'archived_on')

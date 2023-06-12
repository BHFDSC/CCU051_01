# Databricks notebook source
# MAGIC %md # CCU051_D01_parameters
# MAGIC (with no archive table but rather a frame of datasets and a function to extract data on archived_on date)
# MAGIC 
# MAGIC **Description** This notebook defines a set of parameters to be used in other notebooks D02-D12
# MAGIC 
# MAGIC **Approach** This notebook is loaded when each notebook in the analysis pipeline is run, so helper functions and parameters are consistently available
# MAGIC 
# MAGIC **Author(s)** Genevieve Cezard, Tom Bolton, Alexia Sampri
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2022.11.14 (Based on new approach/code developed by Tom Bolton, from CCU051 with no freezing/CCU002_07)
# MAGIC 
# MAGIC **Date last updated** 2023.03.28
# MAGIC 
# MAGIC **Date last run** 2023.03.28 (/2023.05.16 just to check last batches)
# MAGIC  
# MAGIC **Data input** None - manual parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re

# COMMAND ----------

# MAGIC %md # 1. Define parameters

# COMMAND ----------

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu051'

# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = 'dars_nic_391419_j3w9t'
dbc = f'{db}_collab'

# -----------------------------------------------------------------------------
# Datasets to use
# -----------------------------------------------------------------------------
# data frame of datasets
tmp_archived_on = '2022-12-31'
data = [
    ['deaths',  dbc, f'deaths_{db}_archive',            tmp_archived_on, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',   dbc, f'gdppr_{db}_archive',             tmp_archived_on, 'NHS_NUMBER_DEID',                'DATE']
  , ['hes_apc', dbc, f'hes_apc_all_years_archive',      tmp_archived_on, 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_op',  dbc, f'hes_op_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'APPTDATE'] 
  , ['hes_ae',  dbc, f'hes_ae_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'ARRIVALDATE'] 
  , ['vacc',    dbc, f'vaccine_status_{db}_archive',    tmp_archived_on, 'PERSON_ID_DEID',                 'DATE_AND_TIME']
  , ['sgss',    dbc, f'sgss_{db}_archive',              tmp_archived_on, 'PERSON_ID_DEID',                 'Specimen_Date'] 
  , ['pmeds',   dbc, f'primary_care_meds_{db}_archive', tmp_archived_on, 'Person_ID_DEID',                 'ProcessingPeriodDate']     
  , ['ecds',    dbc, f'lowlat_ecds_all_years_archive',  tmp_archived_on, 'PERSON_ID_DEID',                 'ARRIVAL_DATE']
  , ['hes_cc',  dbc, f'hes_cc_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'CCSTARTDATE'] 
]
parameters_df_datasets = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'archived_on', 'idVar', 'dateVar'])
print('parameters_df_datasets:\n', parameters_df_datasets.to_string())

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# reference tables
path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
path_ref_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
path_ref_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'
path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'
path_ref_opcs4           = 'dss_corporate.opcs_codes_v02'

# curated tables
path_cur_hes_apc_long      = f'{dbc}.{proj}_cur_hes_apc_all_years_archive_long'
path_cur_hes_apc_oper_long = f'{dbc}.{proj}_cur_hes_apc_all_years_archive_oper_long'
path_cur_deaths_long       = f'{dbc}.{proj}_cur_deaths_{db}_archive_long'
path_cur_deaths_sing       = f'{dbc}.{proj}_cur_deaths_{db}_archive_sing'
path_cur_lsoa_region       = f'{dbc}.{proj}_cur_lsoa_region_lookup'
path_cur_lsoa_imd          = f'{dbc}.{proj}_cur_lsoa_imd_lookup'
path_cur_lsoa_ruc          = f'{dbc}.{proj}_cur_lsoa_ruc_lookup'
path_cur_ethnic_desc_cat   = f'{dbc}.{proj}_cur_ethnic_desc_cat_lookup'
path_cur_covid             = f'{dbc}.{proj}_cur_covid'

# out tables
path_out_codelist_covid               = f'{dbc}.{proj}_out_codelist_covid'
path_out_codelist_qcovid              = f'{dbc}.{proj}_out_codelist_qcovid'
path_out_codelist_quality_assurance   = f'{dbc}.{proj}_out_codelist_quality_assurance'
path_out_PCR_test                     = f'{dbc}.{proj}_out_PCR_test'

# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------
proj_start_date = '2022-06-01'
proj_end_date = '2022-09-30'


# COMMAND ----------

# Check latest archived dates for each dataset of interest - checked on 2023-05-16 but 2022-12-31 used for CCU051 project

#tmp = spark.table(f'{dbc}.vaccine_status_{db}_archive')
#tmpt = tab(tmp, 'archived_on')
# -> Vaccine status dataset latest update : 2023-03-31 but using 2022-12-31

#tmp = spark.table(f'{dbc}.deaths_{db}_archive')
#tmpt = tab(tmp, 'archived_on')
# -> Death dataset latest update : 2023-03-31/2023-04-27 but using 2022-12-31

#tmp = spark.table(f'{dbc}.gdppr_{db}_archive')
#tmpt = tab(tmp, 'archived_on')
# -> GDPPR dataset latest update : 2023-03-31/2023-04-27 but using 2022-12-31

#tmp = spark.table(f'{dbc}.hes_apc_all_years_archive')
#tmpt = tab(tmp, 'archived_on')
# -> HES APC dataset latest update : 2023-03-31/2023-04-27 but using 2022-12-31

#tmp = spark.table(f'{dbc}.hes_op_all_years_archive')
#tmpt = tab(tmp, 'archived_on')
# -> HES Outpatient dataset latest update : 2023-03-31/2023-04-27 but using 2022-12-31

#tmp = spark.table(f'{dbc}.hes_ae_all_years_archive')
#tmpt = tab(tmp, 'archived_on')
# -> HES A&E dataset latest update : 2023-03-31 but using 2022-12-31

#tmp = spark.table(f'{dbc}.sgss_{db}_archive')
#tmpt = tab(tmp, 'archived_on')
# -> SGSS dataset latest update : 2023-02-28 but using 2022-12-31

# COMMAND ----------

# MAGIC %md # 2. Function to get data on archived_on date

# COMMAND ----------

# function to extract the batch corresponding to the pre-defined archived_on date from the archive for the specified dataset
from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']  
  print(_path + ' (archived_on = ' + _archived_on + ')')
  
  # check path exists # commented out for runtime
#   _tmp_exists = spark.sql(f"SHOW TABLES FROM {_row['database']}")\
#     .where(f.col('tableName') == _row['table'])\
#     .count()
#   assert _tmp_exists == 1, f"path = {_path} not found"

  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _tmp_records = _tmp.count()
  print(f'  {_tmp_records:,} records')
  assert _tmp_records > 0, f"number of records == 0"

  # return dataframe
  return _tmp

# COMMAND ----------

# MAGIC %md # 3. Print defined parameters

# COMMAND ----------

print(f'Project:')
print("  {0:<22}".format('proj') + " = " + f'{proj}') 
print(f'')
print(f'Databases:')
print("  {0:<22}".format('db') + " = " + f'{db}') 
print("  {0:<22}".format('dbc') + " = " + f'{dbc}') 
print(f'')
print(f'Datasets:')
print(f'')
print(f'  parameters_df_datasets')
print(parameters_df_datasets[['dataset', 'database', 'table', 'archived_on']].to_string())
print(f'')
print(f'Paths:')
print(f'')
tmp = vars().copy()
for var in list(tmp.keys()):
  if(re.match('^path_.*$', var)):
    print("  {0:<22}".format(var) + " = " + tmp[var])    
print(f'')
print(f'Dates:')    
print("  {0:<22}".format('proj_start_date') + " = " + f'{proj_start_date}')
print(f'')

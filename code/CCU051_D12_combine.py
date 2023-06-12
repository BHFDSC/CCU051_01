# Databricks notebook source
# MAGIC %md # CCU051_D12_combine
# MAGIC 
# MAGIC **Description** This notebook combine all the data needed for analysis for CCU051 (based on initial copy from CCU002_07).
# MAGIC 
# MAGIC **Author(s)** Genevieve Cezard, Alexia Sampri
# MAGIC 
# MAGIC **Project** CCU051
# MAGIC 
# MAGIC **First copied over** 2022.12.06 (from CCU002_07)
# MAGIC 
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC 
# MAGIC **Date last updated** 2023.03.31
# MAGIC 
# MAGIC **Date last run** 2023.03.31
# MAGIC 
# MAGIC **Data input** The following datasets: \
# MAGIC 1- cohort (CCU051_out_cohort)\
# MAGIC 2- Weights (CCU051_out_weights)\
# MAGIC 3- Vaccination (CCU051_out_vaccination)\
# MAGIC 4- Covariates (CCU051_out_covariates)\
# MAGIC 5- Outcomes (CCU051_out_outcomes)
# MAGIC 
# MAGIC **Data output** - This code creates a dataset for analysis, called 'ccu051_out_analysis' + 'ccu051_out_Qcovid' as a separate dataset

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

# Cohort
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

# Weights
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_weights')
weights = spark.table(f'{dbc}.{proj}_out_weights')

# Covariates
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_covariates')
covariates = spark.table(f'{dbc}.{proj}_out_covariates')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_covariates_supp')
covariates_supp = spark.table(f'{dbc}.{proj}_out_covariates_supp')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_PCR_test')
covariates_PCR_test = spark.table(f'{dbc}.{proj}_out_PCR_test')

# Vaccination
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_vaccination')
vaccination = spark.table(f'{dbc}.{proj}_out_vaccination')

# Outcomes
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_outcomes')
outcomes = spark.table(f'{dbc}.{proj}_out_outcomes')

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

# MAGIC %md ## 2.1. Prepare Cohort

# COMMAND ----------

# rename columns to match Scotland code for the COALESCE project
cohort1 = cohort\
  .withColumnRenamed('ETHNIC_CAT','ethnicity_5cat')\
  .withColumnRenamed('RUC11_bin','urban_rural_2cat')\
  .withColumnRenamed('AGE','age')\
  .withColumnRenamed('SEX','sex')\
  .withColumnRenamed('DOD','death_date')
  
cohort1_selection = cohort1.select('PERSON_ID', 'sex','age','ethnicity_5cat','imd_2019_quintiles','urban_rural_2cat','region','death_date')


# COMMAND ----------

display(cohort1)

# COMMAND ----------

display(cohort1_selection)

# COMMAND ----------

# MAGIC %md ## 2.2. Prepare Vaccination

# COMMAND ----------

# vaccination variables have been renamed already in D10 notebook to match Scotland code for the COALESCE project

vaccination1_selection = vaccination.select('PERSON_ID', 'date_vacc_1','date_vacc_2','date_vacc_3','date_vacc_4','date_vacc_5','date_vacc_6','num_doses_recent')


# COMMAND ----------

display(vaccination)

# COMMAND ----------

display(vaccination1_selection)

# COMMAND ----------

# MAGIC %md ## 2.3. Prepare Outcome

# COMMAND ----------

# rename columns to match Scotland code for the COALESCE project
outcomes1 = outcomes\
  .withColumnRenamed('out_covid_hes_apc_any_date','covid_acoa_hosp_date')\
  .withColumnRenamed('out_covid_hes_apc_pri_date','covid_mcoa_hosp_date')\
  .withColumnRenamed('out_covid_hes_apc_emergency_pri_date','covid_mcoa_em_hosp_date')\
  .withColumnRenamed('out_covid_hes_apc_any_flag','covid_acoa_hosp')\
  .withColumnRenamed('out_covid_hes_apc_pri_flag','covid_mcoa_hosp')\
  .withColumnRenamed('out_covid_hes_apc_emergency_pri_flag','covid_mcoa_em_hosp')\
  .withColumnRenamed('out_covid_deaths_any_date','covid_ac_death_date')\
  .withColumnRenamed('out_covid_deaths_pri_date','covid_mc_death_date')\
  .withColumnRenamed('out_covid_deaths_any_flag','covid_ac_death')\
  .withColumnRenamed('out_covid_deaths_pri_flag','covid_mc_death')

outcomes1_selection = outcomes1.select('PERSON_ID', 'covid_acoa_hosp_date','covid_mcoa_hosp_date','covid_mcoa_em_hosp_date', 'covid_acoa_hosp', 'covid_mcoa_hosp','covid_mcoa_em_hosp', 'covid_ac_death_date','covid_mc_death_date','covid_ac_death','covid_mc_death')

# COMMAND ----------

display(outcomes1)

# COMMAND ----------

display(outcomes1_selection)

# COMMAND ----------

# MAGIC %md ## 2.4. Prepare Covariates

# COMMAND ----------

#display(covariates)

# COMMAND ----------

# Renaming 34 QCovid items. cov_hx_qcovid_solidorgantransplant_flag cov_hx_qcovid_takingantieukotrien_flag
covariates1 = covariates\
  .withColumnRenamed('cov_hx_qcovid_atrialfibrillation_flag','Q_DIAG_AF')\
  .withColumnRenamed('cov_hx_qcovid_asthma_flag','Q_DIAG_ASTHMA')\
  .withColumnRenamed('cov_hx_qcovid_cancerofbloodorbonem_flag','Q_DIAG_BLOOD_CANCER')\
  .withColumnRenamed('cov_hx_qcovid_cysticfibrosisbronch_flag','Q_DIAG_PULM_RARE')\
  .withColumnRenamed('cov_hx_qcovid_cerebralpalsy_flag','Q_DIAG_CEREBRALPALSY')\
  .withColumnRenamed('cov_hx_qcovid_coronaryheartdisease_flag','Q_DIAG_CHD')\
  .withColumnRenamed('cov_hx_qcovid_livercirrhosis_flag','Q_DIAG_CIRRHOSIS')\
  .withColumnRenamed('cov_hx_qcovid_congenitalheartprobl_flag','Q_DIAG_CONGEN_HD')\
  .withColumnRenamed('cov_hx_qcovid_copd_flag','Q_DIAG_COPD')\
  .withColumnRenamed('cov_hx_qcovid_dementia_flag','Q_DIAG_DEMENTIA')\
  .withColumnRenamed('cov_hx_qcovid_epilepsy_flag','Q_DIAG_EPILEPSY')\
  .withColumnRenamed('cov_hx_qcovid_learningdisabilityor_flag','Q_DIAG_LEARNDIS')\
  .withColumnRenamed('cov_hx_qcovid_priorfractureofhipwr_flag','Q_DIAG_FRACTURE')\
  .withColumnRenamed('cov_hx_qcovid_hivoraids_flag','Q_DIAG_HIV_AIDS')\
  .withColumnRenamed('cov_hx_qcovid_severecombinedimmuno_flag','Q_DIAG_IMMU')\
  .withColumnRenamed('cov_hx_qcovid_motorneuronediseaseo_flag','Q_DIAG_NEURO')\
  .withColumnRenamed('cov_hx_qcovid_parkinsonsdisease_flag','Q_DIAG_PARKINSONS')\
  .withColumnRenamed('cov_hx_qcovid_pulmonaryhypertensio_flag','Q_DIAG_PULM_HYPER')\
  .withColumnRenamed('cov_hx_qcovid_peripheralvasculardi_flag','Q_DIAG_PVD')\
  .withColumnRenamed('cov_hx_qcovid_rheumatoidarthritiso_flag','Q_DIAG_RA_SLE')\
  .withColumnRenamed('cov_hx_qcovid_lungororalcancer_flag','Q_DIAG_RESP_CANCER')\
  .withColumnRenamed('cov_hx_qcovid_bipolardiseaseorschi_flag','Q_DIAG_SEV_MENT_ILL')\
  .withColumnRenamed('cov_hx_qcovid_sicklecellviabool_flag','Q_DIAG_SICKLE_CELL')\
  .withColumnRenamed('cov_hx_qcovid_strokeortia_flag','Q_DIAG_STROKE')\
  .withColumnRenamed('cov_hx_qcovid_thrombosisorpulmonar_flag','Q_DIAG_VTE')\
  .withColumnRenamed('cov_hx_qcovid_diabetes_flag','Q_DIAG_DIABETES')\
  .withColumnRenamed('cov_hx_qcovid_chronickidneydisease_flag','Q_DIAG_CKD')\
  .withColumnRenamed('cov_hx_qcovid_inflammatoryboweldis_flag','Q_DIAG_IBD')\
  .withColumnRenamed('cov_hx_qcovid_heartfailure_flag','Q_DIAG_CCF')\
  .withColumnRenamed('cov_hx_qcovid_bonemarrowtransplant_flag','Q_MARROW')\
  .withColumnRenamed('cov_hx_qcovid_radiotherapyinlast6m_flag','Q_RADIO')\
  .withColumnRenamed('cov_hx_qcovid_solidorgantransplant_flag','Q_SOLIDTRANSPLANT')\
  .withColumnRenamed('cov_hx_qcovid_prescribedoralsteroi_flag','Q_PRESC_STERIOD')\
  .withColumnRenamed('cov_hx_qcovid_prescribedimmunosupp_flag','Q_PRESC_IMMUNOSUPP')\
  .withColumnRenamed('cov_hx_qcovid_takingantileukotrien_flag','Q_LEUKO')\
  .withColumnRenamed('cov_bmi_value','Q_BMI')\
  .withColumnRenamed('cov_shielded','shielding')
  
# From Steven's R code, the following variables are listed:
# $Q_DIAG_AF $Q_DIAG_ASTHMA $Q_DIAG_BLOOD_CANCER $Q_DIAG_CCF $Q_DIAG_CEREBRALPALSY $Q_DIAG_CHD $Q_DIAG_CIRRHOSIS $Q_DIAG_CONGEN_HD $Q_DIAG_COPD $Q_DIAG_DEMENTIA $Q_DIAG_EPILEPSY $Q_DIAG_FRACTURE
# $Q_DIAG_HIV_AIDS $Q_DIAG_IMMU $Q_DIAG_NEURO $Q_DIAG_PARKINSONS $Q_DIAG_PULM_HYPER $Q_DIAG_PULM_RARE $Q_DIAG_PVD $Q_DIAG_RA_SLE $Q_DIAG_RESP_CANCER $Q_DIAG_SEV_MENT_ILL $Q_DIAG_SICKLE_CELL
# $Q_DIAG_STROKE $Q_DIAG_VTE $Q_DIAG_DIABETES_1 $Q_DIAG_DIABETES_2
# Not used: 'Q_DIAG_PULM_RARE'
# QCovid items not included in covariates are: $Q_ETHNICITY - $Q_HOME_CAT - $Q_LEARN_CAT (using Q_DIAG_LEARNDIS instead) - $Q_DIAG_CKD_LEVEL (using Q_DIAG_CKD instead)
# Note : count of qcovid clinical risk groups. This does not include BMI, ethnicity or housing category.

covariates1_selection = covariates1.select('PERSON_ID','Q_BMI','shielding')

covariates2_selection = covariates1.select('PERSON_ID','Q_DIAG_AF','Q_DIAG_ASTHMA','Q_DIAG_BLOOD_CANCER','Q_DIAG_PULM_RARE','Q_DIAG_CEREBRALPALSY','Q_DIAG_CHD','Q_DIAG_CIRRHOSIS','Q_DIAG_CONGEN_HD','Q_DIAG_COPD','Q_DIAG_DEMENTIA','Q_DIAG_EPILEPSY','Q_DIAG_LEARNDIS','Q_DIAG_FRACTURE','Q_DIAG_HIV_AIDS','Q_DIAG_IMMU','Q_DIAG_NEURO','Q_DIAG_PARKINSONS','Q_DIAG_PULM_HYPER','Q_DIAG_PVD','Q_DIAG_RA_SLE','Q_DIAG_RESP_CANCER','Q_DIAG_SEV_MENT_ILL','Q_DIAG_SICKLE_CELL','Q_DIAG_STROKE','Q_DIAG_VTE','Q_DIAG_DIABETES','Q_DIAG_CKD','Q_DIAG_IBD','Q_DIAG_CCF','Q_MARROW','Q_RADIO','Q_SOLIDTRANSPLANT','Q_PRESC_STERIOD','Q_PRESC_IMMUNOSUPP','Q_LEUKO')

# COMMAND ----------

display(covariates2_selection)

# COMMAND ----------

display(covariates1_selection)

# COMMAND ----------

#display(covariates_supp)

# COMMAND ----------

covariates3_selection = covariates_supp.select('PERSON_ID','noncovid_hosp_date')

# COMMAND ----------

display(covariates3_selection)

# COMMAND ----------

display(covariates_PCR_test)

# COMMAND ----------

covariates4_selection = covariates_PCR_test.select('PERSON_ID','PCR_last_date')

# COMMAND ----------

display(covariates4_selection)

# COMMAND ----------

# MAGIC %md # 3. Create

# COMMAND ----------

# MAGIC %md ## 3.1. Create analysis dataset

# COMMAND ----------

m1 = merge(cohort1_selection, weights, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()
m2 = merge(m1, vaccination1_selection, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()
m3 = merge(m2, outcomes1_selection, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()
m4 = merge(m3, covariates1_selection, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()
m5 = merge(m4, covariates3_selection, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()
m6 = merge(m5, covariates4_selection, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()

analysis = m6\
  .withColumn('CHUNK', f.floor(f.rand(seed=1234) * 10) + f.lit(1))

# COMMAND ----------

tmpt = tab(analysis, 'num_doses_recent'); print()
analysis = analysis\
  .fillna(0, subset=['num_doses_recent'])
tmpt = tab(analysis, 'num_doses_recent'); print()

# COMMAND ----------

tmpt = tab(analysis, 'age','num_doses_recent'); print()

# COMMAND ----------

# MAGIC %md ## 3.2. Create Qcovid dataset

# COMMAND ----------

QCovid = covariates2_selection\
  .withColumn('CHUNK', f.floor(f.rand(seed=1234) * 10) + f.lit(1))

# COMMAND ----------

display(QCovid)

# COMMAND ----------

# MAGIC %md # 4. Check

# COMMAND ----------

# check
count_var(analysis, 'PERSON_ID'); print()
print(len(analysis.columns)); print()
print(pd.DataFrame({f'_cols': analysis.columns}).to_string()); print()

# COMMAND ----------

display(analysis)

# COMMAND ----------

# MAGIC %md # 5. Save

# COMMAND ----------

# MAGIC %md ## 5.1. Save analysis dataset

# COMMAND ----------

# save name
outName = f'{proj}_out_analysis'.lower()

# save
analysis.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 5.2. Save Qcovid dataset

# COMMAND ----------

#QCovid items are saved in a separate dataset as the whole dataset cannot be transfered into R

# COMMAND ----------

# save name
outName_QCovid = f'{proj}_out_QCovid'.lower()

# save
QCovid.write.mode('overwrite').saveAsTable(f'{dbc}.{outName_QCovid}')
spark.sql(f'ALTER TABLE {dbc}.{outName_QCovid} OWNER TO {dbc}')

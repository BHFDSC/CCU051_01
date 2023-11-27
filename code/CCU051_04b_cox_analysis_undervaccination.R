#################################################################################
## TITLE: Longitudinal analysis for England (dataset (CCU051/COALESCE project) 
##
## Description: Analysis of severe COVID-19 events
##
## Revisited version to include Main and Extended analyses
## Partly based on code developed by Steven Kerr for output files
##
## Created clean version on: 2023-05-05
## Updated on: 2023-05-08 (DAE)
## Updated on: 2023-07-03 (SDE)
##
## Created and updated by : Genevieve Cezard
## 
######################################################################
library(tidyverse)
library(survival)
library(broom)

output_dir = "./output/"

setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU051")
#setwd("C:/ProgramData/UserDataFolders/S-1-5-21-2953574002-3863863878-259671515-1012/My Files/Home Folder/collab/CCU051")
#setwd("/mnt/efs/gic30/dars_nic_391419_j3w9t_collab/CCU051")
#setwd("/mnt/efs/as3293/dars_nic_391419_j3w9t_collab/CCU051")

# Read in
df_cohort = readRDS('./data/df_cohort.rds')

study_start = as.Date("2022-06-01")
study_end = as.Date("2022-09-30")

# Add more entries to names_map to automatically generate display names
var_names <- c(
  "Total N (%)",
  "sex",
  "age",
  "age_3cat",
  "imd_2019_quintiles",
  "urban_rural_2cat",
  "ethnicity_5cat",
  "qc_count_cat",
  "qc_count_3cat"
)
# All display names for the table
display_names <- c(
  "Total",
  "Sex",
  "Age",
  "Age group",
  "IMD quintile",
  "Urban/rural classification",
  "Ethnicity",
  "Number of QCovid risk groups",
  "Number of QCovid risk groups"
)
# This will be used for changing variables names to display names
names_map <- setNames(display_names, var_names)
names_map["shielding"] = "Ever shielding"
names_map["age_17cat"] = "Age group"
names_map["bmi_cat"] = "BMI"
names_map["region"] = "Region"
names_map["last_positive_test_group"] = "Last positive test"


Get_Cox_sample = function(df_cohort,dep_var) {
  # Total population
  N_cohort <- nrow(df_cohort)
  # Select case
  select_case <- df_cohort[df_cohort[[dep_var]] == "1",]
  N_case <- nrow(select_case)
  print(paste0("N_case: ",N_case))  
  # Select 20 controls per case
  all_control <- df_cohort[df_cohort[[dep_var]] == "0",]
  select_control <-df_cohort[sample(nrow(all_control),size=N_case*40),]
  N_control <- nrow(select_control)
  print(paste0("N_control: ",N_control)) 
  # Combine
  Cox_sample <- rbind(select_case,select_control)
  N_sample <- nrow(Cox_sample)
  print(paste0("N_sample: ", N_sample))  
  # Calculate weights
  Cox_sample$weight <- ifelse(Cox_sample[[dep_var]] == "1",1,(N_cohort-N_case)/N_control)
  table(Cox_sample$weight)
  
  return(Cox_sample)
}

Output_results = function(analysis_type, dep_var, age_group) {
  #create directory
  dir = paste0(output_dir, analysis_type, "_", dep_var, "_", age_group)
  if (!dir.exists(dir)) {
    dir.create(dir)
  }
  # Coefficients
  coef = data.frame("variable" = names(coef(model)), "coef" = coef(model), row.names = 1:length(coef(model)))
  write.csv(coef, paste0(dir, "/coef.csv"), row.names = FALSE)
  
  # Covariance matrix
  cov = vcov(model)
  write.csv(cov, paste0(dir, "/cov.csv"))
  
  # Results table
  cox_results = tidy(model) %>%
    filter(!grepl("calendar", term)) %>%
    mutate(
      ucl = estimate + 1.96 * std.error,
      lcl = estimate - 1.96 * std.error
    ) %>%
    select(term, estimate, lcl, ucl) %>%
    mutate_if(is.numeric, ~ formatC(round(exp(.), 2), format = "f", big.mark = ",", drop0trailing = TRUE)) %>%
    mutate(estimate = paste0(estimate, " (", lcl, ", ", ucl, ")")) %>%
    select(-lcl, -ucl) %>%
    rename(HR = estimate) %>%
    data.frame()
  write.csv(cox_results, paste0(dir, "/results.csv"), row.names = FALSE)
}


#######################################
## Select variables of interest only ##
#######################################
# Select variables of interest only
df_cohort_reduced <- df_cohort %>% dplyr::select('under_vaccinated','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_cat', 'qc_count_3cat','bmi_cat','bmi_cat_kids','shielding','region','last_positive_test_group','death_date','age_3cat','covid_mcoa_em_hosp_death','covid_mcoa_em_hosp_death_date')
df_cohort_reduced$region = fct_relevel(df_cohort_reduced$region, "London")
df_cohort_reduced$bmi_cat = fct_relevel(df_cohort_reduced$bmi_cat, "25-30")

# Calculation on full cohort 
df_cohort_reduced$surv_date <- as.Date("2022-09-30")
df_cohort_reduced$surv_date <- pmin(df_cohort_reduced$surv_date,df_cohort_reduced$covid_mcoa_em_hosp_death_date,df_cohort_reduced$death_date, na.rm = TRUE)
df_cohort_reduced$tstart <- as.Date("2022-06-01")
df_cohort_reduced <- df_cohort_reduced %>% mutate(surv_time = as.numeric(surv_date - tstart))


########################
## Cox for 5-15 y old ##
########################
# Filter by age group
df_cohort_5_15 <- subset(df_cohort_reduced, age_3cat == "5-15")

# SAMPLING for COX regression in England
Cox_sample <- Get_Cox_sample(df_cohort_5_15,"covid_mcoa_em_hosp_death")

#---------------------------------#
# COX MODEL - unadjusted analysis #
#---------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated, data = Cox_sample, weights = weight)
Output_results(analysis_type="Unadj_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "5-15")

#---------------------------#
# COX MODEL - main analysis #
#---------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_3cat, data = Cox_sample, weights = weight)
Output_results(analysis_type="cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "5-15")

#-------------------------------#
# COX MODEL - extended analysis #
#-------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_3cat + shielding + region + last_positive_test_group, data = Cox_sample, weights = weight)
# bmi_cat and bmi_cat_kids doesn't work. 
Output_results(analysis_type="Ext_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "5-15")


#########################
## Cox for 16-74 y old ##
#########################
# Select dataset and filter by age group
df_cohort_16_74 <- subset(df_cohort_reduced, age_3cat == "16-74")

# SAMPLING for COX regression in England
Cox_sample <- Get_Cox_sample(df_cohort_16_74,"covid_mcoa_em_hosp_death")

# Relevel for 16-64 models
Cox_sample$age_17cat = fct_relevel(Cox_sample$age_17cat, "18-24")

#---------------------------------#
# COX MODEL - unadjusted analysis #
#---------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated, data = Cox_sample, weights = weight)
Output_results(analysis_type="Unadj_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "16-74")

#---------------------------#
# COX MODEL - main analysis #
#---------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat, data = Cox_sample, weights = weight)
Output_results(analysis_type="cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "16-74")

#-------------------------------#
# COX MODEL - extended analysis #
#-------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat + bmi_cat + shielding + region + last_positive_test_group, data = Cox_sample, weights = weight)
Output_results(analysis_type="Ext_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "16-74")

#-------------------------------------------#
# COX MODEL - extended analysis without BMI #
#-------------------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat + shielding + region + last_positive_test_group, data = Cox_sample, weights = weight)
Output_results(analysis_type="Ext_noBMI_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "16-74")


########################
## Cox for 75 + y old ##
########################
# Select dataset and filter by age group
df_cohort_75plus <- subset(df_cohort_reduced, age_3cat == "75+")

# SAMPLING for COX regression in England
Cox_sample <- Get_Cox_sample(df_cohort_75plus,"covid_mcoa_em_hosp_death")

# Relevel for 75+ models
Cox_sample$age_17cat = fct_relevel(Cox_sample$age_17cat, "75-79")

#---------------------------------#
# COX MODEL - unadjusted analysis #
#---------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated, data = Cox_sample, weights = weight)
Output_results(analysis_type="Unadj_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "75plus")

#---------------------------#
# COX MODEL - main analysis #
#---------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat, data = Cox_sample, weights = weight)
Output_results(analysis_type="cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "75plus")

#-------------------------------#
# COX MODEL - extended analysis #
#-------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat + bmi_cat + shielding + region + last_positive_test_group, data = Cox_sample, weights = weight)
Output_results(analysis_type="Ext_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "75plus")

#-------------------------------------------#
# COX MODEL - extended analysis without BMI #
#-------------------------------------------#
model = coxph(Surv(surv_time,covid_mcoa_em_hosp_death) ~ under_vaccinated + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat + shielding + region + last_positive_test_group, data = Cox_sample, weights = weight)
Output_results(analysis_type="Ext_noBMI_cox",dep_var = "covid_mcoa_em_hosp_death",age_group = "75plus")


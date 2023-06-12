#################################################################################
## TITLE: Cross-sectional analysis for England (dataset (CCU051/COALESCE project) 
##
## Description: Analysis of sub-optimal vaccination - using aggregated data
##
## Partly based on code developed by Steven Kerr to create output files
## And slides from Chris Robertson on logistic regression on aggregated data
##
## Created on: 2023-03-17
## Last updated on: 2023-05-10
##
## Created and updated by : Genevieve Cezard
## 
######################################################################
library(tidyverse)

setwd("/mnt/efs/gic30/dars_nic_391419_j3w9t_collab/CCU051")
#setwd("/mnt/efs/as3293/dars_nic_391419_j3w9t_collab/CCU051")

output_dir = "./output/"

# Read in
df_cohort = readRDS('./data/df_cohort.rds')

study_start = as.Date("2022-06-01")
study_end = as.Date("2022-09-30")

# Add more entries to names_map to automatically generate display names
var_names <- c(
  "Total N (%)",
  "sex",
  "age",
  "age_4cat",
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


##------------------------------------------------------------------------------------------------##
## 1- Functions to fit logistic model and saves results table, coefficients and covariance matrix ##
##------------------------------------------------------------------------------------------------##
# Logistic regression on aggregated data
lr_analysis_agg = function(df_cohort_agg, age_group, dep_var, ind_vars) {

  ## Define path for output files
  dir = paste0(output_dir, "lr_undervaccination_", age_group)
  if (!dir.exists(dir)) {
    dir.create(dir)
  }

  # Formula and model on aggregated data  
  formula = as.formula(paste(c(paste0("cbind(",lr_dep_var, ",denom -",lr_dep_var,") ~ "), ind_vars), collapse = " + "))
  model = glm(formula, data = df_cohort_agg, family = "binomial")
  
  # Coefficients
  coef = data.frame("variable" = names(coef(model)), "coef" = coef(model), row.names = 1:length(coef(model)))
  write.csv(coef, paste0(dir, "/coef.csv"), row.names = FALSE)
  
  # Covariance matrix
  cov = vcov(model)
  write.csv(cov, paste0(dir, "/cov.csv"))
  
  # Results table
  write.csv(create_logistic_results_table(df_cohort_agg, model, dep_var, ind_vars), paste0(dir, "/results.csv"), row.names = FALSE)
}


# Format results in output tables
create_logistic_results_table = function(df_cohort, model, dep_var, ind_vars) {

  df_vars = data.frame()
  
  for (var in ind_vars) {

    Levels = as.character(levels( as.factor(pull(df_cohort, !!sym(var)))))
    
    new_df = data.frame(Variable = var, Levels) %>%
      mutate(
        OR = case_when( Levels == as.character(levels( as.factor(pull(df_cohort, !!sym(var))))[1])  ~ 'Ref',
                        TRUE ~ NA_character_) ) 
    
    new_df = df_cohort %>%
      group_by(!!sym(var)) %>%
      # Level 0 gets mapped to 1, and 1 gets mapped to 2 by as.numeric
      summarise(undervax = 1 - mean(as.numeric(df_cohort$under_vaccinated)-1)) %>%
      mutate('Under-vaccinated' = round(100 * undervax, 1) ) %>%
      mutate('Under-vaccinated' = paste0('Under-vaccinated', '%')) %>%
      rename(Levels = !!sym(var)) %>%
      right_join(new_df, by = 'Levels')
    
    df_vars = bind_rows(df_vars, new_df)
  }
  
  df_vars = mutate(
    df_vars, 
    term = paste0(Variable, Levels),
    Variable = as.character(Variable)) %>%
    filter(!is.na(Levels))
  
  # Get ORs & 95% CIs
  results = round(exp(cbind("OR" = coef(model), confint.default(model, level = 0.95), digits = 2)), digits = 2) %>%
    as.data.frame() %>%
    dplyr::select(-digits)
  
  names(results) = c("OR", "lower", "upper")
  
  results = mutate(results, term = rownames(results))
  
  results = mutate(results, OR = paste0(OR, " (", lower, ", ", upper, ")")) %>%
    dplyr::select(term, OR)
  
  results = left_join(df_vars, results, by = 'term') %>%
    mutate(
      OR = coalesce(OR.x, OR.y),
      Variable = names_map[Variable] ,
      Variable = case_when(
        !duplicated(Variable) ~ Variable,
        TRUE ~ ""
      )
    ) %>%
    dplyr::select(-term, -OR.x, -OR.y) %>%
    dplyr::select(Variable, Levels, 'Under-vaccinated', OR) %>%
    rename("Odds ratio" = OR)
}

##--------------------##
## 2. Prepare dataset ##
##--------------------##
# Select variables of interest only
df_cohort_reduced <- df_cohort %>% dplyr::select('under_vaccinated','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_cat','qc_count_3cat','death_date','age_4cat','bmi_cat','shielding','region','last_positive_test_group')
df_cohort_reduced$region = fct_relevel(df_cohort_reduced$region, "London")
df_cohort_reduced$bmi_cat = fct_relevel(df_cohort_reduced$bmi_cat, "25-30")

# Add denominator - for aggregation
df_cohort_reduced$denom <- 1

# Transform "under_vaccinated" into a numeric 0/1 variable - for aggregation
#class(df_cohort_reduced$under_vaccinated)
df_cohort_reduced$under_vaccinated <- as.numeric(as.character(df_cohort_reduced$under_vaccinated))


##---------------------------------------------##
## 3. Logistic regression models - get results ##
##---------------------------------------------##
# Define outcome for the under-vaccination analysis
lr_dep_var = "under_vaccinated"

###########################################
# Model using aggregated dataset for 5-11 #
###########################################
# Select data for age group "5-11"
df_cohort_reduced_5_11 = filter(df_cohort_reduced, age_4cat == "5-11", is.na(death_date) | death_date > study_start) %>% droplevels()

# Run model with the 6 core variables (main analysis)
df_agg <- aggregate(cbind(denom,under_vaccinated)~sex+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_3cat+age_4cat, data=df_cohort_reduced_5_11, FUN=sum)
lr_ind_vars = c("sex", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_3cat")
lr_analysis_agg(df_agg, "5-11", lr_dep_var, lr_ind_vars)

# Run model adding on extra variables (extended analysis)
#df_agg_ext <- aggregate(cbind(denom,under_vaccinated)~sex+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_3cat+age_4cat+bmi_cat+shielding+region+last_positive_test_group, data=df_cohort_reduced_5_11, FUN=sum)
#lr_ind_vars_ext = c("sex", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_3cat","bmi_cat","shielding","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext, "5-11", lr_dep_var, lr_ind_vars_ext)

# Run model adding on extra variables (extended analysis - removing BMI and shielding)
#df_agg_ext2 <- aggregate(cbind(denom,under_vaccinated)~sex+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_3cat+age_4cat+region+last_positive_test_group, data=df_cohort_reduced_5_11, FUN=sum)
#lr_ind_vars_ext2 = c("sex", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_3cat","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext2, "5-11", lr_dep_var, lr_ind_vars_ext2)

############################################
# Model using aggregated dataset for 12-15 #
############################################
# Select data for age group "12-15"
df_cohort_reduced_12_15 = filter(df_cohort_reduced, age_4cat == "12-15", is.na(death_date) | death_date > study_start) %>% droplevels()

# Run model with the 6 core variables (main analysis)
df_agg <- aggregate(cbind(denom,under_vaccinated)~sex+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_3cat+age_4cat, data=df_cohort_reduced_12_15, FUN=sum)
lr_ind_vars = c("sex", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_3cat")
lr_analysis_agg(df_agg, "12-15", lr_dep_var, lr_ind_vars)

# Run model adding on extra variables (extended analysis)
#df_agg_ext <- aggregate(cbind(denom,under_vaccinated)~sex+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_3cat+age_4cat+bmi_cat+shielding+region+last_positive_test_group, data=df_cohort_reduced_12_15, FUN=sum)
#lr_ind_vars_ext = c("sex", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_3cat","bmi_cat","shielding","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext, "12-15", lr_dep_var, lr_ind_vars_ext)

# Run model adding on extra variables (extended analysis - removing BMI and shielding)
#df_agg_ext2 <- aggregate(cbind(denom,under_vaccinated)~sex+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_3cat+age_4cat+region+last_positive_test_group, data=df_cohort_reduced_12_15, FUN=sum)
#lr_ind_vars_ext2 = c("sex", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_3cat","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext2, "12-15", lr_dep_var, lr_ind_vars_ext2)

############################################
# Model using aggregated dataset for 16-74 #
############################################
# Select data for age group "16-74"
df_cohort_reduced_16_74 = filter(df_cohort_reduced, age_4cat == "16-74", is.na(death_date) | death_date > study_start) %>% droplevels()

# Run model with the 6 core variables (main analysis)
df_agg <- aggregate(cbind(denom,under_vaccinated)~sex+age_17cat+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_cat+age_4cat, data=df_cohort_reduced_16_74, FUN=sum)
df_agg = df_agg %>% mutate(age_17cat = fct_relevel(age_17cat, "18-24"))
lr_ind_vars = c("sex", "age_17cat", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_cat")
lr_analysis_agg(df_agg, "16-74", lr_dep_var, lr_ind_vars)

# Run model adding on extra variables (extended analysis)
#df_agg_ext <- aggregate(cbind(denom,under_vaccinated)~sex+age_17cat+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_cat+age_4cat+bmi_cat+shielding+region+last_positive_test_group, data=df_cohort_reduced_16_74, FUN=sum)
#df_agg_ext = df_agg_ext %>% mutate(age_17cat = fct_relevel(age_17cat, "18-24"))
#lr_ind_vars_ext = c("sex", "age_17cat", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_cat","bmi_cat","shielding","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext, "16-74", lr_dep_var, lr_ind_vars_ext)

# Run model adding on extra variables (extended analysis - removing shielding)
#df_agg_ext2 <- aggregate(cbind(denom,under_vaccinated)~sex+age_17cat+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_cat+age_4cat+bmi_cat+region+last_positive_test_group, data=df_cohort_reduced_16_74, FUN=sum)
#df_agg_ext2 = df_agg_ext2 %>% mutate(age_17cat = fct_relevel(age_17cat, "18-24"))
#lr_ind_vars_ext2 = c("sex", "age_17cat", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_cat","bmi_cat","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext2, "16-74", lr_dep_var, lr_ind_vars_ext2)

##########################################
# Model using aggregated dataset for 75+ #
##########################################
# Select data for age group "75+"
df_cohort_reduced_75plus = filter(df_cohort_reduced, age_4cat == "75+", is.na(death_date) | death_date > study_start) %>% droplevels()

# Run model with the 6 core variables (main analysis)
df_agg <- aggregate(cbind(denom,under_vaccinated)~sex+age_17cat+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_cat+age_4cat, data=df_cohort_reduced_75plus, FUN=sum)
lr_ind_vars = c("sex", "age_17cat", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_cat")
lr_analysis_agg(df_agg, "75+", lr_dep_var, lr_ind_vars)

# Run model adding on extra variables (extended analysis)
#df_agg_ext <- aggregate(cbind(denom,under_vaccinated)~sex+age_17cat+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_cat+age_4cat+bmi_cat+shielding+region+last_positive_test_group, data=df_cohort_reduced_75plus, FUN=sum)
#lr_ind_vars_ext = c("sex", "age_17cat", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_cat","bmi_cat","shielding","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext, "75+", lr_dep_var, lr_ind_vars_ext)

# Run model adding on extra variables (extended analysis - removing shielding)
#df_agg_ext2 <- aggregate(cbind(denom,under_vaccinated)~sex+age_17cat+ethnicity_5cat+urban_rural_2cat+imd_2019_quintiles+qc_count_cat+age_4cat+bmi_cat+region+last_positive_test_group, data=df_cohort_reduced_75plus, FUN=sum)
#lr_ind_vars_ext2 = c("sex", "age_17cat", "ethnicity_5cat", "urban_rural_2cat", "imd_2019_quintiles", "qc_count_cat","bmi_cat","region","last_positive_test_group")
#lr_analysis_agg(df_agg_ext2, "75+", lr_dep_var, lr_ind_vars_ext2)
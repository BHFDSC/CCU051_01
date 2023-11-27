#################################################################################
## TITLE: Longitudinal analysis for England (dataset (CCU051/COALESCE project) 
##
## Description: Create survival data with time-varying exposure 
##             and run Cox models for 3 age groups
##
##
## Created on: 2023-07-04 (in SDE)
## Updated on: 2023-08-30
##
## Created and updated by : Genevieve Cezard
## 
################################################################################
library(tidyverse)
library(survival)
library(broom)

output_dir = "./output/"
setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU051")


# Read in
df_cohort = readRDS('./data/df_cohort.rds')

# Use sample to test code
#df_cohort10 = readRDS('./data/df_cohort_test10.rds')

study_start = as.Date("2022-06-01")
study_end = as.Date("2022-09-30")

# To display names in output tables
var_names <- c(
  "Total N (%)",
  "sex",
  "age",
  "age_3cat",
  "age_4cat",
  "age_17cat",
  "imd_2019_quintiles",
  "urban_rural_2cat",
  "ethnicity_5cat",
  "qc_count_cat",
  "qc_count_3cat",
  "shielding",
  "bmi_cat",
  "region",
  "last_positive_test_group"
)
# All display names for the table
display_names <- c(
  "Total",
  "Sex",
  "Age",
  "Age group",
  "Age group",
  "Age group",
  "IMD quintile",
  "Urban/rural classification",
  "Ethnicity",
  "Number of QCovid risk groups",
  "Number of QCovid risk groups",
  "Ever shielding",
  "BMI",
  "Region",
  "Last positive test"
)

##########################################
## 1- Create dataset with survival time ##
##########################################
# Select variables of interest only
df_cohort_reduced <- df_cohort %>% 
  dplyr::select('individual_id','under_vaccinated','num_doses_start','date_vacc_1', 'date_vacc_2', 'date_vacc_3', 'date_vacc_4', 'date_vacc_5',
                'age_3cat','age_4cat','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_cat', 'qc_count_3cat',
                'shielding','region','last_positive_test_group', 'death_date','covid_mcoa_em_hosp_death','covid_mcoa_em_hosp_death_date')
df_cohort_reduced$region = fct_relevel(df_cohort_reduced$region, "London")

# Calculation of survival time on full cohort 
df_cohort_reduced$surv_date <- as.Date("2022-09-30")
df_cohort_reduced$surv_date <- pmin(df_cohort_reduced$surv_date,df_cohort_reduced$covid_mcoa_em_hosp_death_date,df_cohort_reduced$death_date, na.rm = TRUE)

df_cohort_reduced <- df_cohort_reduced %>% mutate(surv_time = as.numeric(surv_date - as.Date("2022-06-01")))
df_cohort_reduced$tstart <- as.numeric(0)
df_cohort_reduced$tstop <- as.numeric(df_cohort_reduced$surv_time)


##########################################
## 2- Create the 3 samples for analysis ##
##########################################
Get_Cox_sample = function(df_cohort,dep_var) {
  # Set seed to fix samples
  set.seed(12345)
  
  # Total population
  N_cohort <- nrow(df_cohort)
  
  # Select case
  select_case <- df_cohort[df_cohort[[dep_var]] == "1",]
  N_case <- nrow(select_case)
  print(paste0("N_case: ",N_case))
  
  # Select 50 controls per case
  all_control <- df_cohort[df_cohort[[dep_var]] == "0",]
  select_control <-all_control[sample(nrow(all_control),size = N_case*50,replace = FALSE),]
  N_control <- nrow(select_control)
  print(paste0("N_control: ",N_control))
  
  # Combine
  Cox_sample <- rbind(select_case,select_control)
  N_sample <- nrow(Cox_sample)
  print(paste0("N_sample: ", N_sample))
  #N_sample_unique <- length(unique(Cox_sample$individual_id))
  #print(paste0("N_sample_unique: ",N_sample_unique))
  
  # Calculate weights
  Cox_sample$weight <- ifelse(Cox_sample[[dep_var]] == "1",1,(N_cohort-N_case)/N_control)
  table(Cox_sample$weight)
  
  return(Cox_sample)
}

#------------------------------------------------------------#
# 2.a. Create and save the sample dataset for 5-15 years old #
#------------------------------------------------------------#
df_cohort_5_15 <- subset(df_cohort_reduced, age_3cat == "5-15")
Cox_sample_5_15 <- Get_Cox_sample(df_cohort_5_15,"covid_mcoa_em_hosp_death")
saveRDS(Cox_sample_5_15, './data/Cox_sample_5_15.rds')

# Check for unique IDs - There was some duplicate issues - now solved
#length(unique(Cox_sample_5_15$individual_id))
#nrow(Cox_sample_5_15)
# Identify duplicates
#Cox_sample_5_15$individual_id[duplicated(Cox_sample_5_15$individual_id)]

#-------------------------------------------------------------#
# 2.b. Create and save the sample dataset for 16-74 years old #
#-------------------------------------------------------------#
df_cohort_16_74 <- subset(df_cohort_reduced, age_3cat == "16-74")
Cox_sample_16_74 <- Get_Cox_sample(df_cohort_16_74,"covid_mcoa_em_hosp_death")
Cox_sample_16_74$age_17cat = fct_relevel(Cox_sample_16_74$age_17cat, "18-24")
saveRDS(Cox_sample_16_74, './data/Cox_sample_16_74.rds')

#-----------------------------------------------------------#
# 2.c. Create and save the sample dataset for 75+ years old #
#-----------------------------------------------------------#
df_cohort_75plus <- subset(df_cohort_reduced, age_3cat == "75+")
Cox_sample_75plus <- Get_Cox_sample(df_cohort_75plus,"covid_mcoa_em_hosp_death")
Cox_sample_75plus$age_17cat = fct_relevel(Cox_sample_75plus$age_17cat, "75-79")
saveRDS(Cox_sample_75plus, './data/Cox_sample_75plus.rds')


############################################################
## 3. Create vaccine deficit variable and associated time ##
############################################################

# Try initializing tmerge() for the full sample
#df_cohort_reduced$surv_time <- ifelse(df_cohort_reduced$surv_time == 0, 0.0001, df_cohort_reduced$surv_time)
#df_wide = tmerge(df_cohort_reduced, df_cohort_reduced, id = individual_id, event = event(surv_time, covid_mcoa_em_hosp_death))
# I get the following error message: "Error: memory exhausted (limit reached?)"
# It worked if I only keep "df_cohort_reduced" in the environment - It takes less than 2 hours

# Faster: read in Cox_sample
#Cox_sample_5_15 = readRDS('./data/Cox_sample_5_15.rds')
#Cox_sample_16_74 = readRDS('./data/Cox_sample_16_74.rds')
#Cox_sample_75plus = readRDS('./data/Cox_sample_75plus.rds')

Add_time_varying_exposure = function(df) {
  #df<-Cox_sample_5_15 # For testing
  
  # Adapted Steven's code - see lines 327-557 in "04b_cox_analysis.R"                    #
  # https://github.com/EAVE-II/under_vaccinated/blob/master/code/03_cohort_description.R #
  df$surv_time <- ifelse(df$surv_time == 0, 0.0001, df$surv_time)
  df = tmerge(df, df, id = individual_id, event = event(surv_time, covid_mcoa_em_hosp_death))
  
  df_vs <- df %>%
    mutate(
      vacc_0 = -Inf,
      vacc_1 = as.numeric(as.Date(date_vacc_1) - study_start),
      vacc_2 = as.numeric(as.Date(date_vacc_2) - study_start),
      vacc_3 = as.numeric(as.Date(date_vacc_3) - study_start),
      vacc_4 = as.numeric(as.Date(date_vacc_4) - study_start),
      vacc_5 = as.numeric(as.Date(date_vacc_5) - study_start)
    ) %>%
    dplyr::select('individual_id', 'vacc_0', 'vacc_1', 'vacc_2', 'vacc_3', 'vacc_4', 'vacc_5')
  #print(head(df_vs))
  
  df_long = pivot_longer(df_vs,
                       cols = c("vacc_0", "vacc_1", "vacc_2", "vacc_3", "vacc_4", "vacc_5"),
                       names_to = "vs", values_to = "time"
  ) %>%   filter(!is.na(time))
  #print(head(df_long))
  
  df_long$vs <- as.numeric(gsub("vacc_","",df_long$vs))
  #class(df_long$vs)
  
  df_vs = tmerge(df, df_long, id = individual_id, exposure = tdc(time, vs)) %>% rename(time = exposure)

  # Add vaccine deficit status (variable "vacc_deficit_time")
  df_vs = df_vs %>%
    mutate(
      vacc_deficit_time = case_when(
        age_4cat == "5-11" ~ pmax(1-time, 0),
        age_4cat == "12-15" ~ pmax(2-time, 0),
        age_4cat == "16-74" ~ pmax(3-time, 0),
        age_4cat == "75+" ~ pmax(4-time, 0)
      )
    ) %>%
    mutate(vacc_deficit_time = factor(vacc_deficit_time))
  
  # Recalculate surv_time (variable "vacc_deficit_surv_time") - used for PY calculation
  df_vs = df_vs %>%
    mutate(vacc_deficit_surv_time = tstop - tstart)

  return(df_vs)
}

Cox_sample_5_15_vs <- Add_time_varying_exposure(Cox_sample_5_15)
Cox_sample_16_74_vs <- Add_time_varying_exposure(Cox_sample_16_74)
Cox_sample_75plus_vs <- Add_time_varying_exposure(Cox_sample_75plus)

#####################################
## 4. Run model and output results ##
#####################################
Output_results = function(analysis_type, dep_var, age_group) {
  #create directory
  dir = paste0(output_dir, analysis_type, "_", dep_var, "_", age_group)
  if (!dir.exists(dir)) {
    dir.create(dir)
  }
  # Coefficients
  coef = data.frame("variable" = names(coef(model)), "coef" = coef(model), row.names = 1:length(coef(model)))
  write.csv(coef, paste0(dir, "/coef_",analysis_type,"_",age_group,".csv"), row.names = FALSE)
  
  # Covariance matrix
  cov = vcov(model)
  write.csv(cov, paste0(dir, "/cov_",analysis_type,"_",age_group,".csv"))
  
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
  write.csv(cox_results, paste0(dir, "/results_",analysis_type,"_",age_group,".csv"), row.names = FALSE)
  
  # Test proportional hazards assumptions via Schoenfeld residuals.
  ph_test = cox.zph(model)
  ph_test$model <- analysis_type
  ph_test$age_group <- age_group
  df = data.frame(
    print(ph_test)
  )
  write.csv(df, paste0(output_dir,"Cox_vaxdef_tests/ph_test_",analysis_type,"_",age_group,".csv"))
  
  # Schoenfeld_residuals_plot per variable
  resid = data.frame(
    trans_time = ph_test$x,
    time = ph_test$time,
    ph_test$y
  ) %>% 
    pivot_longer(
      cols = c(-trans_time, -time)
    ) 
  
  plot<- resid %>% 
    ggplot(aes(
      x = trans_time,
      y = value
    )) +
    facet_wrap(~name) +
    geom_smooth()
  ggsave(paste0(output_dir,"Cox_vaxdef_tests/Schoenfeld_residuals_plot_",analysis_type,"_",age_group,".png"), height = 297, width = 210, unit = "mm", dpi = 300, scale = 1)

  # Schoenfeld_residuals_plot per category
  ph_test = cox.zph(model,terms=FALSE)
  ph_test$model <- analysis_type
  ph_test$age_group <- age_group
  
  resid = data.frame(
    trans_time = ph_test$x,
    time = ph_test$time,
    ph_test$y
  ) %>% 
    pivot_longer(
      cols = c(-trans_time, -time)
    ) 
  
  plot<- resid %>% 
    ggplot(aes(
      x = trans_time,
      y = value
    )) +
    facet_wrap(~name) +
    geom_smooth()
  ggsave(paste0(output_dir,"Cox_vaxdef_tests/Schoenfeld_residuals_plot_v2_",analysis_type,"_",age_group,".png"), height = 297, width = 210, unit = "mm", dpi = 300, scale = 1)
  
}

# Test model performance 
Model_perf = function(model,analysis_type, age_group) { 
  models_performance = data.frame(print(broom::glance(model)))
  models_performance$model <- analysis_type
  models_performance$age_group <- age_group
  write.csv(models_performance, paste0(output_dir, "Cox_vaxdef_tests/Models_performance_",analysis_type,"_",age_group,".csv"))

  return(models_performance)
}

#--------------------------------#
# 4.a. Cox models for 5-15 y old #
#--------------------------------#
# Unadjusted analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time, data = Cox_sample_5_15_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death",age_group = "5-15")
models_perf_unadj_5_15 <- Model_perf(model = model,analysis_type="Cox_vaxdef_unadj",age_group = "5-15")

# Main analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_3cat, data = Cox_sample_5_15_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_main",dep_var = "covid_mcoa_em_hosp_death",age_group = "5-15")
models_perf_main_5_15 <- Model_perf(model = model,analysis_type="Cox_vaxdef_main",age_group = "5-15")

# Extended analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_3cat + shielding + region + last_positive_test_group, data = Cox_sample_5_15_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_ext",dep_var = "covid_mcoa_em_hosp_death",age_group = "5-15")
models_perf_ext_5_15 <- Model_perf(model = model,analysis_type="Cox_vaxdef_ext",age_group = "5-15")

#---------------------------------#
# 4.b. Cox models for 16-74 y old #
#---------------------------------#
# Unadjusted analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time, data = Cox_sample_16_74_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death",age_group = "16-74")
models_perf_unadj_16_74 <- Model_perf(model = model,analysis_type="Cox_vaxdef_unadj",age_group = "16-74")

# Main analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat, data = Cox_sample_16_74_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_main",dep_var = "covid_mcoa_em_hosp_death",age_group = "16-74")
models_perf_main_16_74 <- Model_perf(model = model,analysis_type="Cox_vaxdef_main",age_group = "16-74")

# Extended analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat + shielding + region + last_positive_test_group, data = Cox_sample_16_74_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_ext",dep_var = "covid_mcoa_em_hosp_death",age_group = "16-74")
models_perf_ext_16_74 <- Model_perf(model = model,analysis_type="Cox_vaxdef_ext",age_group = "16-74")

#-------------------------------#
# 4.c. Cox models for 75+ y old #
#-------------------------------#
# Unadjusted analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time, data = Cox_sample_75plus_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death",age_group = "75plus")
models_perf_unadj_75plus <- Model_perf(model = model,analysis_type="Cox_vaxdef_unadj",age_group = "75plus")

# Main analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat, data = Cox_sample_75plus_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_main",dep_var = "covid_mcoa_em_hosp_death",age_group = "75plus")
models_perf_main_75plus <- Model_perf(model = model,analysis_type="Cox_vaxdef_main",age_group = "75plus")

# Extended analysis
model = coxph(Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time + sex + age_17cat + ethnicity_5cat + urban_rural_2cat + imd_2019_quintiles + qc_count_cat + shielding + region + last_positive_test_group, data = Cox_sample_75plus_vs, weights = weight)
Output_results(analysis_type="Cox_vaxdef_ext",dep_var = "covid_mcoa_em_hosp_death",age_group = "75plus")
models_perf_ext_75plus <- Model_perf(model = model,analysis_type="Cox_vaxdef_ext",age_group = "75plus")

#############################################################
## 5. Model performance and proportional hazard assumption ##
#############################################################
#---------------------------#
# Combine model performance #
#---------------------------#
models_performance <- rbind(models_perf_unadj_5_15,models_perf_main_5_15,models_perf_ext_5_15,
                            models_perf_unadj_16_74,models_perf_main_16_74,models_perf_ext_16_74,
                            models_perf_unadj_75plus,models_perf_main_75plus,models_perf_ext_75plus)
models_performance <- models_performance %>% dplyr::select(-n,-nevent,-nobs)

write.csv(models_performance, paste0(output_dir, "Cox_vaxdef_tests/Models_performance_all_models.csv"))

#---------------------------------------#
# Get Kaplan Meier curves per age group #
#---------------------------------------#
# Generate numbers in table to be exported #
# What about weight?? Where would this fit in that table.

Get_N_survival_plot= function(analysis_type, dep_var, ind_vars, dataset_in,age_group) {
  formula <- paste0("Surv(tstart,tstop,",dep_var,") ~",paste(ind_vars, collapse = " + "))
  CI_table = survfit( 
    #as.formula("Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time"),
    as.formula(formula),
    # Reset the origin to take account of time
    # That is, e.g. for dose 1 we start the clock at 0 on the day they received their dose
    # Then when they receive dose 2, we restart the clock again at 0
    # Necessary to do it this way rather than just using the duration column
    # because they could have other time-dependent variables
    #data = Cox_sample_5_15_vs %>%
    data = dataset_in %>%
      group_by(individual_id, vacc_deficit_time) %>%
      mutate(
        offset = min(tstart),
        tstart = tstart - offset,
        tstop = tstop - offset
      ) %>%
      select(-offset) %>%
      ungroup() %>%
      droplevels()
  ) %>%
    tidy() %>%
    mutate(
      strata = gsub("vacc_deficit_time=0", "0", strata),
      strata = gsub("vacc_deficit_time=1", "1", strata),
      strata = gsub("vacc_deficit_time=2", "2", strata),
      strata = gsub("vacc_deficit_time=3", "3", strata),
      strata = gsub("vacc_deficit_time=4", "4", strata)
    )
  print(head(CI_table))    
  CI_table = CI_table %>% mutate(week = floor(time/7)) %>%
    group_by(strata,week) %>%
    mutate(
      #n.risk = sum(n.risk),
      n.event = sum(n.event)
    ) %>% 
    # Add in the function to round numbers and reselect variables to keep
    mutate( 
      n.risk_rounded5 = round(n.risk/5.0)*5,
      n.event_rounded5 = round(n.event/5.0)*5
    )%>%
    select(week, strata, n.risk_rounded5, n.event_rounded5) %>%
    #unique()
    slice(1)
  print(head(CI_table))
  
  #write.csv(CI_table, paste0(dir, "/CI_table.csv"))
  write.csv(CI_table, paste0(output_dir, "/Cox_vaxdef_tests/CI_table_",analysis_type,"_",age_group,".csv"))
}

Get_N_survival_plot(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "vacc_deficit_time", dataset_in = Cox_sample_5_15_vs, age_group = "5-15")
Get_N_survival_plot(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "vacc_deficit_time", dataset_in = Cox_sample_16_74_vs, age_group = "16-74")
Get_N_survival_plot(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "vacc_deficit_time", dataset_in = Cox_sample_75plus_vs, age_group = "75+")

#-----------------------------------------------------------------------#
# Get Kaplan Meier curves per age group - weighted for under_vaccinated #
#-----------------------------------------------------------------------#
# Generate numbers in table to be exported #

Get_N_survival_plot_weighted= function(analysis_type, dep_var, ind_vars, dataset_in,age_group) {
  dataset_in <- dataset_in %>% mutate(tstop = ifelse(tstop == 0, 0.001, tstop))
  formula <- paste0("Surv(tstart,tstop,",dep_var,") ~",paste(ind_vars, collapse = " + "))
  CI_table = survfit( 
    #as.formula("Surv(tstart,tstop,covid_mcoa_em_hosp_death) ~ vacc_deficit_time"),
    as.formula(formula),
    # Reset the origin to take account of time
    # That is, e.g. for dose 1 we start the clock at 0 on the day they received their dose
    # Then when they receive dose 2, we restart the clock again at 0
    # Necessary to do it this way rather than just using the duration column
    # because they could have other time-dependent variables
    #data = Cox_sample_5_15_vs %>%
    data = dataset_in %>%
      group_by(individual_id, vacc_deficit_time) %>%
      #group_by(individual_id, under_vaccinated) %>%
      mutate(
        offset = min(tstart),
        tstart = tstart - offset,
        tstop = tstop - offset
      ) %>%
      select(-offset) %>%
      ungroup() %>%
      droplevels(),
    weights = weight
  ) %>%
    tidy() %>%
    mutate(
      #strata = gsub("under_vaccinated=0","0", strata),
      #strata = gsub("under_vaccinated=1","1", strata)
      strata = gsub("vacc_deficit_time=0", "0", strata),
      strata = gsub("vacc_deficit_time=1", "1", strata),
      strata = gsub("vacc_deficit_time=2", "2", strata),
      strata = gsub("vacc_deficit_time=3", "3", strata),
      strata = gsub("vacc_deficit_time=4", "4", strata)
    )
  print(head(CI_table))  
  CI_table = CI_table %>% mutate(week = floor(time/7)) %>%
    group_by(strata,week) %>%
    mutate(
      #n.risk = sum(n.risk),
      n.event = sum(n.event)
    ) %>% 
    # Add in the function to round numbers and reselect variables to keep
    mutate( 
      n.risk_rounded5 = round(n.risk/5.0)*5,
      n.event_rounded5 = round(n.event/5.0)*5
    )%>%
    select(week, strata,n.risk_rounded5, n.event_rounded5) %>%
    #unique()
    slice(1)
  print(head(CI_table))
  
  #write.csv(CI_table, paste0(dir, "/CI_table.csv"))
  write.csv(CI_table, paste0(output_dir, "/Cox_vaxdef_tests/CI_table_",analysis_type,"_",age_group,"_weighted.csv"))
  
}

Get_N_survival_plot_weighted(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "vacc_deficit_time", dataset_in = Cox_sample_5_15_vs, age_group = "5-15")
Get_N_survival_plot_weighted(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "vacc_deficit_time", dataset_in = Cox_sample_16_74_vs, age_group = "16-74")
Get_N_survival_plot_weighted(analysis_type="Cox_vaxdef_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "vacc_deficit_time", dataset_in = Cox_sample_75plus_vs, age_group = "75+")

#Cox_sample_5_15 <- Cox_sample_5_15 %>% mutate(tstop = ifelse(tstop == 0, 0.001, tstop))
#Get_N_survival_plot_weighted(analysis_type="Cox_unvax_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "under_vaccinated", dataset_in = Cox_sample_5_15, age_group = "5-15")
#Get_N_survival_plot_weighted(analysis_type="Cox_unvax_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "under_vaccinated", dataset_in = Cox_sample_16_74, age_group = "16-74")
#Get_N_survival_plot_weighted(analysis_type="Cox_unvax_unadj",dep_var = "covid_mcoa_em_hosp_death", ind_var = "under_vaccinated", dataset_in = Cox_sample_75plus, age_group = "75+")


  
#-------------------#
# Test collinearity #
#-------------------#
library(car)
vif(model)
# This doesn't work on unadjusted model, we need at least 2 variables in the model
# Tested for the main model in children, I get the following message:
# => Error in vif.default(model) : there are aliased coefficients in the model


#################################################################################
## TITLE: Longitudinal analysis for England (dataset (CCU051/COALESCE project) 
##
## Description: Calculation of person-years
##
## The function code is adapted from code developed by Stuart Bedston
##
## Created on: 2023-04-17
## Updated on: 2023-05-09 (DAE)
## Updated on: 2023-08-17 (SDE)
##
## Created and updated by : Genevieve Cezard
## 
######################################################################
library(tidyverse)
library(survival)

output_dir = "./output/"
setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU051")

# Read in
df_cohort = readRDS('./data/df_cohort.rds')

study_start = as.Date("2022-06-01")
study_end = as.Date("2022-09-30")

create_dir = function(age_group) {
  dir = paste0(output_dir, "py_", age_group)
  if (!dir.exists(dir)) {
    dir.create(dir)
  }
}

###################################
## 1. CALCULATION OF PERSON-YEAR ##
###################################
# Select variables of interest only
df_cohort_reduced <- df_cohort %>% dplyr::select('under_vaccinated','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_cat', 'qc_count_3cat','death_date','age_4cat','age_3cat','covid_mcoa_em_hosp_death','covid_mcoa_em_hosp_death_date','bmi_cat','shielding','region','last_positive_test_group')

# Calculation on full cohort 
df_cohort_reduced$surv_date <- as.Date("2022-09-30")
df_cohort_reduced$surv_date <- pmin(df_cohort_reduced$surv_date,df_cohort_reduced$covid_mcoa_em_hosp_death_date,df_cohort_reduced$death_date, na.rm = TRUE)
df_cohort_reduced$tstart <- as.Date("2022-06-01")
df_cohort_reduced <- df_cohort_reduced %>% mutate(surv_time = as.numeric(surv_date - tstart))

# surv_time is giving person-day for each individual
#summary(df_cohort_reduced$surv_time)


# describe events and person-years
describe_py <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      events = sum(covid_mcoa_em_hosp_death == "1"),
      pyears = sum(surv_time)/365.25
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100
    ) %>%
    select(xvar, xlbl = one_of(xvar), total_n, total_p, events, pyears
    ) %>% # Add in the function to round numbers and reselect variables to keep
    mutate( 
      total_n_rounded5 = round(total_n/5.0)*5,
      total_p = round(total_p, 1),
      events_rounded5 = round(events/5.0)*5,
      pyears_rounded1 = round(pyears, 0),
      rate_per_1000 = round( (1000 * events_rounded5 / pyears_rounded1),2)
    )%>%
    select(xvar,xlbl,total_n_rounded5,total_p,events_rounded5,pyears_rounded1,rate_per_1000)
}

# add dummy "total" column to allow easy total stats to be added to the table
df_cohort_reduced <- df_cohort_reduced %>% mutate(total = factor("total"))

xvars_min <- c('under_vaccinated','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_cat','bmi_cat','region','shielding','last_positive_test_group')

# Removed 'bmi_cat' from the kids list due to some disclosure issues in the high bmi categories
xvars_min_kids <- c('under_vaccinated','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_3cat','region','shielding','last_positive_test_group')


hosp_death_overall_np <-
  df_cohort_reduced %>%
  lapply(c("total", xvars_min), describe_py, .data = .) %>%
  dplyr::bind_rows() %>%
  write_csv( file = "output/hosp_death_overall_np.csv")

hosp_death_05_15_np <-
  df_cohort_reduced %>%
  filter(age_3cat == "5-15") %>%
  lapply(c("total", xvars_min_kids), describe_py, .data = .) %>%
  dplyr::bind_rows() %>%
  write_csv( file = "output/hosp_death_05_15_np.csv")

hosp_death_16_74_np <-
  df_cohort_reduced %>%
  filter(age_3cat == "16-74") %>%
  lapply(c("total", xvars_min), describe_py, .data = .) %>%
  dplyr::bind_rows() %>%
  write_csv( file = "output/hosp_death_16_74_np.csv")

hosp_death_75plus_np <-
  df_cohort_reduced %>%
  filter(age_3cat == "75+") %>%
  lapply(c("total", xvars_min), describe_py, .data = .) %>%
  dplyr::bind_rows() %>%
  write_csv( file = "output/hosp_death_75plus_np.csv")



##########################################################
## 2. CALCULATION OF PERSON-YEAR on sample with weights ##
##########################################################

# 1- Add time for time varying exposure (vaccine deficit) in the 3 samples
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
Cox_sample_5_15 = readRDS('./data/Cox_sample_5_15.rds')
Cox_sample_16_74 = readRDS('./data/Cox_sample_16_74.rds')
Cox_sample_75plus = readRDS('./data/Cox_sample_75plus.rds')

Cox_sample_5_15_vs <- Add_time_varying_exposure(Cox_sample_5_15)
Cox_sample_16_74_vs <- Add_time_varying_exposure(Cox_sample_16_74)
Cox_sample_75plus_vs <- Add_time_varying_exposure(Cox_sample_75plus)

Cox_sample_5_15_vs$surv_time_weighted <- Cox_sample_5_15_vs$vacc_deficit_surv_time * Cox_sample_5_15_vs$weight
Cox_sample_16_74_vs$surv_time_weighted <- Cox_sample_16_74_vs$vacc_deficit_surv_time * Cox_sample_16_74_vs$weight
Cox_sample_75plus_vs$surv_time_weighted <- Cox_sample_75plus_vs$vacc_deficit_surv_time * Cox_sample_75plus_vs$weight

# 2. Describe number of events and person-years
describe_py_weight <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      n_sample = n(),
      n_weighted = sum(weight),
      events = sum(event == "1"), # Use "event" as it is updated based on changes in vaccine deficit status
      pyears_weighted = sum(surv_time_weighted)/365.25,
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = n_weighted / sum(n_weighted) * 100
    ) %>%
    select(xvar, xlbl = one_of(xvar), n_sample, n_weighted, total_p, events, pyears_weighted
    ) %>% # Add in the function to round numbers and re-select variables to keep
    mutate( 
      n_sample_rounded5 = round(n_sample/5.0)*5,
      n_weighted_rounded5 = round(n_weighted/5.0)*5,
      total_p = round(total_p, 1),
      events_rounded5 = round(events/5.0)*5,
      pyears_weighted_rounded_1decimal = round(pyears_weighted, 1),
      rate_per_1000 = round( (1000 * events_rounded5 / pyears_weighted_rounded_1decimal),2)
    )%>%
    select(xvar,xlbl,n_sample_rounded5,n_weighted_rounded5,total_p,events_rounded5,pyears_weighted_rounded_1decimal,rate_per_1000)
}

xvars_min <- c('vacc_deficit_time','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_cat','region','shielding','last_positive_test_group')
xvars_min_kids <- c('vacc_deficit_time','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_3cat','region','shielding','last_positive_test_group')

Cox_sample_5_15_vs <- Cox_sample_5_15_vs %>% mutate(total = factor("total"))
hosp_death_vaxdef_05_15_np <-
  Cox_sample_5_15_vs %>%
  filter(age_3cat == "5-15") %>%
  lapply(c("total", xvars_min_kids), describe_py_weight, .data = .) %>%
  dplyr::bind_rows() %>%
  write_csv( file = "output/hosp_death_vaxdef_05_15_np.csv")

Cox_sample_16_74_vs <- Cox_sample_16_74_vs %>% mutate(total = factor("total"))
hosp_death_vaxdef_16_74_np <-
  Cox_sample_16_74_vs %>%
  filter(age_3cat == "16-74") %>%
  lapply(c("total", xvars_min), describe_py_weight, .data = .) %>%
  dplyr::bind_rows() %>%
  write_csv( file = "output/hosp_death_vaxdef_16_74_np.csv")

Cox_sample_75plus_vs <- Cox_sample_75plus_vs %>% mutate(total = factor("total"))
hosp_death_vaxdef_75plus_np <-
  Cox_sample_75plus_vs %>%
  filter(age_3cat == "75+") %>%
  lapply(c("total", xvars_min), describe_py_weight, .data = .) %>%
  dplyr::bind_rows() %>%
  write_csv( file = "output/hosp_death_vaxdef_75plus_np.csv")

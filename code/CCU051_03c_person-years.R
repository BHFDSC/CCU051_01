#################################################################################
## TITLE: Longitudinal analysis for England (dataset (CCU051/COALESCE project) 
##
## Description: Calculation of person-years
##
## The function code is adapted from code developed by Stuart Bedston
##
## Created on: 2023-04-17
## Updated on: 2023-05-09
##
## Created and updated by : Genevieve Cezard
## 
######################################################################
library(tidyverse)

output_dir = "./output/"

setwd("/mnt/efs/gic30/dars_nic_391419_j3w9t_collab/CCU051")
#setwd("/mnt/efs/as3293/dars_nic_391419_j3w9t_collab/CCU051")

# Read in
df_cohort = readRDS('./data/df_cohort.rds')

create_dir = function(age_group) {
  dir = paste0(output_dir, "py_", age_group)
  if (!dir.exists(dir)) {
    dir.create(dir)
  }
}

################################
## CALCULATION OF PERSON-YEAR ##
################################
# Select variables of interest only
df_cohort_reduced <- df_cohort %>% dplyr::select('under_vaccinated','sex','age_17cat', 'ethnicity_5cat', 'urban_rural_2cat', 'imd_2019_quintiles', 'qc_count_cat', 'qc_count_3cat','death_date','age_4cat','age_3cat','covid_mcoa_em_hosp_death','covid_mcoa_em_hosp_death_date','bmi_cat','shielding','region','last_positive_test_group')

# Calculation on full cohort 
df_cohort_reduced$surv_date <- as.Date("2022-09-30")
df_cohort_reduced$surv_date <- pmin(df_cohort_reduced$surv_date,df_cohort_reduced$covid_mcoa_em_hosp_death_date,df_cohort_reduced$death_date, na.rm = TRUE)
df_cohort_reduced$tstart <- as.Date("2022-06-01")
df_cohort_reduced <- df_cohort_reduced %>% mutate(surv_time = as.numeric(surv_date - tstart))

# surv_time is giving person-day for each individual
#summary(df_cohort_reduced$surv_time)


# describe events and person-years =============================================
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
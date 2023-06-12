############################################################################
## TITLE: Data preparation of England dataset for CCU051 - COALESCE project 
##
## Description: Formatting and adding derived variables
##
## Partly based on code developed by Steven Kerr and Stuart Bedston 
##
## Created on: 2023-01-31
## Updated on: 2023-05-08
##
## By : Genevieve Cezard, Alexia Sampri
##
###########################################################################
library(tidyverse)

setwd("/mnt/efs/gic30/dars_nic_391419_j3w9t_collab/CCU051")
#setwd("/mnt/efs/as3293/dars_nic_391419_j3w9t_collab/CCU051")

study_start = as.Date("2022-06-01")
study_end = as.Date("2022-09-30")

#df <- data.table::fread("data/ccu051_cohort20230331.csv.gz", data.table = FALSE)
df <- data.table::fread("data/ccu051_cohort20230426.csv.gz", data.table = FALSE)
df_QCovid <- data.table::fread("data/ccu051_qcovid20230331.csv.gz", data.table = FALSE)


##--------------------------------------------------------##
## 0- Select a random sample of the data to test the code ##
##--------------------------------------------------------##
# For example take a 10% random sample of the England dataset (CHUNK 1)
#df_test10 <- subset(df, CHUNK == 1)

# Test with a 40 % random sample of the England dataset (CHUNK 1-4)
#df_test40 <- subset(df, CHUNK < 5)

# Test with a 50 % random sample of the England dataset (CHUNK 1-5)
#df_test50 <- subset(df, CHUNK < 6)


############################################################
#### Create cohort dataframe with all required variables ###
############################################################
# Remove the CHUNK variable in both datasets
df <- df %>% select(-CHUNK)
#df_test <- df_test10 %>% select(-CHUNK)
#rm(df_test10)
#df_test <- df_test40 %>% select(-CHUNK)
#rm(df_test40)
#df_test <- df_test50 %>% select(-CHUNK)
#rm(df_test50)
df_QCovid <- df_QCovid %>% select(-CHUNK)


# Join information back into a unique dataset
#df_cohort <- left_join(df_test, df_QCovid, by = "PERSON_ID")
df_cohort <- left_join(df, df_QCovid, by = "PERSON_ID") # -> this keeps leading to memory issues but occasional runs successfully

rm(df_QCovid)
rm(df)
#rm(df_test)

##-----------------------------------------##
## 0- Reformat/Refactor existing variables ##
##-----------------------------------------##
df_cohort <- df_cohort %>% mutate(
    sex = fct_recode(
      as.character(sex), 
      `Male` = '1',
      `Female` = '2'
      ),
    imd_2019_quintiles = fct_recode(
      as.character(imd_2019_quintiles), 
        `1 - Most deprived` = '1',
        `5 - Least deprived` = '5'
      ),
    ethnicity_5cat = fct_recode(
      as.character(ethnicity_5cat), 
      `Asian` = 'Asian or Asian British',
      `Black` = 'Black or Black British'
    )
  )

# Impose reference category
df_cohort <- df_cohort %>% mutate(
    sex                   = fct_relevel(sex,                 "Female"),
    ethnicity_5cat        = fct_relevel(ethnicity_5cat,      "White"),
    urban_rural_2cat      = fct_relevel(urban_rural_2cat,    "Urban"),
    imd_2019_quintiles    = fct_relevel(imd_2019_quintiles,  "1 - Most deprived")
  )


##----------------------------##
## 1- Add age group variables ##
##----------------------------##
df_cohort <- df_cohort %>% mutate(
    age_3cat = cut(age,
                   breaks = c(5, 16, 75, Inf),
                   labels = c("5-15", "16-74", "75+"),
                   right = FALSE
    ),
    age_4cat = cut(age,
                   breaks = c(5, 12, 16, 75, Inf),
                   labels = c("5-11", "12-15", "16-74", "75+"),
                   right = FALSE
    ),
    age_17cat = cut(age,
                    breaks = c(5, 12, 16, 18, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, Inf),
                    labels = c(
                      "5-11", "12-15", "16-17", "18-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54",
                      "55-59", "60-64", "65-69", "70-74", "75-79", "80-84", "85+"
                    ),
                    right = FALSE
    )
  )

##----------------------------##
## 2- Add Q-Covid risk groups ##
##----------------------------##
# Add BMI categories and QCovid risk score
df_cohort <- df_cohort %>% mutate(
    bmi_cat = cut(
      Q_BMI,
      breaks = c(10, 18.5, 25, 30, 35, 40, Inf),
      labels = c("10-18.5", "18.5-25", "25-30", "30-35", "35-40", "40+"),
      include.lowest = TRUE,
      right = FALSE
    ),
    bmi_cat_kids = cut(
      Q_BMI,
      breaks = c(10, 18.5, 25, 30, Inf),
      labels = c("10-18.5", "18.5-25", "25-30", "30+"),
      include.lowest = TRUE,
      right = FALSE
    )
  )

# Count Qcovid items
# This part is broken down into smaller steps because R keeps crashing  when they are combined in 1 step and running on full sample
df_cohort <- df_cohort %>% mutate(
    n_risk_gps =
      (Q_DIAG_AF                == 1) +
      (Q_DIAG_PULM_RARE         == 1) +
      (Q_DIAG_ASTHMA            == 1) +
      (Q_DIAG_BLOOD_CANCER      == 1) +
      (Q_DIAG_CEREBRALPALSY     == 1) +
      (Q_DIAG_CHD               == 1) +
      (Q_DIAG_CIRRHOSIS         == 1) +
      (Q_DIAG_CONGEN_HD         == 1) +
      (Q_DIAG_COPD              == 1) +
      (Q_DIAG_DEMENTIA          == 1) +
      (Q_DIAG_EPILEPSY          == 1) +
      (Q_DIAG_FRACTURE          == 1) +
      (Q_DIAG_HIV_AIDS          == 1) +
      (Q_DIAG_IMMU              == 1) +
      (Q_DIAG_LEARNDIS          == 1) +
      (Q_DIAG_NEURO             == 1) +
      (Q_DIAG_PARKINSONS        == 1) +
      (Q_DIAG_PULM_HYPER        == 1) +
      (Q_DIAG_CCF               == 1) +
      (Q_DIAG_PVD               == 1) +
      (Q_DIAG_RA_SLE            == 1) +
      (Q_DIAG_RESP_CANCER       == 1) +
      (Q_DIAG_SEV_MENT_ILL      == 1) +
      (Q_DIAG_SICKLE_CELL       == 1) +
      (Q_DIAG_STROKE            == 1) +
      (Q_DIAG_VTE               == 1) +
      (Q_DIAG_DIABETES          == 1) +
      (Q_DIAG_CKD               == 1) +
      (Q_DIAG_IBD               == 1) +      
      (Q_MARROW                 == 1) +
      (Q_RADIO                  == 1) +
      (Q_SOLIDTRANSPLANT        == 1) +
      (Q_PRESC_STERIOD          == 1) +
      (Q_PRESC_IMMUNOSUPP       == 1) +
      (Q_LEUKO                  == 1)
  )

df_cohort <- df_cohort %>% mutate(
    qc_count_cat = case_when(
      n_risk_gps %in% 0:4 ~ str_pad(n_risk_gps, width = 2, pad = "0"),
      n_risk_gps >= 5 ~ "05plus"
      )
  )
df_cohort <- df_cohort %>% mutate(qc_count_cat = factor(qc_count_cat))
df_cohort$qc_count_cat <- relevel(df_cohort$qc_count_cat, ref = '00')

df_cohort <- df_cohort %>% mutate(
    qc_count_3cat = case_when(
      n_risk_gps %in% 0:1 ~ str_pad(n_risk_gps, width = 2, pad = "0"),
      n_risk_gps >= 2 ~ "02plus"
    )
  )
df_cohort <- df_cohort %>% mutate(qc_count_3cat = factor(qc_count_3cat))
df_cohort$qc_count_3cat <- relevel(df_cohort$qc_count_3cat, ref = '00')

##------------------------------------------------------------##
## 3- Add Vaccinations, and vaccine related derived variables ##
##------------------------------------------------------------##
## 'num_doses_recent' is already available in England dataset ##
## Including only 2 extra derived variables to England data: 'num_doses_start' and 'under_vaccinated' ##
## Vaccination types have been extracted but not included in England data as not relevant for modelling ##

# Number of doses at study_start, up to dose 5
df_cohort <- df_cohort %>% mutate(
    num_doses_start = case_when(
      date_vacc_5 <= study_start ~ 5,
      date_vacc_4 <= study_start ~ 4,
      date_vacc_3 <= study_start ~ 3,
      date_vacc_2 <= study_start ~ 2,
      date_vacc_1 <= study_start ~ 1,
      TRUE ~ 0
    )
  )

# We are now using 'under_vaccinated' rather than 'fully_vaccinated'
df_cohort <- df_cohort %>% mutate(
    under_vaccinated = case_when(
      age_4cat == "5-11" & date_vacc_1 <= study_start ~ 0,
      age_4cat == "12-15" & date_vacc_2 <= study_start ~ 0,
      age_4cat == "16-74" & date_vacc_3 <= study_start ~ 0,
      age_4cat == "75+" & date_vacc_4 <= study_start ~ 0,
      TRUE ~ 1
      )
    )
df_cohort <- df_cohort %>% mutate(under_vaccinated = factor(under_vaccinated))

##----------------------------------##
## 4- Add household characteristics ##
##----------------------------------##
## Not available for England ##

##-----------------------##
## 5- Add shielding list ##
##-----------------------##
df_cohort <- df_cohort %>% mutate(
  shielding = replace_na(shielding, 0),
  shielding = factor(shielding),
  shielding = fct_relevel(shielding, "0"),
  shielding = fct_recode(
    as.character(shielding), 
    'Never shielding' = '0',
    'Ever shielding' = '1'
  )
)

##---------------------------------------------------------------------------##
## 6- Add in time since last positive test and variant of last positive test ##
##---------------------------------------------------------------------------##
## No variant recording in England ##

df_cohort <- df_cohort %>% mutate(
    last_positive_test = as.numeric(study_start - as.Date(PCR_last_date)),
    last_positive_test_group = as.character(
      cut(last_positive_test,
          breaks = c(0, 92, 183, Inf),
          labels = c("0-13_weeks", "14-26_weeks", "27+_weeks"),
          right = FALSE
      )
    )
  )
df_cohort <- df_cohort %>% mutate(
    last_positive_test_group = ifelse(is.na(last_positive_test_group), "never_positive", last_positive_test_group),
    last_positive_test_group = factor(last_positive_test_group, c("never_positive", "0-13_weeks", "14-26_weeks", "27+_weeks"))
  )

##-----------------------------------------------------------------##
## 7 - Add number of PCR tests in last 6 months before study_start ##
##-----------------------------------------------------------------##
## Not added to England data yet but could be if needed ##

##------------------------------------------------------##
## 8- Add endpoints / Covid hospitalisations and deaths ##
##------------------------------------------------------##
## Separate hospitalisation and death endpoints are already in England dataset ##
## Those have been renamed to match the names provided in Steven's code ##
## This code is used to combine hospitalisation and death ##

# Create a list of cols where NA means 0
cols = c("covid_acoa_hosp", "covid_mcoa_hosp", "covid_mcoa_em_hosp", "covid_ac_death","covid_mc_death")
#df_cohort <- mutate_at(df_cohort, cols, ~ as.numeric(.)) # All these variables are of integer type.
#df_cohort <- mutate_at(df_cohort,cols, ~ case_when(is.na(.) ~ 0,TRUE ~ .))

# Alternative code using across()
#df_cohort <- df_cohort %>% mutate(across(all_of(cols),as.numeric))
df_cohort <- df_cohort %>% mutate(across(all_of(cols),~ifelse(is.na(.),0,.)))

# For primary/main position only
df_cohort = df_cohort %>% mutate(
    covid_mcoa_hosp_death = ifelse(covid_mcoa_hosp == 1 | covid_mc_death == 1, 1, 0),
    covid_mcoa_hosp_death_date = ifelse(covid_mcoa_hosp_death == 1, pmin(covid_mcoa_hosp_date, covid_mc_death_date, na.rm = TRUE), as.Date(NA)) %>%
      as.Date(origin = "1970-01-01")
  )

# For primary/main position only - hospitalisation as emergency method of admission
df_cohort = df_cohort %>% mutate(
    covid_mcoa_em_hosp_death = ifelse(covid_mcoa_em_hosp == 1 | covid_mc_death == 1, 1, 0),
    covid_mcoa_em_hosp_death_date = ifelse(covid_mcoa_em_hosp_death == 1, pmin(covid_mcoa_em_hosp_date, covid_mc_death_date, na.rm = TRUE), as.Date(NA)) %>%
      as.Date(origin = "1970-01-01")
  )

# For all positions
df_cohort = df_cohort %>% mutate(
    covid_acoa_hosp_death = ifelse(covid_acoa_hosp == 1 | covid_ac_death_date == 1, 1, 0),
    covid_acoa_hosp_death_date = ifelse(covid_acoa_hosp_death == 1, pmin(covid_acoa_hosp_date, covid_ac_death_date, na.rm = TRUE), as.Date(NA)) %>%
      as.Date(origin = "1970-01-01")
  )


##--------------------------------------------------------##
## 9- Add Covid and non-Covid hospitalisation covariates  ##
##--------------------------------------------------------##
## Previous covid hospitalisation not in England dataset but could be if needed ##
## non-COVID hospitalisations date available in England dataset ##


##--------------------------------------------------------------##
## 10- Re-weighting, taking into account contact with healthcare ##
##--------------------------------------------------------------##
## Weight variables based on population estimates by age-sex or age-sex-region are included in England dataset ##
## as well as contact with health services in the last 2 years and in the last 5 years ##
## The re-weighting is no longer used in Scotland nor Wales nor England ##


##---------------------------------##
## 11- Create dataset for analysis ##
##---------------------------------##
df_cohort <- data.frame(df_cohort)
df_cohort <- rename(df_cohort, individual_id = PERSON_ID)

# Check NA
sapply(df_cohort, function(x) sum(is.na(x)))

# Save out
saveRDS(df_cohort, './data/df_cohort.rds')
#saveRDS(df_cohort, './data/df_cohort_test50.rds')
#saveRDS(df_cohort, './data/df_cohort_test40.rds')
#saveRDS(df_cohort, './data/df_cohort_test10.rds')

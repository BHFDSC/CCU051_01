######################################################################
## TITLE: Descriptive for England (dataset (CCU051/COALESCE project) 
##
## Description: Create cohort description tables.
##
## Based on code developed by Steven Kerr and Stuart Bedston
##
## Copied over on: 2023-02-06
## Updated on: 2023-05-22
##
## By : Alexia Sampri
## 
######################################################################

library(dtplyr)
library(dplyr)

setwd("/mnt/efs/as3293/dars_nic_391419_j3w9t_collab/CCU051")

# source("./CCU051_01a_transfer_data.R")
# source("./CCU051_01b_transfer_data_QCovid.R")
# source("./CCU051_02_data_preparation.R")

study_start = as.Date("2022-06-01")
study_end = as.Date("2022-09-30")

# Read in
df_cohort = readRDS('./data/df_cohort.rds')

# Create output directory
output_dir <- "./output/"
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

xvars <- c("sex",
         "age_4cat",
         "age_17cat",
         "imd_2019_quintiles",
         "urban_rural_2cat",
         "ethnicity_5cat",
         "bmi_cat",
         "qc_count_cat",
         "qc_count_3cat",
         "num_doses_start",
         "under_vaccinated",
         "Contact_health_services_2y",
         "Contact_health_services_5y",
         "covid_mcoa_hosp_death",
        "covid_mcoa_em_hosp_death"

)

df <- df_cohort %>% select(age_4cat,
                           under_vaccinated,
                           Q_DIAG_AF,
                           Q_DIAG_ASTHMA,
                           Q_DIAG_BLOOD_CANCER,
                           Q_DIAG_PULM_RARE,
                           Q_DIAG_CEREBRALPALSY,
                           Q_DIAG_CHD,
                           Q_DIAG_CIRRHOSIS,
                           Q_DIAG_CONGEN_HD,
                           Q_DIAG_COPD,
                           Q_DIAG_DEMENTIA,
                           Q_DIAG_EPILEPSY,
                           Q_DIAG_LEARNDIS,
                           Q_DIAG_FRACTURE,
                           Q_DIAG_HIV_AIDS,
                           Q_DIAG_IMMU,
                           Q_DIAG_NEURO,
                           Q_DIAG_PARKINSONS,
                           Q_DIAG_PULM_HYPER,
                           Q_DIAG_PVD,
                           Q_DIAG_RA_SLE,
                           Q_DIAG_RESP_CANCER,
                           Q_DIAG_SEV_MENT_ILL,
                           Q_DIAG_SICKLE_CELL,
                           Q_DIAG_STROKE,
                           Q_DIAG_VTE,
                           Q_DIAG_DIABETES,
                           Q_DIAG_CKD,
                           Q_DIAG_IBD,
                           Q_DIAG_CCF,
                           Q_MARROW,
                           Q_RADIO,
                           Q_SOLIDTRANSPLANT,
                           Q_PRESC_STERIOD,
                           Q_PRESC_IMMUNOSUPP,
                           Q_LEUKO
                           
                           
)

df_cohort <- df_cohort %>% select(sex,
           age_4cat,
           age_17cat,
           imd_2019_quintiles,
           urban_rural_2cat,
           ethnicity_5cat,
           bmi_cat,
           qc_count_cat,
           qc_count_3cat,
           num_doses_start,
           under_vaccinated,
           Contact_health_services_2y,
           Contact_health_services_5y,
           covid_mcoa_hosp_death,
           covid_mcoa_em_hosp_death
           
)



df_cohort$num_doses_start <- as.factor(df_cohort$num_doses_start)

df_cohort$unvaccinated <-ifelse(df_cohort$num_doses_start==0,1,0)
df_cohort$unvaccinated <- as.factor(df_cohort$unvaccinated)

df_cohort$under_vaccinated <- as.factor(df_cohort$under_vaccinated)

df_cohort <- df_cohort %>% mutate(covid_mcoa_hosp_death = ifelse(is.na(covid_mcoa_hosp_death), 0, covid_mcoa_hosp_death))
df_cohort$covid_mcoa_hosp_death <- as.factor(df_cohort$covid_mcoa_hosp_death)

df_cohort <- df_cohort %>% mutate(covid_mcoa_em_hosp_death = ifelse(is.na(covid_mcoa_em_hosp_death), 0, covid_mcoa_em_hosp_death))
df_cohort$covid_mcoa_em_hosp_death <- as.factor(df_cohort$covid_mcoa_em_hosp_death)

df_cohort <- df_cohort %>% mutate(Contact_health_services_2y = ifelse(is.na(Contact_health_services_2y), 0, Contact_health_services_2y))
df_cohort$Contact_health_services_2y <- as.factor(df_cohort$Contact_health_services_2y)

df_cohort <- df_cohort %>% mutate(Contact_health_services_5y = ifelse(is.na(Contact_health_services_5y), 0, Contact_health_services_5y))
df_cohort$Contact_health_services_5y <- as.factor(df_cohort$Contact_health_services_5y)


################################
 
xvars <- c("sex",
           "age_4cat",
           "age_17cat",
           "imd_2019_quintiles",
           "urban_rural_2cat",
           "ethnicity_5cat",
           "bmi_cat",
           "qc_count_cat",
           "qc_count_3cat",
           "num_doses_start",
           "under_vaccinated"
           
)

d_sample <- df_cohort %>% select(sex,
                                  age_4cat,
                                  age_17cat,
                                  imd_2019_quintiles,
                                  urban_rural_2cat,
                                  ethnicity_5cat,
                                  bmi_cat,
                                  qc_count_cat,
                                  qc_count_3cat,
                                  num_doses_start,
                                  under_vaccinated
)

# describe =====================================================================
cat("describe\n")

describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      undervac_n = sum(under_vaccinated == "1")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      undervac_p = undervac_n / sum(undervac_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      undervac_n,
      undervac_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
d_sample <- d_sample %>% mutate(total = factor("total"))

undervac_overall_np <-
  d_sample %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_05_11_np <-
  d_sample %>%
  filter(age_4cat == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_12_15_np <-
  d_sample %>%
  filter(age_4cat == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_16_74_np <-
  d_sample %>%
  filter(age_4cat == "16-74") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_75plus_np <-
  d_sample %>%
  filter(age_4cat == "75+") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()


cat("print\n")

# save CSVs

t_undervac_overall_np <-
  undervac_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_n = round(undervac_n/5.0)*5,
    undervac_p = if_else(is.na(undervac_n), NA_real_, round(undervac_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_overall_np.csv"
  )

t_undervac_05_11_np <-
  undervac_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_n = round(undervac_n/5.0)*5,
    undervac_p = if_else(is.na(undervac_n), NA_real_, round(undervac_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_05_11_np.csv"
  )

t_undervac_12_15_np <-
  undervac_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_n = round(undervac_n/5.0)*5,
    undervac_p = if_else(is.na(undervac_n), NA_real_, round(undervac_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_12_15_np.csv"
  )

t_undervac_16_74_np <-
  undervac_16_74_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_n = round(undervac_n/5.0)*5,
    undervac_p = if_else(is.na(undervac_n), NA_real_, round(undervac_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_16_74_np.csv"
  )

t_undervac_75plus_np <-
  undervac_75plus_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_n = round(undervac_n/5.0)*5,
    undervac_p = if_else(is.na(undervac_n), NA_real_, round(undervac_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_75plus_np.csv"
  )


######    mcoa ###########

xvars <- c("sex",
           "age_4cat",
           "age_17cat",
           "imd_2019_quintiles",
           "urban_rural_2cat",
           "ethnicity_5cat",
           "bmi_cat",
           "qc_count_cat",
           "qc_count_3cat",
           "num_doses_start",
           "covid_mcoa_hosp_death"
           
)

d_sample <- df_cohort %>% select(sex,
                                 age_4cat,
                                 age_17cat,
                                 imd_2019_quintiles,
                                 urban_rural_2cat,
                                 ethnicity_5cat,
                                 bmi_cat,
                                 qc_count_cat,
                                 qc_count_3cat,
                                 num_doses_start,
                                 covid_mcoa_hosp_death
)


# describe =====================================================================
cat("describe\n")

describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      covid_mcoa_hosp_death_n = sum(covid_mcoa_hosp_death == "1")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      covid_mcoa_hosp_death_p = covid_mcoa_hosp_death_n / sum(covid_mcoa_hosp_death_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      covid_mcoa_hosp_death_n,
      covid_mcoa_hosp_death_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
d_sample <- d_sample %>% mutate(total = factor("total"))

covid_mcoa_hosp_death_overall_np <-
  d_sample %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_hosp_death_05_11_np <-
  d_sample %>%
  filter(age_4cat == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_hosp_death_12_15_np <-
  d_sample %>%
  filter(age_4cat == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_hosp_death_16_74_np <-
  d_sample %>%
  filter(age_4cat == "16-74") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_hosp_death_75plus_np <-
  d_sample %>%
  filter(age_4cat == "75+") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()


# save CSVs

t_covid_mcoa_hosp_death_overall_np <-
  covid_mcoa_hosp_death_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_hosp_death_n = round(covid_mcoa_hosp_death_n/5.0)*5,
    covid_mcoa_hosp_death_p = if_else(is.na(covid_mcoa_hosp_death_n), NA_real_, round(covid_mcoa_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_hosp_death_overall_np.csv"
  )

t_covid_mcoa_hosp_death_05_11_np <-
  covid_mcoa_hosp_death_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_hosp_death_n = round(covid_mcoa_hosp_death_n/5.0)*5,
    covid_mcoa_hosp_death_p = if_else(is.na(covid_mcoa_hosp_death_n), NA_real_, round(covid_mcoa_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_hosp_death_05_11_np.csv"
  )

t_covid_mcoa_hosp_death_12_15_np <-
  covid_mcoa_hosp_death_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_hosp_death_n = round(covid_mcoa_hosp_death_n/5.0)*5,
    covid_mcoa_hosp_death_p = if_else(is.na(covid_mcoa_hosp_death_n), NA_real_, round(covid_mcoa_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_hosp_death_12_15_np.csv"
  )

t_covid_mcoa_hosp_death_16_74_np <-
  covid_mcoa_hosp_death_16_74_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_hosp_death_n = round(covid_mcoa_hosp_death_n/5.0)*5,
    covid_mcoa_hosp_death_p = if_else(is.na(covid_mcoa_hosp_death_n), NA_real_, round(covid_mcoa_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_hosp_death_16_74_np.csv"
  )

t_covid_mcoa_hosp_death_75plus_np <-
  covid_mcoa_hosp_death_75plus_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_hosp_death_n = round(covid_mcoa_hosp_death_n/5.0)*5,
    covid_mcoa_hosp_death_p = if_else(is.na(covid_mcoa_hosp_death_n), NA_real_, round(covid_mcoa_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_hosp_death_75plus_np.csv"
  )


#########    mcoa emergency         ##########

xvars <- c("sex",
           "age_4cat",
           "age_17cat",
           "imd_2019_quintiles",
           "urban_rural_2cat",
           "ethnicity_5cat",
           "bmi_cat",
           "qc_count_cat",
           "qc_count_3cat",
           "num_doses_start",
           "covid_mcoa_em_hosp_death"
           
)

d_sample <- df_cohort %>% select(sex,
                                 age_4cat,
                                 age_17cat,
                                 imd_2019_quintiles,
                                 urban_rural_2cat,
                                 ethnicity_5cat,
                                 bmi_cat,
                                 qc_count_cat,
                                 qc_count_3cat,
                                 num_doses_start,
                                 covid_mcoa_em_hosp_death
)


# describe =====================================================================
cat("describe\n")

describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      covid_mcoa_em_hosp_death_n = sum(covid_mcoa_em_hosp_death == "1")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      covid_mcoa_em_hosp_death_p = covid_mcoa_em_hosp_death_n / sum(covid_mcoa_em_hosp_death_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      covid_mcoa_em_hosp_death_n,
      covid_mcoa_em_hosp_death_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
d_sample <- d_sample %>% mutate(total = factor("total"))

covid_mcoa_em_hosp_death_overall_np <-
  d_sample %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_em_hosp_death_05_11_np <-
  d_sample %>%
  filter(age_4cat == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_em_hosp_death_12_15_np <-
  d_sample %>%
  filter(age_4cat == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_em_hosp_death_16_74_np <-
  d_sample %>%
  filter(age_4cat == "16-74") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_mcoa_em_hosp_death_75plus_np <-
  d_sample %>%
  filter(age_4cat == "75+") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()


# save CSVs

t_covid_mcoa_em_hosp_death_overall_np <-
  covid_mcoa_em_hosp_death_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_em_hosp_death_n = round(covid_mcoa_em_hosp_death_n/5.0)*5,
    covid_mcoa_em_hosp_death_p = if_else(is.na(covid_mcoa_em_hosp_death_n), NA_real_, round(covid_mcoa_em_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_em_hosp_death_overall_np.csv"
  )

t_covid_mcoa_em_hosp_death_05_11_np <-
  covid_mcoa_em_hosp_death_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_em_hosp_death_n = round(covid_mcoa_em_hosp_death_n/5.0)*5,
    covid_mcoa_em_hosp_death_p = if_else(is.na(covid_mcoa_em_hosp_death_n), NA_real_, round(covid_mcoa_em_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_em_hosp_death_05_11_np.csv"
  )

t_covid_mcoa_em_hosp_death_12_15_np <-
  covid_mcoa_em_hosp_death_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_em_hosp_death_n = round(covid_mcoa_em_hosp_death_n/5.0)*5,
    covid_mcoa_em_hosp_death_p = if_else(is.na(covid_mcoa_em_hosp_death_n), NA_real_, round(covid_mcoa_em_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_em_hosp_death_12_15_np.csv"
  )

t_covid_mcoa_em_hosp_death_16_74_np <-
  covid_mcoa_em_hosp_death_16_74_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_em_hosp_death_n = round(covid_mcoa_em_hosp_death_n/5.0)*5,
    covid_mcoa_em_hosp_death_p = if_else(is.na(covid_mcoa_em_hosp_death_n), NA_real_, round(covid_mcoa_em_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_em_hosp_death_16_74_np.csv"
  )

t_covid_mcoa_em_hosp_death_75plus_np <-
  covid_mcoa_em_hosp_death_75plus_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_mcoa_em_hosp_death_n = round(covid_mcoa_em_hosp_death_n/5.0)*5,
    covid_mcoa_em_hosp_death_p = if_else(is.na(covid_mcoa_em_hosp_death_n), NA_real_, round(covid_mcoa_em_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_mcoa_em_hosp_death_75plus_np.csv"
  )

########  Contact_health_services_2y #########

xvars <- c("sex",
           "age_4cat",
           "age_17cat",
           "imd_2019_quintiles",
           "urban_rural_2cat",
           "ethnicity_5cat",
           "bmi_cat",
           "qc_count_cat",
           "qc_count_3cat",
           "num_doses_start",
           "Contact_health_services_2y"
           
)

d_sample <- df_cohort %>% select(sex,
                                 age_4cat,
                                 age_17cat,
                                 imd_2019_quintiles,
                                 urban_rural_2cat,
                                 ethnicity_5cat,
                                 bmi_cat,
                                 qc_count_cat,
                                 qc_count_3cat,
                                 num_doses_start,
                                 Contact_health_services_2y
)

# load =========================================================================
cat("load\n")

# describe =====================================================================
cat("describe\n")

describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      covid_health_serv_2y_hosp_death_n = sum(Contact_health_services_2y == "1")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      covid_health_serv_2y_hosp_death_p = covid_health_serv_2y_hosp_death_n / sum(covid_health_serv_2y_hosp_death_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      covid_health_serv_2y_hosp_death_n,
      covid_health_serv_2y_hosp_death_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
d_sample <- d_sample %>% mutate(total = factor("total"))

covid_health_serv_2y_hosp_death_overall_np <-
  d_sample %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_2y_hosp_death_05_11_np <-
  d_sample %>%
  filter(age_4cat == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_2y_hosp_death_12_15_np <-
  d_sample %>%
  filter(age_4cat == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_2y_hosp_death_16_74_np <-
  d_sample %>%
  filter(age_4cat == "16-74") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_2y_hosp_death_75plus_np <-
  d_sample %>%
  filter(age_4cat == "75+") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()


# save CSVs

t_covid_health_serv_2y_hosp_death_overall_np <-
  covid_health_serv_2y_hosp_death_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_2y_hosp_death_n = round(covid_health_serv_2y_hosp_death_n/5.0)*5,
    covid_health_serv_2y_hosp_death_p = if_else(is.na(covid_health_serv_2y_hosp_death_n), NA_real_, round(covid_health_serv_2y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_2y_hosp_death_overall_np.csv"
  )

t_covid_health_serv_2y_hosp_death_05_11_np <-
  covid_health_serv_2y_hosp_death_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_2y_hosp_death_n = round(covid_health_serv_2y_hosp_death_n/5.0)*5,
    covid_health_serv_2y_hosp_death_p = if_else(is.na(covid_health_serv_2y_hosp_death_n), NA_real_, round(covid_health_serv_2y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_2y_hosp_death_05_11_np.csv"
  )

t_covid_health_serv_2y_hosp_death_12_15_np <-
  covid_health_serv_2y_hosp_death_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_2y_hosp_death_n = round(covid_health_serv_2y_hosp_death_n/5.0)*5,
    covid_health_serv_2y_hosp_death_p = if_else(is.na(covid_health_serv_2y_hosp_death_n), NA_real_, round(covid_health_serv_2y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_2y_hosp_death_12_15_np.csv"
  )

t_covid_health_serv_2y_hosp_death_16_74_np <-
  covid_health_serv_2y_hosp_death_16_74_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_2y_hosp_death_n = round(covid_health_serv_2y_hosp_death_n/5.0)*5,
    covid_health_serv_2y_hosp_death_p = if_else(is.na(covid_health_serv_2y_hosp_death_n), NA_real_, round(covid_health_serv_2y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_2y_hosp_death_16_74_np.csv"
  )

t_covid_health_serv_2y_hosp_death_75plus_np <-
  covid_health_serv_2y_hosp_death_75plus_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_2y_hosp_death_n = round(covid_health_serv_2y_hosp_death_n/5.0)*5,
    covid_health_serv_2y_hosp_death_p = if_else(is.na(covid_health_serv_2y_hosp_death_n), NA_real_, round(covid_health_serv_2y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_2y_hosp_death_75plus_np.csv"
  )


######    Contact_health_services_5y ###########

xvars <- c("sex",
           "age_4cat",
           "age_17cat",
           "imd_2019_quintiles",
           "urban_rural_2cat",
           "ethnicity_5cat",
           "bmi_cat",
           "qc_count_cat",
           "qc_count_3cat",
           "num_doses_start",
           "Contact_health_services_5y"
           
)

d_sample <- df_cohort %>% select(sex,
                                 age_4cat,
                                 age_17cat,
                                 imd_2019_quintiles,
                                 urban_rural_2cat,
                                 ethnicity_5cat,
                                 bmi_cat,
                                 qc_count_cat,
                                 qc_count_3cat,
                                 num_doses_start,
                                 Contact_health_services_5y
)


d_sample$num_doses_start <- as.factor(d_sample$num_doses_start)

d_sample <- d_sample %>% mutate(covid_health_serv_5y_hosp_death = ifelse(is.na(covid_health_serv_5y_hosp_death), 0, covid_health_serv_5y_hosp_death))
d_sample$covid_health_serv_5y_hosp_death <- as.factor(d_sample$covid_health_serv_5y_hosp_death)

# describe =====================================================================
cat("describe\n")

describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      covid_health_serv_5y_hosp_death_n = sum(Contact_health_services_5y == "1")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      covid_health_serv_5y_hosp_death_p = covid_health_serv_5y_hosp_death_n / sum(covid_health_serv_5y_hosp_death_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      covid_health_serv_5y_hosp_death_n,
      covid_health_serv_5y_hosp_death_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
d_sample <- d_sample %>% mutate(total = factor("total"))

covid_health_serv_5y_hosp_death_overall_np <-
  d_sample %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_5y_hosp_death_05_11_np <-
  d_sample %>%
  filter(age_4cat == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_5y_hosp_death_12_15_np <-
  d_sample %>%
  filter(age_4cat == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_5y_hosp_death_16_74_np <-
  d_sample %>%
  filter(age_4cat == "16-74") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

covid_health_serv_5y_hosp_death_75plus_np <-
  d_sample %>%
  filter(age_4cat == "75+") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()


# save CSVs

t_covid_health_serv_5y_hosp_death_overall_np <-
  covid_health_serv_5y_hosp_death_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_5y_hosp_death_n = round(covid_health_serv_5y_hosp_death_n/5.0)*5,
    covid_health_serv_5y_hosp_death_p = if_else(is.na(covid_health_serv_5y_hosp_death_n), NA_real_, round(covid_health_serv_5y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_5y_hosp_death_overall_np.csv"
  )

t_covid_health_serv_5y_hosp_death_05_11_np <-
  covid_health_serv_5y_hosp_death_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_5y_hosp_death_n = round(covid_health_serv_5y_hosp_death_n/5.0)*5,
    covid_health_serv_5y_hosp_death_p = if_else(is.na(covid_health_serv_5y_hosp_death_n), NA_real_, round(covid_health_serv_5y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_5y_hosp_death_05_11_np.csv"
  )

t_covid_health_serv_5y_hosp_death_12_15_np <-
  covid_health_serv_5y_hosp_death_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_5y_hosp_death_n = round(covid_health_serv_5y_hosp_death_n/5.0)*5,
    covid_health_serv_5y_hosp_death_p = if_else(is.na(covid_health_serv_5y_hosp_death_n), NA_real_, round(covid_health_serv_5y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_5y_hosp_death_12_15_np.csv"
  )

t_covid_health_serv_5y_hosp_death_16_74_np <-
  covid_health_serv_5y_hosp_death_16_74_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_5y_hosp_death_n = round(covid_health_serv_5y_hosp_death_n/5.0)*5,
    covid_health_serv_5y_hosp_death_p = if_else(is.na(covid_health_serv_5y_hosp_death_n), NA_real_, round(covid_health_serv_5y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_5y_hosp_death_16_74_np.csv"
  )

t_covid_health_serv_5y_hosp_death_75plus_np <-
  covid_health_serv_5y_hosp_death_75plus_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    covid_health_serv_5y_hosp_death_n = round(covid_health_serv_5y_hosp_death_n/5.0)*5,
    covid_health_serv_5y_hosp_death_p = if_else(is.na(covid_health_serv_5y_hosp_death_n), NA_real_, round(covid_health_serv_5y_hosp_death_p, 1))
  )%>%
  write_csv(
    file = "output/t_covid_health_serv_5y_hosp_death_75plus_np.csv"
  )

########   Unvaccinated ##########
xvars <- c("sex",
           "age_4cat",
           "age_17cat",
           "imd_2019_quintiles",
           "urban_rural_2cat",
           "ethnicity_5cat",
           "bmi_cat",
           "qc_count_cat",
           "qc_count_3cat",
           "num_doses_start",
           "unvaccinated"
           
)

d_sample <- df_cohort %>% select(sex,
                                 age_4cat,
                                 age_17cat,
                                 imd_2019_quintiles,
                                 urban_rural_2cat,
                                 ethnicity_5cat,
                                 bmi_cat,
                                 qc_count_cat,
                                 qc_count_3cat,
                                 num_doses_start,
                                 unvaccinated
)


describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      unvac_n = sum(unvaccinated == "1")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      unvac_p = unvac_n / sum(unvac_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      unvac_n,
      unvac_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
d_sample <- d_sample %>% mutate(total = factor("total"))

unvac_overall_np <-
  d_sample %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

unvac_05_11_np <-
  d_sample %>%
  filter(age_4cat == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

unvac_12_15_np <-
  d_sample %>%
  filter(age_4cat == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

unvac_16_74_np <-
  d_sample %>%
  filter(age_4cat == "16-74") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

unvac_75plus_np <-
  d_sample %>%
  filter(age_4cat == "75+") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()


cat("print\n")


# save CSVs

unvac_overall_np <-
  unvac_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    unvac_n = round(unvac_n/5.0)*5,
    unvac_p = if_else(is.na(unvac_n), NA_real_, round(unvac_p, 1))
  )%>%
  write_csv(
    file = "output/t_unvac_overall_np.csv"
  )

unvac_05_11_np <-
  unvac_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    unvac_n = round(unvac_n/5.0)*5,
    unvac_p = if_else(is.na(unvac_n), NA_real_, round(unvac_p, 1))
  )%>%
  write_csv(
    file = "output/t_unvac_05_11_np.csv"
  )

unvac_12_15_np <-
  unvac_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    unvac_n = round(unvac_n/5.0)*5,
    unvac_p = if_else(is.na(unvac_n), NA_real_, round(unvac_p, 1))
  )%>%
  write_csv(
    file = "output/t_unvac_12_15_np.csv"
  )

unvac_16_74_np <-
  unvac_16_74_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    unvac_n = round(unvac_n/5.0)*5,
    unvac_p = if_else(is.na(unvac_n), NA_real_, round(unvac_p, 1))
  )%>%
  write_csv(
    file = "output/t_unvac_16_74_np.csv"
  )

unvac_75plus_np <-
  unvac_75plus_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    unvac_n = round(unvac_n/5.0)*5,
    unvac_p = if_else(is.na(unvac_n), NA_real_, round(unvac_p, 1))
  )%>%
  write_csv(
    file = "output/t_unvac_75plus_np.csv"
  )

########   Under vaccinated - QCovid ##########

xvars <- c("age_4cat",
           "Q_DIAG_AF",
           "Q_DIAG_ASTHMA",
           "Q_DIAG_BLOOD_CANCER",
           "Q_DIAG_PULM_RARE",
           "Q_DIAG_CEREBRALPALSY",
           "Q_DIAG_CHD",
           "Q_DIAG_CIRRHOSIS",
           "Q_DIAG_CONGEN_HD",
           "Q_DIAG_COPD",
           "Q_DIAG_DEMENTIA",
           "Q_DIAG_EPILEPSY",
           "Q_DIAG_LEARNDIS",
           "Q_DIAG_FRACTURE",
           "Q_DIAG_HIV_AIDS",
           "Q_DIAG_IMMU",
           "Q_DIAG_NEURO",
           "Q_DIAG_PARKINSONS",
           "Q_DIAG_PULM_HYPER",
           "Q_DIAG_PVD",
           "Q_DIAG_RA_SLE",
           "Q_DIAG_RESP_CANCER",
           "Q_DIAG_SEV_MENT_ILL",
           "Q_DIAG_SICKLE_CELL",
           "Q_DIAG_STROKE",
           "Q_DIAG_VTE",
           "Q_DIAG_DIABETES",
           "Q_DIAG_CKD",
           "Q_DIAG_IBD",
           "Q_DIAG_CCF",
           "Q_MARROW",
           "Q_RADIO",
           "Q_SOLIDTRANSPLANT",
           "Q_PRESC_STERIOD",
           "Q_PRESC_IMMUNOSUPP",
           "Q_LEUKO"
           
)


df$Q_DIAG_AF <- as.factor(df$Q_DIAG_AF)
df$Q_DIAG_ASTHMA <- as.factor(df$Q_DIAG_ASTHMA)
df$Q_DIAG_BLOOD_CANCER <- as.factor(df$Q_DIAG_BLOOD_CANCER)
df$Q_DIAG_PULM_RARE <- as.factor(df$Q_DIAG_PULM_RARE)
df$Q_DIAG_CEREBRALPALSY <- as.factor(df$Q_DIAG_CEREBRALPALSY)
df$Q_DIAG_CHD<- as.factor(df$Q_DIAG_CHD)
df$Q_DIAG_CIRRHOSIS <-as.factor(df$Q_DIAG_CIRRHOSIS)
df$Q_DIAG_CONGEN_HD <-as.factor(df$Q_DIAG_CONGEN_HD)
df$Q_DIAG_COPD <-as.factor(df$Q_DIAG_COPD)
df$Q_DIAG_DEMENTIA <-as.factor(df$Q_DIAG_DEMENTIA)
df$Q_DIAG_EPILEPSY <-as.factor(df$Q_DIAG_EPILEPSY)
df$Q_DIAG_LEARNDIS<-as.factor(df$Q_DIAG_LEARNDIS)
df$Q_DIAG_FRACTURE <-as.factor(df$Q_DIAG_FRACTURE)
df$Q_DIAG_HIV_AIDS <-as.factor(df$Q_DIAG_HIV_AIDS)
df$Q_DIAG_IMMU <-as.factor(df$Q_DIAG_IMMU)
df$Q_DIAG_NEURO <-as.factor(df$Q_DIAG_NEURO)
df$Q_DIAG_PARKINSONS <-as.factor(df$Q_DIAG_PARKINSONS)
df$Q_DIAG_PULM_HYPER <-as.factor(df$Q_DIAG_PULM_HYPER)
df$Q_DIAG_PVD <-as.factor(df$Q_DIAG_PVD)
df$Q_DIAG_RA_SLE <-as.factor(df$Q_DIAG_RA_SLE)
df$Q_DIAG_RESP_CANCER <-as.factor(df$Q_DIAG_RESP_CANCER)
df$Q_DIAG_SEV_MENT_ILL <-as.factor(df$Q_DIAG_SEV_MENT_ILL)
df$Q_DIAG_SICKLE_CELL <-as.factor(df$Q_DIAG_SICKLE_CELL)
df$Q_DIAG_STROKE <-as.factor(df$Q_DIAG_STROKE)
df$Q_DIAG_VTE <-as.factor(df$Q_DIAG_VTE)
df$Q_DIAG_DIABETES <-as.factor(df$Q_DIAG_DIABETES)
df$Q_DIAG_CKD <-as.factor(df$Q_DIAG_CKD)
df$Q_DIAG_IBD <-as.factor(df$Q_DIAG_IBD)
df$Q_DIAG_CCF <-as.factor(df$Q_DIAG_CCF)
df$Q_MARROW <-as.factor(df$Q_MARROW)
df$Q_RADIO <-as.factor(df$Q_RADIO)
df$Q_SOLIDTRANSPLANT <-as.factor(df$Q_SOLIDTRANSPLANT)
df$Q_PRESC_STERIOD <-as.factor(df$Q_PRESC_STERIOD)
df$Q_PRESC_IMMUNOSUPP <-as.factor(df$Q_PRESC_IMMUNOSUPP)
df$Q_LEUKO <-as.factor(df$Q_LEUKO)

df$under_vaccinated <- as.factor(df$under_vaccinated)

# describe =====================================================================
cat("describe\n")

describe <- function(.data, xvar) {
  .data %>%
    group_by_at(vars(one_of(xvar))) %>%
    summarise(
      total_n = n(),
      undervac_Q_n = sum(under_vaccinated == "1")
    ) %>%
    ungroup() %>%
    mutate(
      xvar = xvar,
      total_p = total_n / sum(total_n) * 100,
      undervac_Q_p = undervac_Q_n / sum(undervac_Q_n) * 100
    ) %>%
    select(
      xvar,
      xlbl = one_of(xvar),
      total_n,
      total_p,
      undervac_Q_n,
      undervac_Q_p
    )
}

# add dummy "total" column to allow easy total stats to be added to the table
df <- df %>% mutate(total = factor("total"))

undervac_Q_overall_np <-
  df %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_Q_05_11_np <-
  df %>%
  filter(age_4cat == "5-11") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_Q_12_15_np <-
  df %>%
  filter(age_4cat == "12-15") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_Q_16_74_np <-
  df %>%
  filter(age_4cat == "16-74") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()

undervac_Q_75plus_np <-
  df %>%
  filter(age_4cat == "75+") %>%
  lapply(c("total", xvars), describe, .data = .) %>%
  bind_rows()


cat("print\n")

# save CSVs

t_undervac_Q_overall_np <-
  undervac_Q_overall_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_Q_n = round(undervac_Q_n/5.0)*5,
    undervac_Q_p = if_else(is.na(undervac_Q_n), NA_real_, round(undervac_Q_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_Q_overall_np.csv"
  )

t_undervac_Q_05_11_np <-
  undervac_Q_05_11_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_Q_n = round(undervac_Q_n/5.0)*5,
    undervac_Q_p = if_else(is.na(undervac_Q_n), NA_real_, round(undervac_Q_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_Q_05_11_np.csv"
  )

t_undervac_Q_12_15_np <-
  undervac_Q_12_15_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_Q_n = round(undervac_Q_n/5.0)*5,
    undervac_Q_p = if_else(is.na(undervac_Q_n), NA_real_, round(undervac_Q_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_Q_12_15_np.csv"
  )

t_undervac_Q_16_74_np <-
  undervac_Q_16_74_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_Q_n = round(undervac_Q_n/5.0)*5,
    undervac_Q_p = if_else(is.na(undervac_Q_n), NA_real_, round(undervac_Q_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_Q_16_74_np.csv"
  )

t_undervac_Q_75plus_np <-
  undervac_Q_75plus_np %>%
  mutate(
    total_n = round(total_n/5.0)*5,
    total_p = round(total_p, 1),
    undervac_Q_n = round(undervac_Q_n/5.0)*5,
    undervac_Q_p = if_else(is.na(undervac_Q_n), NA_real_, round(undervac_Q_p, 1))
  )%>%
  write_csv(
    file = "output/t_undervac_Q_75plus_np.csv"
  )

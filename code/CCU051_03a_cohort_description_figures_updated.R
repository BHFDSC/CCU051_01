######################################################################
## TITLE: Descriptive for England (dataset (CCU051/COALESCE project) 
##
## Description: Create cohort description figures.
##
## Based on code developed by Steven Kerr steven.kerr@ed.ac.uk
##
## Copied over on: 2023-02-06
## Updated on: 2023-05-22
##
## By : Alexia Sampri
## 
######################################################################

library(tidyverse)
library(lubridate)
library(dtplyr)
library(janitor)
library(broom)
library(scales)
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

#### Vaccine uptake visualisations
# Stu's code

## Vaccinations by age group and week
d_study_weeks <- seq(
  from = floor_date(study_start, "week"),
  to   = floor_date(study_end, "week"),
  by   = "1 week"
)

d_dummy_weeks <- expand_grid(
  age_4cat = sort(unique(df_cohort$age_4cat)),
  dose_num = c("1", "2", "3", "4"),
  dose_week = d_study_weeks
)

d_vacc_week <-
  df_cohort %>%
  select(
    age_4cat,
    `1` = date_vacc_1,
    `2` = date_vacc_2,
    `3` = date_vacc_3,
    `4` = date_vacc_4
  ) %>%
  pivot_longer(
    cols           = c("1", "2", "3", "4"),
    names_to       = "dose_num",
    values_to      = "dose_date",
    values_drop_na = TRUE
  ) %>%
  mutate(dose_week = floor_date(dose_date, "week")) %>%
  filter(dose_week <= study_end) %>%
  count(age_4cat, dose_num, dose_week) %>%
  full_join(d_dummy_weeks, by = c("age_4cat", "dose_num", "dose_week")) %>%
  mutate(n = replace_na(n, as.integer(0))) %>%
  arrange(age_4cat, dose_num, dose_week)


d_vacc_week %>%
  # Suppression
  mutate(n = if_else(1 <= n & n < 10, as.integer(5), n)) %>%
  ggplot(aes(x = dose_week, y = n, colour = dose_num)) +
  facet_wrap(~age_4cat, ncol = 1, scales = "free_y") +
  geom_line() +
  xlab("") +
  ylab("") +
  labs(color = "Dose") +
  scale_y_continuous(labels = scales::comma) +
  xlab("Date") +
  ylab("Count")

ggsave(paste0(output_dir, "vacc_count_by_week_dose_age_group.png"), height = 10)


df_cohort$date_vacc_1 <- as.Date(df_cohort$date_vacc_1)
df_cohort$date_vacc_2 <- as.Date(df_cohort$date_vacc_2)
df_cohort$date_vacc_3 <- as.Date(df_cohort$date_vacc_3)
df_cohort$date_vacc_4 <- as.Date(df_cohort$date_vacc_4)
df_cohort$date_vacc_5 <- as.Date(df_cohort$date_vacc_5)
df_cohort$death_date <- as.Date(df_cohort$death_date)



## Time between vaccinations
d_vacc_diff <-
  df_cohort %>%
  select(
    individual_id,
    age_4cat,
    dose1    = date_vacc_1,
    dose2    = date_vacc_2,
    dose3    = date_vacc_3,
    dose4    = date_vacc_4
  ) %>%
  # remove events after study window
  mutate(across(
    .cols = where(is.Date),
    .fns  = ~ if_else(.x <= study_end, .x, NA_Date_)
  )) %>%
  mutate(
    # weeks between doses
    `Dose 1 to 2` = interval(dose1, dose2) / dweeks(),
    `Dose 2 to 3` = interval(dose2, dose3) / dweeks(),
    `Dose 3 to 4` = interval(dose3, dose4) / dweeks(),
    # round off
    `Dose 1 to 2` = floor(`Dose 1 to 2`),
    `Dose 2 to 3` = floor(`Dose 2 to 3`),
    `Dose 3 to 4` = floor(`Dose 3 to 4`)
  ) %>%
  select(
    age_4cat,
    `Dose 1 to 2`,
    `Dose 2 to 3`,
    `Dose 3 to 4`
  ) %>%
  pivot_longer(
    cols           = -age_4cat,
    names_to       = "dose_to_dose",
    values_to      = "diff_weeks",
    values_drop_na = TRUE
  ) %>%
  count(age_4cat, dose_to_dose, diff_weeks)

d_vacc_diff %>%
  # Suppression
  mutate(n = if_else(1 <= n & n < 10, as.integer(5), n)) %>%
  ggplot(aes(
    x = diff_weeks,
    y = n,
  )) +
  facet_grid(age_4cat ~ dose_to_dose, scales = "free") +
  geom_col() +
  scale_y_continuous(labels = scales::comma) +
  xlab("Weeks") +
  ylab("Count")

ggsave(paste0(output_dir, "time_between_doses.png"), height = 10, width = 10)


### Uptake cumulative incidence

date_vacc_start <- as.Date("2020-12-08")


df <- df_cohort %>%
   select(individual_id, age_4cat, date_vacc_1, date_vacc_2, date_vacc_3, date_vacc_4, date_vacc_5, death_date)

df <- df %>%
  mutate(surv_date = pmin(study_end, death_date, na.rm = TRUE))

df <- df %>%
  mutate(surv_time = as.numeric(surv_date - (date_vacc_start)))

df <- df %>%
  mutate(event = 0) %>%
  # Some people died before study start and will therefore have negative survival time. Remove them.
  filter(surv_time >= 0) %>%
  # If they had an event on day of vaccination, assume that the event happened shortly after vaccination
  mutate(surv_time = ifelse(surv_time == 0, 0.001, surv_time))


df <- df_cohort %>%
  select(individual_id, age_4cat, date_vacc_1, date_vacc_2, date_vacc_3, date_vacc_4, date_vacc_5, death_date) %>%
  mutate(surv_date = pmin(study_end, death_date, na.rm = TRUE)) %>%
  mutate(surv_time = as.numeric(surv_date - (date_vacc_start))) %>%
  mutate(event = 0) %>%
  # Some people died before study start and will therefore have negative survival time. Remove them.
  filter(surv_time >= 0) %>%
  # If they had an event on day of vaccination, assume that the event happened shortly after vaccination
  mutate(surv_time = ifelse(surv_time == 0, 0.001, surv_time))
# 
df <- tmerge(df, df, id = individual_id, event = event(surv_time, event))


# dataframe of start times for different vaccination status

df_vs <- df %>%
  mutate(
    Unvaccinated = 0,
    `Dose 1` = as.numeric(date_vacc_1 - date_vacc_start),
    `Dose 2` = as.numeric(date_vacc_2 - date_vacc_start),
    `Dose 3` = as.numeric(date_vacc_3 - date_vacc_start),
    `Dose 4` = as.numeric(date_vacc_4 - date_vacc_start),
    `Dose 5` = as.numeric(date_vacc_5 - date_vacc_start),
    Death = as.numeric(death_date - date_vacc_start)
  ) %>%
  select(-date_vacc_1, -date_vacc_2, -date_vacc_3, -date_vacc_4, -date_vacc_5)

df_vs <- pivot_longer(df_vs,
                      cols = c("Unvaccinated", "Dose 1", "Dose 2", "Dose 3", "Dose 4", "Dose 5", "Death"),
                      names_to = "vs", values_to = "time"
) %>%
  filter(!is.na(time))



# Add in vaccination status as a time dependent variable
df <- tmerge(df, df_vs, id = individual_id, dose_event = event(time, vs)) %>%
  mutate(dose_event = factor(dose_event))

# Cumulative incidence curve
plot <- survfit(
  Surv(tstart, tstop, dose_event) ~ age_4cat,
  data = df,
  id = individual_id
) %>%
  tidy() %>%
  mutate(
    strata = strata %>%
      str_replace("age_4cat=", "") %>%
      factor() %>%
      fct_relevel("5-11", "12-15", "16-74", "75+"),
    State = state %>%
      factor() %>%
      fct_recode("Unvaccinated" = "(s0)") %>%
      fct_relevel("Death")
  ) %>%
  # Convert time to date
  mutate(
    time = date_vacc_start + time
  ) %>%
  ggplot(aes(
    x = time,
    y = estimate,
    fill = State
  )) +
  facet_wrap(~strata, ncol = 1) +
  geom_area() +
  scale_fill_brewer(type = "qual") +
  ylab("Cumulative incidence") +
  xlab("Day")

ggsave(paste0(output_dir, "uptake_cumulative_incidence.png"), plot, height = 10)


#Stu's code

lkp_events = c(
  "Unvaccinated",
  "Dose 1",
  "Dose 2",
  "Dose 3",
  "Dose 4",
  "Death",
  "study_end"
)

# make multi-state date set for dose

d_mstate_vacc =
  df_vs %>%
  mutate(
    Death = death_date,
    study_end = study_end
  ) %>%
  select(
    individual_id,
    age_4cat,
    `Dose 1`    = date_vacc_1,
    `Dose 2`    = date_vacc_2,
    `Dose 3`    = date_vacc_3,
    `Dose 4`    = date_vacc_4,
    Death,
    study_end
  ) %>%
  # any first day events just add half a day
  mutate(across(
    .cols = where(is.Date),
    .fns  = ~ if_else(.x == study_start, .x + ddays(0.5), as_datetime(.x))
  )) %>%
  # remove events after someone has either moved out, died, or reached the
  # end of the study window
  mutate(
    end_follow_up = pmin(Death, study_end, na.rm = TRUE)
  ) %>%
  mutate(across(
    .cols = where(is.POSIXct),
    .fns  = ~ if_else(.x <= end_follow_up, .x, NA_POSIXct_)
  )) %>%
  select(-end_follow_up) %>%
  pivot_longer(
    cols           = c(-individual_id, -age_4cat),
    names_to       = "event_name",
    values_to      = "event_date",
    values_drop_na = TRUE
  ) %>%
  # add row number for alf
  lazy_dt() %>%
  arrange(individual_id, event_date) %>%
  group_by(individual_id) %>%
  mutate(alf_seq = row_number()) %>%
  as_tibble() %>%
  # define survival columns
  mutate(
    tstart     = if_else(alf_seq == 1, as_datetime(study_start), lag(event_date)),
    tstart     = interval(as_datetime(study_start), tstart) / ddays(),
    state_from = if_else(alf_seq == 1, "Unvaccinated", lag(event_name)),
    state_from = factor(state_from, lkp_events) %>% fct_drop(),
    tstop      = event_date,
    tstop      = interval(as_datetime(study_start), tstop) / ddays(),
    state_to   = factor(event_name, lkp_events) %>% fct_drop()
  ) %>%
  select(
    -event_name,
    -event_date
  ) %>%
  # anyone who has event on last day, remove row for that transition from event to study end
  filter(!(tstart > 0 & tstart == tstop & state_to == "study_end")) %>%
  # any same day events that go from dose to death / move out just add half a day again
  mutate(
    tstop = if_else(
      condition = state_from %in% c("Dose 1", "Dose 2", "Dose 3", "Dose 4") &
        state_to  == "Death" &
        tstart == tstop,
      true = tstop + 0.5,
      false = tstop
    )
  ) %>%
  # finalise censored category
  mutate(
    state_to = state_to %>%
      fct_collapse("(censored)" = "study_end") %>%
      fct_relevel("(censored)")
  )

# explore
d_mstate_vacc %>% tabyl(state_from, state_to)

# fit mstate
mstate_vacc = survfit(
  formula = Surv(tstart, tstop, state_to) ~ age_4cat,
  data = d_mstate_vacc,
  id = individual_id
)

mstate_vacc %>%
  tidy() %>%
  mutate(
    strata = strata %>%
      str_replace("age_4cat=", "") %>%
      factor() %>%
      fct_relevel("5-11", "12-15", "16-74", "75+"),
    State = state %>%
      factor() %>%
      fct_recode("Unvaccinated" = "(s0)") %>%
      fct_relevel("Death")
  ) %>%
  # Convert time to date
  mutate(
  time = study_start + time
  ) %>%
  ggplot(aes(
    x = time,
    y = estimate,
    fill = State
  )) +
  facet_wrap(~strata, ncol = 1) +
  geom_area() +
  scale_fill_brewer( type = "qual") +
  ylab('Cumulative incidence') +
  xlab('Day')

ggsave(paste0(output_dir, 'uptake_cumulative_incidence.png'), height = 10)
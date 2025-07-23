# Supply of Urogynecologists ----
# setup ----
library(dplyr)
library(tidyverse)
library(lubridate)
library(readxl)
library(dplyr)
library(tidyr)
library(ggplot2)
# devtools::install_github("e-mitchell/meps_r_pkg/MEPS")
library(MEPS)
library(survey)
library(haven)    # if reading .sav
library(janitor)
library(labelled)
library(tidycensus)
library(nhanesA)

# Data Sources:
# Medical Expenditure Panel Survey (MEPS)
# Behavioral Risk Factor Surveillance System (BRFSS)
# National Health and Nutrition Examination Survey (NHANES)
# Study of Women’s Health Across the Nation (SWAN)
# American Community Survey (ACS) Microdata and Public Use Microdata Sample (PUMS)

# Constants ----
initial_supply <- 1169
new_fellows_per_year <- 55
retirements_per_year <- 55


# Parameters -----
years <- 2022:2100
initial_supply <- 1169
hours_per_provider <- 36 * 48  # 1,728 hours/year

simulate_supply <- function(years, new_per_year, retire_per_year, initial_supply, scenario_label) {
  supply <- numeric(length(years))
  supply[1] <- initial_supply

  for (i in 2:length(years)) {
    supply[i] <- supply[i - 1] + new_per_year - retire_per_year
  }

  tibble(
    Year = years,
    Supply = supply * hours_per_provider,  # convert to total hours
    Scenario = scenario_label
  ) %>%
    mutate(Supply = Supply / hours_per_provider)  # convert back to FTEs
}

# Create scenarios
supply_scenario_df <- bind_rows(
  simulate_supply(years, 55, 55, initial_supply, "Status Quo"),
  simulate_supply(years, 61, 55, initial_supply, "Train 10% More"),
  simulate_supply(years, 55, 40, initial_supply, "Retire 2 Years Later"),
  simulate_supply(years, 55, 70, initial_supply, "Retire 2 Years Earlier")
)


# Example state-level population input (you'll substitute with real data)
state_pop <- tibble(
  State = c("CA", "TX", "NY"),
  Women_65_plus = c(3000000, 2000000, 1500000)
)

simulate_demand <- function(population_over_65, visits_per_person_per_year = 1.5) {
  total_visits <- sum(population_over_65$Women_65_plus) * visits_per_person_per_year
  required_fte <- total_visits / (hours_per_provider * 48)  # Assuming 48 working weeks
  return(required_fte)
}

status_quo_demand <- simulate_demand(state_pop)

bind_rows(
  lapply(names(supply_scenarios), function(name) {
    supply_scenarios[[name]] %>%
      mutate(Scenario = name)
  })
) %>%
  ggplot(aes(x = Year, y = Urogynecologist_FTEs, color = Scenario)) +
  geom_line(size = 1.2) +
  geom_hline(yintercept = status_quo_demand, linetype = "dashed", color = "black") +
  labs(title = "Projected Urogynecology Workforce Supply",
       subtitle = "Compared to estimated demand",
       y = "Full-Time Equivalent Urogynecologists") +
  theme_minimal() + annotate("text", x = 2040, y = 5600, label = "Estimated Demand", hjust = 1, vjust = -0.5)


# Path to the Excel file
census_path <- "/Users/tylermuffly/Dropbox (Personal)/tyler/data-raw/np2023-t2.xlsx"

# Step 1: Read the 'Main series (thousands)' sheet, skipping the header rows
raw <- read_excel(census_path, sheet = "Main series (thousands)", skip = 3)

# Step 2: Rename columns using the second row as header values
names(raw) <- c("Age_Group", as.character(as.numeric(raw[2, -1])))
raw <- raw[-c(1:2), ]  # remove metadata/header rows

# Step 3: Keep only rows for ".65 years and over"
raw_filtered <- raw %>%
  filter(Age_Group == ".65 years and over")

# Step 4: Convert from wide to long format and to actual population
demand_df <- raw_filtered %>%
  pivot_longer(-Age_Group, names_to = "Year", values_to = "Population_thousands") %>%
  mutate(
    Year = as.integer(Year),
    Population_65plus = as.numeric(Population_thousands) * 1000
  ) %>%
  select(Year, Population_65plus)

# Step 5: Calculate demand based on visit rate and provider hours
visits_per_woman_per_year <- 1.5
hours_per_provider_per_year <- 36 * 48  # 1,728 hours/year

demand_df <- demand_df %>%
  mutate(
    Total_Visits = Population_65plus * visits_per_woman_per_year,
    Urogynecology_Demand_FTEs = Total_Visits / hours_per_provider_per_year
  )

# Step 6: Plot the projected demand
ggplot(demand_df, aes(x = Year, y = Urogynecology_Demand_FTEs)) +
  geom_point(color = "darkred", alpha = 0.4) +
  geom_smooth(method = "loess", span = 0.3, se = FALSE, color = "firebrick", linewidth = 1.4) +
  labs(
    title = "Smoothed Demand for Urogynecologists (U.S. Women Aged 65+)",
    y = "Full-Time Equivalent Urogynecologists",
    x = "Year"
  ) +
  theme_minimal()


ggplot() +
  geom_line(data = demand_df, aes(x = Year, y = Urogynecology_Demand_FTEs),
            color = "darkred", size = 1.2) +
  geom_line(data = supply_scenario_df, aes(x = Year, y = Supply, color = Scenario)) +
  labs(title = "Projected Urogynecology Supply and Demand", y = "FTE Urogynecologists")

####
demand_cleaned <- demand_df %>%
  group_by(Year) %>%
  summarise(Urogynecology_Demand_FTEs = sum(Urogynecology_Demand_FTEs, na.rm = TRUE))


# Merge demand and supply into one dataset
combined_df <- left_join(supply_scenario_df, demand_cleaned, by = "Year") %>%
  mutate(Gap = Supply - Urogynecology_Demand_FTEs)

combined_df_clean <- combined_df %>% filter(!is.na(Gap))

# Plot gap
ggplot(combined_df_clean, aes(x = Year, y = Gap, color = Scenario)) +
  geom_line(size = 1.2) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(
    title = "Projected Supply Gap for Urogynecologists",
    y = "FTE Urogynecologist Surplus / Deficit",
    x = "Year"
  ) +
  theme_minimal() +
  scale_x_continuous(breaks = seq(2025, 2100, by = 5))



## === Load national projections from Census Excel file ----
census_path <- "/Users/tylermuffly/Dropbox (Personal)/tyler/data-raw/np2023-t2.xlsx"

raw <- read_excel(census_path, sheet = "Main series (thousands)", skip = 3)
names(raw) <- c("Age_Group", as.character(as.numeric(raw[2, -1])))
raw <- raw[-c(1:2), ]

# Extract national total for women 65+ population
national_pop_65plus <- raw %>%
  filter(Age_Group == ".65 years and over") %>%
  pivot_longer(-Age_Group, names_to = "Year", values_to = "Population_thousands") %>%
  mutate(
    Year = as.integer(Year),
    Population_65plus = as.numeric(Population_thousands) * 1000
  ) %>%
  select(Year, Population_65plus)

# === Calculate National Demand ===
visits_per_woman_per_year <- 1.5
hours_per_provider_per_year <- 36 * 48  # 1,728 hours/year

demand_df <- national_pop_65plus %>%
  mutate(
    Total_Visits = Population_65plus * visits_per_woman_per_year,
    Urogynecology_Demand_FTEs = Total_Visits / hours_per_provider_per_year
  )

# === Simulate Supply Scenarios ===
years <- demand_df$Year
initial_supply <- 1169

simulate_supply <- function(years, new_per_year, retire_per_year, initial_supply, scenario_label) {
  supply <- numeric(length(years))
  supply[1] <- initial_supply
  for (i in 2:length(years)) {
    supply[i] <- supply[i - 1] + new_per_year - retire_per_year
  }
  tibble(Year = years, Urogynecologist_FTEs = supply, Scenario = scenario_label)
}

supply_scenarios <- bind_rows(
  simulate_supply(years, 55, 55, initial_supply, "Status Quo"),
  simulate_supply(years, 61, 55, initial_supply, "Train 10% More"),
  simulate_supply(years, 55, 40, initial_supply, "Retire 2 Years Later"),
  simulate_supply(years, 55, 70, initial_supply, "Retire 2 Years Earlier")
)

# === Join Demand and Plot ===
# Optionally pick a fixed demand year for static comparison line
status_quo_demand_2040 <- demand_df %>% filter(Year == 2040) %>% pull(Urogynecology_Demand_FTEs)

demand_df_filtered <- demand_df %>%
  filter(Year %% 5 == 0) %>%
  group_by(Year) %>%
  summarise(
    Population_65plus = sum(Population_65plus, na.rm = TRUE)
  ) %>%
  mutate(
    Total_Visits = Population_65plus * visits_per_woman_per_year,
    Urogynecology_Demand_FTEs = Total_Visits / hours_per_provider_per_year
  )


ggplot(supply_scenarios, aes(x = Year, y = Urogynecologist_FTEs, color = Scenario)) +
  geom_line(size = 1.2) +
  geom_line(data = demand_df_filtered, aes(x = Year, y = Urogynecology_Demand_FTEs),
            color = "darkred", linewidth = 1.2, inherit.aes = FALSE) +
  geom_hline(yintercept = demand_df_filtered %>% filter(Year == 2040) %>% pull(Urogynecology_Demand_FTEs),
             linetype = "dashed", color = "black") +
  annotate("text", x = 2040, y = demand_df_filtered %>% filter(Year == 2040) %>% pull(Urogynecology_Demand_FTEs) + 500,
           label = "Estimated Demand (2040)", hjust = 1) +
  labs(
    title = "Projected Urogynecology Workforce Supply",
    subtitle = "Compared to estimated national demand (every 5 years)",
    y = "Full-Time Equivalent Urogynecologists"
  ) +
  theme_minimal() +
  scale_x_continuous(breaks = seq(2025, 2100, by = 5))


# Medical Group Management Association (MGMA) reports are paywalled but:
# Anecdotally report 2,000–2,500 visits/year for GYN subspecialists
visits_per_fte <- 2600  # conservative midpoint
required_fte <- total_urogynecology_visits / visits_per_fte


# Demand: MEPS  Medical Expenditure Panel Survey ----

# Load Office-Based Medical Provider Visits File (OBV)
obv <- read_MEPS(year = 2021, type = "OB")

# Load Full-Year Consolidated File (FY)
fy <- read_MEPS(year = 2021, type = "FYC")

# This is the Office-Based Medical Provider Visits (OBV) file from MEPS for a given year (e.g., 2021). Each row is a medical visit.  This joins the Full-Year Consolidated (FY) file to obv by DUPERSID, which is the unique person identifier in MEPS.  The OBV file contains event-level data (each row is a visit).  The FY file contains person-level data (age, sex, insurance, weights, etc.). So this adds demographic and health info (like SEX, AGELAST) to each visit.

meps_combined <- obv %>%
  left_join(fy, by = "DUPERSID") %>%
  filter(SEX == 2, AGELAST >= 18)
# 112,053 Office-Based Medical Provider Visits for UI in women over 18 years old.

#names(obv)[str_detect(names(obv), "ICD|DX")]
# This confirms your MEPS OBV file does not include any direct ICD diagnosis fields like ICD10CDX1. This is expected: MEPS stores condition diagnoses in separate files, and you must link them using EVNTIDX and DUPERSID.

# Load COND file
cond <- read_MEPS(year = 2021, type = "COND")
ui_conditions <- cond %>%
  filter(ICD10CDX == "N39")

#779 rows where ICD10CDX == "N39" (disorders of the urinary system, which includes incontinence).  Not filtered to female and not filtered to age.

# All 779 entries with ICD10CDX == "N39" include all types of N39.x, including incontinence (like N39.3, N39.4).  MEPS likely uses three-character truncated codes in public releases to protect confidentiality.  This is a conditions-level file with:

# Load Condition-Event Link file.  Link condition to events.
clnk <- read_MEPS(year = 2021, type = "CLNK")

# Step A: Link conditions to events
ui_event_links <- ui_conditions %>%
  inner_join(clnk, by = c("DUPERSID", "CONDIDX"))

# Step B: Link to office visits
obv_ui <- ui_event_links %>%
  inner_join(obv, by = c("DUPERSID", "EVNTIDX"))

# Step 4: Link person-level data (age, sex)
obv_ui_combined <- obv_ui %>%
  left_join(fy, by = "DUPERSID") %>%
  filter(SEX == 2, AGELAST >= 18)
# 605 women > 18 yo who have incontinence in an office-based medical setting

table(as_factor(obv_ui$DRSPLTY_M18), useNA = "ifany")

# Did not run this because my goal is To study all women with urinary incontinence who received office-based care, not just those treated by OB/GYNs or urologists.
all_ui_visits <- obv_ui_combined %>%
  filter(DRSPLTY_M18 != 24) #Remove pediatricians
# 604 women > 18 yo

rm(clnk)
rm(fy)
rm(obv)
rm(cond)
rm(meps_combined)
invisible(gc())

# ✅ FULL UPDATED PIPELINE
# Step 0: The dummy variable count = 1 is a simple trick used in survey analysis to count the number of visits.  Setting count = 1 lets us use svytotal(~count, design) to compute the weighted total number of office visits in the U.S. that match your inclusion criteria.
all_ui_visits <- all_ui_visits %>%
  mutate(count = 1)

# Step 1: Create survey design
meps_design <- svydesign(
  id = ~VARPSU.x,
  strata = ~VARSTR.x,
  weights = ~PERWT21F.x,
  data = all_ui_visits,
  nest = TRUE
)

options(survey.lonely.psu = "adjust")  # prevent errors with single-PSU strata

# Step 2: Estimate total office-based visits for urinary incontinence
svytotal(~count, meps_design)

## 🧮 Convert Total Visits to Estimated FTEs ----
# Use a general outpatient productivity assumption
# Typical OB/GYN outpatient productivity
# This includes all provider specialties, not just OB/GYN or urology.
# The productivity benchmark (2,600 visits/year) assumes a full-time outpatient provider.
visits_per_fte <- 2600
total_ui_visits <- svytotal(~count, meps_design)[1]
fte_estimate <- as.numeric(total_ui_visits) / visits_per_fte
fte_estimate
# 2254 FTEs needed to take care of UI in office settings

## 📊 Compare Visits by Age Group ----
# Bin patient ages into groups
all_ui_visits <- all_ui_visits %>%
  mutate(age_group = case_when(
    AGELAST < 18 ~ "<18",
    AGELAST < 35 ~ "18–34",
    AGELAST < 50 ~ "35–49",
    AGELAST < 65 ~ "50–64",
    TRUE ~ "65+"
  ))

# Rebuild survey design with age groups
meps_design <- svydesign(
  id = ~VARPSU.x,
  strata = ~VARSTR.x,
  weights = ~PERWT21F.x,
  data = all_ui_visits,
  nest = TRUE
)
# Estimate visits per age group
svyby(~count, ~age_group, meps_design, svytotal)

# Estimate FTEs for 65+ OB/GYN visits
fte_for_65plus <- 2908655 / 2600  # using 2600 visits/FTE/year
fte_for_65plus
# ≈ 1,118 FTEs needed nationally to serve 65+ population in office-based OB/GYN care

## Compare Visits by Insurance Type ----

table(all_ui_visits$INSC1231, useNA = "ifany") # Check values first


# Label insurance types clearly
all_ui_visits <- all_ui_visits %>%
  mutate(insurance = case_when(
    INSC1231 == 1 ~ "Any Private",
    INSC1231 == 2 ~ "Public Only",
    INSC1231 == 3 ~ "Uninsured",
    is.na(INSC1231) | INSC1231 %in% c(-7, -8, -9) ~ "Missing",
    TRUE ~ "Other"
  ))

# Rebuild survey design
meps_design <- svydesign(
  id = ~VARPSU.x,
  strata = ~VARSTR.x,
  weights = ~PERWT21F.x,
  data = all_ui_visits,
  nest = TRUE
)

# Estimate total visits by insurance type
svyby(~count, ~insurance, meps_design, svytotal)
# Among women age ≥18 with a urinary incontinence diagnosis, seen in office-based visits:About 5.76 million visits were by women with private insurance.  About 99,000 visits were by women with public-only insurance.  The standard error is large for public-only visits, reflecting the small sample size in MEPS.

# 1. Estimate total visits by insurance type
visit_estimates <- svyby(~count, ~insurance, meps_design, svytotal)

# 2. Estimate total number of visits across all insurance groups
total_visits <- sum(visit_estimates$count, na.rm = TRUE)

# 3. Add percent column
visit_estimates <- visit_estimates %>%
  mutate(
    percent = round(100 * count / total_visits, 1)
  )

# 4. View result
visit_estimates


## ✅ 1. Who’s Providing the Care — Specialty Distribution ----
# Add count for each visit
all_ui_visits <- all_ui_visits %>%
  mutate(count = 1)

# Survey design
meps_design <- svydesign(
  id = ~VARPSU.x,
  strata = ~VARSTR.x,
  weights = ~PERWT21F.x,
  data = all_ui_visits,
  nest = TRUE
)

options(survey.lonely.psu = "adjust")

specialty_map <- c(
  "6"  = "Family Practice",
  "8"  = "General Practice",
  "11" = "Ob/Gyn",
  "14" = "Internal Medicine",
  "20" = "Orthopedics",
  "24" = "Pediatrics",
  "28" = "Psychiatry",
  "29" = "Pulmonology",
  "30" = "Radiology",
  "33" = "Urology",
  "91" = "Other Specialty"
)

# Step-by-step recode with no warnings
all_ui_visits <- all_ui_visits %>%
  mutate(
    specialty_raw = as.character(DRSPLTY_M18),
    specialty = recode(specialty_raw, !!!specialty_map, .default = "Unknown")
  )

# Rebuild design with new specialty variable
meps_design <- update(meps_design, specialty = all_ui_visits$specialty)

# Estimate weighted visit totals by specialty
specialty_distribution <- svyby(~count, ~specialty, meps_design, svytotal, na.rm = TRUE) %>%
  arrange(desc(count)) %>%
  mutate(percent = round(100 * count / sum(count), 1))

specialty_distribution
# About 1 in 3 UI visits are to providers whose specialty is not captured (“Unknown”) — this is common in MEPS and not your fault.  Primary care (general + family practice) provides the majority of UI care.Urologists and Ob/Gyns make up a minority of visits, underscoring how many women with UI are treated outside specialty care.


# Demand: brfss ----
### Dall et al. used a nationally representative sample (like MEPS, BRFSS, or ACS PUMS) to simulate each woman’s risk and demand.
# Construct a probability model for demand

# ---- 1. Load Dataa
# Replace path as needed
# brfss <- haven::read_xpt("LLCP2021.XPT")
#
# brfss <- haven::read_xpt("/Users/tylermuffly/Dropbox (Personal)/tyler/LLCP2021.XPT")
#brfss <- haven::read_xpt(file.choose()). #JESUS H CHRIST
# readr::write_rds(brfss, "brfss.rds")
brfss <- readr::read_rds("brfss.rds")

names(brfss)[str_detect(names(brfss), "SEX|sex|gender|GENDER")]
names(brfss)[str_detect(names(brfss), "AGE|age|years")]
names(brfss)[str_detect(names(brfss), "DIAB|diab|SUGAR|sugar")]
names(brfss)[str_detect(names(brfss), "DEPR|depr|MENT|ment|MOOD")]


# ---- Filter to Women Age 18+
brfss_clean <- brfss %>%
  filter(`_SEX` == 2, `_AGE80` >= 18)

# ---- Recode Key Risk Factors
brfss_clean <- brfss_clean %>%
  mutate(
    obesity = case_when(
      `_BMI5CAT` == 4 ~ 1,  # Obese category
      TRUE ~ 0
    ),
    diabetes = case_when(
      DIABETE4 %in% c(1, 2) ~ 1,  # DIABETE4 is the core diabetes question:
      # 1 = Yes
      # 2 = Yes, but only during pregnancy
      # 3 = No
      # 4 = Borderline/Pre-diabetes
      # 7 = Don’t know, 9 = Refused
      DIABETE4 %in% c(3, 4, 7, 9) ~ 0,  # 3 = No, 4 = Borderline
      TRUE ~ NA_real_
    ),
    depression = case_when(
      `_MENT14D` == 1 ~ 1,   # _MENT14D = "Poor mental health ≥14 days in past 30 days" (derived binary: 1 = Yes, 2 = No)
      `_MENT14D` == 2 ~ 0,
      TRUE ~ NA_real_
    )
  )

# Set up survey design object
# This will return estimated prevalence and standard errors for adult women 18+.
brfss_design <- svydesign(
  ids = ~1,
  weights = ~`_LLCPWT`,
  data = brfss_clean
)


# Weighted prevalence estimates
svymean(~obesity + diabetes + depression, brfss_design, na.rm = TRUE)

names(brfss)[str_detect(names(brfss), "X_STATE|STATE|state|x_state")]

# Step 2: Simulate Demand Across States or ZIP Codes
# Example: state-level demand by diabetes status
state_demand <- svyby(
  ~diabetes,
  ~`_STATE`,  # state FIPS code
  brfss_design,
  svymean,
  na.rm = TRUE
)

# now let's merge this state-level diabetes prevalence with ACS state-level population counts (e.g., women 18+) to simulate total demand.
#install.packages("tidycensus")

# Set your Census API key first if you haven't already
# census_api_key("YOUR_KEY", install = TRUE)

# Get 2021 ACS population by state for women age 18+
acs_pop <- get_acs(
  geography = "state",
  variables = "B01001_025",  # Women age 18+
  year = 2021,
  survey = "acs1"
) %>%
  select(GEOID, NAME, estimate) %>%
  rename(state_fips = GEOID, state_name = NAME, women18plus = estimate) %>%
  mutate(state_fips = as.numeric(state_fips))

acs_pop <- acs_pop %>%
  mutate(women18plus = women18plus * 100)


acs_pop %>% filter(state_name %in% c("California", "Texas", "New York", "Florida", "Alaska"))


state_demand_joined <- state_demand %>%
  rename(state_fips = `_STATE`) %>%
  left_join(acs_pop, by = "state_fips") %>%
  mutate(
    est_women_with_diabetes = diabetes * women18plus
  )

state_demand_joined %>%
  select(state_name, diabetes, women18plus, est_women_with_diabetes) %>%
  arrange(desc(est_women_with_diabetes)) %>%
  head(10)


# Demand: NHANES ----

# Download datasets for 2011–2012
demo <- nhanes("DEMO_G")
demo <- demo %>%
  mutate(RIAGENDR = as.numeric(RIAGENDR))
table(demo$RIAGENDR)
demo %>%
  filter(RIAGENDR == 2) %>%
  summarise(n_females = n())  # Should return ~4900

repro <- nhanes("RHQ_G")
bmx <- nhanes("BMX_G")
uro <- nhanes("KIQ_U_G")  # Incontinence

# Merge
nhanes_combined <- demo %>%
  left_join(repro, by = "SEQN") %>%
  left_join(bmx,   by = "SEQN") %>%
  left_join(uro,   by = "SEQN")

nhanes_clean <- nhanes_combined %>%
  filter(RIAGENDR == 2) %>%  # Female
  mutate(
    age = RIDAGEYR,
    race = case_when(
      RIDRETH1 %in% c("Mexican American", "Other Hispanic") ~ "Hispanic",
      RIDRETH1 == "Non-Hispanic White" ~ "White",
      RIDRETH1 == "Non-Hispanic Black" ~ "Black",
      RIDRETH1 == "Non-Hispanic Asian" ~ "Asian",
      TRUE ~ "Other"
    ),
    hysterectomy = if_else(RHQ031 == "Yes", 1, 0, missing = NA_real_),
    parity = as.numeric(RHQ540),
    BMI = BMXBMI,
    incontinence = if_else(KIQ026 == "Yes", 1, 0, missing = NA_real_),
    wt = WTMEC2YR,
    strata = SDMVSTRA,
    psu = SDMVPSU
  ) %>%
  filter(
    !is.na(incontinence),
    !is.na(parity),
    !is.na(hysterectomy),
    !is.na(BMI),
    !is.na(age)
  )

nhanes_design <- svydesign(
  id = ~psu,
  strata = ~strata,
  weights = ~wt,
  data = nhanes_clean,
  nest = TRUE
)


model <- svyglm(
  incontinence ~ age + BMI + parity + hysterectomy + race,
  design = nhanes_design,
  family = quasibinomial()
)

summary(model)

# ✅ Significant Predictors
# BMI: Each unit increase in BMI is associated with higher odds of incontinence (OR ≈ exp(0.0194) = 1.02, p = 0.042).
#
# Race: Hispanic vs. Black: Hispanic women have significantly higher odds of incontinence compared to non-Hispanic Black women (reference group).
#
# Race: White vs. Black: Similarly, White women also have higher odds compared to Black women.
#
# ❌ Not Significant
# Age: Not statistically associated after accounting for other variables.
#
# Parity: No significant association — this may be due to recall issues, misclassification, or lack of granularity in the parity variable.
#
# Hysterectomy: Surprisingly not significant — but this could be due to limited power or lack of detail on timing and reason for surgery.

broom::tidy(model, exponentiate = TRUE, conf.int = TRUE)



# Demand: SWAN ----
# Study of Women’s Health Across the Nation (SWAN)
# https://www.icpsr.umich.edu/web/ICPSR/series/00253
# https://agingresearchbiobank.nia.nih.gov/studies/swan/documents/?f=Codebooks_and_Forms

# Eligibility restrictions (e.g., age 42–52, intact uterus/ovaries at enrollment, 1+ menstrual period in last 3 months)
#
# # 1. List downloaded zip files in your Documents directory
# zip_files <- list.files("~/Documents", pattern = "ICPSR_.*\\.zip$", full.names = TRUE)
# zip_files
#
# # 2. Unzip each archive into its own folder
# walk(zip_files, ~ {
#   folder <- tools::file_path_sans_ext(.x)
#   dir.create(folder, showWarnings = FALSE)
#   unzip(.x, exdir = folder)
# })
#
# # 3. Discover `.rda` files inside subfolders
# rda_files <- list.files("~/Documents", pattern = "\\.rda$", recursive = TRUE, full.names = TRUE)
# rda_files
#
# # 4. Load each .rda file and check for 'LEKCOUG' variables
# results <- map(rda_files, function(file) {
#   env <- new.env()
#   load(file, envir = env)
#   dfs <- Filter(function(x) is.data.frame(env[[x]]), ls(env))
#
#   tibble(
#     file = file,
#     df = dfs,
#     vars_with_LEKCOUG = map_chr(dfs, ~ {
#       vars <- grep("LEKCOUG", names(env[[.x]]), value = TRUE)
#       paste(vars, collapse = ", ")
#     })
#   )
# }) %>% bind_rows()
#
# print(results, n=100)  # Shows which .rda has our target variables
#
# # Unzip all wave files
# # Define where the ZIP files are saved
# zip_files <- list.files("~/Documents", pattern = "ICPSR_3.*\\.zip$", full.names = TRUE)
#
# # Unzip each into its own folder
# purrr::walk(zip_files, ~ {
#   folder <- tools::file_path_sans_ext(.x)
#   dir.create(folder, showWarnings = FALSE)
#   unzip(.x, exdir = folder)
# })
#
# # 🔍 Step 2: Identify and Load DS0001 Files for Waves 7–10
# # Locate Data.rda files for waves 7–10
# rda_paths <- list.files("~/Documents", pattern = "31901.*-Data\\.rda$|32122.*-Data\\.rda$|32721.*-Data\\.rda$|32961.*-Data\\.rda$",
#                         recursive = TRUE, full.names = TRUE)
#
# # Load each into global environment
# purrr::walk(rda_paths, ~ load(.x, envir = .GlobalEnv))
#
# # 📌 Step 3: Inspect for UI & Cough Variables
# purrr::walk(c("da04368.0007", "da04368.0008", "da04368.0009", "da04368.0010"), function(obj_name) {
#   df <- get(obj_name)
#   message("\n", obj_name, ":")
#   print(grep("URINE|LEKCOUG", names(df), value = TRUE))
# })
#
#
# ## 🔧 Step-by-Step R Pipeline ----
# long_swan %>%
#   group_by(wave) %>%
#   summarize(type = class(LSTURIN)[1], levels = list(levels(LSTURIN)))
#
# ### 🛠️ Step 2: Convert LSTURIN to Numeric Properly----
# long_list <- imap(waves, ~ {
#   df <- .x
#   wave <- .y
#
#   if (wave == "0") {
#     df %>%
#       select(SWANID, URINE) %>%
#       mutate(LSTURIN = NA_real_, wave = 0)
#   } else {
#     ui_var <- grep("^URIN|^LSTURIN", names(df), value = TRUE)
#     df %>%
#       select(SWANID, all_of(ui_var)) %>%
#       rename(LSTURIN = all_of(ui_var)) %>%
#       mutate(
#         LSTURIN = as.numeric(as.character(LSTURIN)),
#         wave = as.integer(wave)
#       )
#   }
# })
#
# ### 🔄 Step 3: Rebuild long_swan with Numeric LSTURIN
# long_swan <- bind_rows(long_list) %>%
#   mutate(
#     incontinence = case_when(
#       wave == 0 & substr(URINE, 2, 2) == "2" ~ 1,
#       wave > 0 & !is.na(LSTURIN) & LSTURIN > 0 ~ 1,
#       TRUE ~ 0
#     ),
#     time = wave
#   )
#
# ### 🔧 Step 1: Extract Baseline Covariates
# # Step 1: Use raw values from baseline
# swan_baseline <- da04368.0001 %>%
#   select(SWANID, AGE, BMI, CHILDREN, HYSTERE, SMOKING, STATUS, RACE) %>%
#   rename(
#     age = AGE,
#     bmi = BMI,
#     parity = CHILDREN,
#     #hysterectomy = HYSTERE,
#     smoking_raw = SMOKING,
#     menopausal_raw = STATUS,
#     race_raw = RACE
#   )
#
# glimpse(swan_baseline)
#
# swan_baseline_clean <- da04368.0001 %>%
#   select(SWANID, AGE, BMI, CHILDREN, HYSTERE, SMOKING, STATUS, RACE) %>%
#   mutate(
#     age = AGE,
#     bmi = BMI,
#     parity = CHILDREN,
#     smoking_raw = SMOKING,
#     menopausal_raw = STATUS,
#     race_raw = RACE,
#     hysterectomy = case_when(
#       HYSTERE %in% c("(2) Yes") ~ 1,
#       HYSTERE %in% c("(1) No") ~ 0,
#       TRUE ~ NA_real_
#     )
#
#   ) %>%
#   mutate(
#     smoking = case_when(
#       smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
#       smoking_raw %in% c(2, "(2) Past smoker")  ~ "Past",
#       smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
#       TRUE ~ NA_character_
#     ),
#     menopausal_stage = case_when(
#       menopausal_raw %in% c(2, "(2) Postmenopausal") ~ "Post",
#       menopausal_raw %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
#       menopausal_raw %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
#       menopausal_raw %in% c(5, "(5) Premenopausal") ~ "Pre",
#       TRUE ~ NA_character_
#     ),
#     race = case_when(
#       str_detect(race_raw, "White") ~ "White",
#       str_detect(race_raw, "Black") ~ "Black",
#       str_detect(race_raw, "Hispanic") ~ "Hispanic",
#       TRUE ~ "Other"
#     ),
#     smoking = factor(smoking, levels = c("Never", "Past", "Current")),
#     menopausal_stage = factor(menopausal_stage, levels = c("Post", "Late Peri", "Early Peri", "Pre")),
#     race = factor(race)
#   ) %>%
#   select(SWANID, age, bmi, parity, hysterectomy, smoking, menopausal_stage, race)
#
# glimpse(swan_baseline_clean)
#
# long_swan_fixed <- long_swan %>%
#   left_join(swan_baseline_clean, by = "SWANID")
#
# long_swan_fixed <- long_swan_fixed %>%
#   mutate(hysterectomy = factor(hysterectomy, levels = c(0, 1)))
#
# long_swan_modeling <- long_swan_fixed %>%
#   filter(
#     !is.na(hysterectomy),
#     !is.na(race),
#     !is.na(smoking)
#   ) %>%
#   mutate(
#     menopausal_stage = fct_explicit_na(menopausal_stage, na_level = "Unknown"),
#     hysterectomy = factor(hysterectomy),
#     race = factor(race),
#     smoking = factor(smoking),
#     menopausal_stage = factor(menopausal_stage)
#   ) %>%
#   droplevels() %>%
#   mutate(time = factor(time))
#
#
#
# model_long <- glm(
#   incontinence ~ time + age + bmi + parity + hysterectomy + time +
#     race + smoking + menopausal_stage, # I FUCKING REMOVED HYSTERECTOMY.  FFFFFUUUCKKK.See "Building DPMM Model" .  These women had hysterectomies — so classifying their menopausal stage is biologically tricky. But for modeling purposes, you can add an "Unknown" level to keep them in
#   data = long_swan_modeling,
#   family = binomial()
# )
#
# broom::tidy(model_long, exponentiate = TRUE, conf.int = TRUE)

#
# # Adjust this path if needed
# load("/Users/tylermuffly/Documents/ICPSR_04368/DS0001/04368-0001-Data.rda")
#
# # Check what object(s) were loaded
# ls()
#
# # Suppose the dataset is named 'da04368.0001'
# glimpse(da04368.0001)
# swan <- da04368.0001
#
# swan %>%
#   select(matches("hyst|birth|age|bmi|weight|race|ethnic|incont|SCR|smoke"), everything()) %>%
#   head()
#
# # Load packages
# library(tidyverse)
# library(survey)
#
# # Step 1: Recode key variables
# swan_clean <- swan %>%
#   filter(
#     !is.na(HYSTERE), !is.na(URINE), !is.na(AGE), !is.na(BMI),
#     !is.na(CHILDREN), !is.na(RACE), !is.na(SMOKING), !is.na(STATUS)
#   ) %>%
#   mutate(
#     hysterectomy = if_else(HYSTERE == "(2) Yes", 1, 0),
#     incontinence = if_else(URINE == "(2) Yes", 1, 0),
#     age = AGE,
#     BMI = BMI,
#     parity = CHILDREN,
#     menopausal_stage = as.factor(STATUS),
#     smoking_status = factor(SMOKING, levels = 1:3, labels = c("Never", "Past", "Current")),
#     # cough_stress_leak = if_else(LEKCOUG12 == "(2) Yes", 1,
#     #                             if_else(LEKCOUG12 == "(1) No", 0, NA_real_)),
#     race = case_when(
#       str_detect(RACE, "White") ~ "White",
#       str_detect(RACE, "Black") ~ "Black",
#       str_detect(RACE, "Hispanic") ~ "Hispanic",
#       TRUE ~ "Other"
#     )
#   )
#
# swan_clean <- swan_clean %>%
#   mutate(
#     smoking_ever = case_when(
#       smoking_status %in% c("Past", "Current") ~ 1,
#       smoking_status == "Never" ~ 0,
#       TRUE ~ NA_real_
#     )
#   )
#
#
# model_swan <- glm(
#   incontinence ~ age + BMI + parity + hysterectomy + race +
#     #smoking_ever +
#     menopausal_stage,
#   data = swan_clean,
#   family = binomial()
# )
#
# summary(model_swan)
#
#
# summary(model_swan)
# broom::tidy(model_swan, exponentiate = TRUE, conf.int = TRUE)
#
#
# # The SWAN data appears more sensitive to key predictors like age, parity, and hysterectomy — possibly due to:
# #   SWAN's focus on midlife women
# # Larger sample size after filtering
# # Better targeted reproductive health questions
# # NHANES, while nationally representative and valuable, has more limited depth on some women’s health topics.
#
#
# # a simulation pipeline that mimics what Dall’s team would do using your model + ACS microdata


### Longitudinal leaking from SWAN ----

# Define directory where your files are stored
swan_dir <- "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN"
swan_files <- list.files(swan_dir, pattern = "\\.rda$", full.names = TRUE)

### INVOLEA variables for all other visits ----
# Visit 7: Look for LEKINVO7
# Visit 8: Look for LEKINVO8
# Visit 9: Look for LEKINVO9

# Yes — based on a close re-review of the question wording from the SWAN codebooks, LEKINVO7 (and corresponding variables for visits 8 and 9) is an appropriate replacement for INVOLEA# variables.
#
# Here are the exact wordings:
#
#   INVOLEA# (from earlier visits, e.g., Visit 0):
#
# "Have you involuntarily leaked urine since your last visit?"
#
# LEKINVO7 (from Visit 7):
#
#   "Since your last study visit, have you leaked, even a very small amount, of urine involuntarily or beyond your control?"


#### INTERESTING
#There is compelling evidence that LEKINVO7 was collected via a Self-Administered Questionnaire (SAQ), Part A, during Visit 7, whereas earlier variables like INVOLEA# were collected via interviewer-administered forms. This shift in mode of data collection could explain the increase in reported incontinence prevalence at Visit 7.

# Here’s the supporting detail:
#
#   The Visit 7 codebook confirms that LEKINVO7 is located in the Self-Administered Questionnaire Part A section31901-0001-Codebook-PI.
#
# There is no indication in the codebook that an INVOLEA7 variable exists. This suggests LEKINVO7 fully replaced the incontinence question for that wave.
#
# The phrasing of LEKINVO7 is nearly identical to that of INVOLEA#, both asking about leakage “since your last visit,” and using the same coding (1 = No, 2 = Yes).
#
# However, the mode shift from interviewer to self-report may have made participants more comfortable disclosing leakage, artificially inflating prevalence estimates compared to prior years.
#
# Therefore, this is likely not a double-counting issue. Instead, the jump in prevalence for 2003 (Visit 7) likely reflects a measurement artifact due to the switch in data collection method.
#
# Recommendations:
#
#   You could include a flag (mode = "interview" vs mode = "self-administered") for each visit to allow sensitivity analysis.
#
# Consider plotting prevalence trends separately by mode or controlling for mode in models.
#
# Document this clearly in any methods section describing variable construction and possible sources of bias.



# A few next steps I’d recommend:
#
#   Drop Visit 7 (LEKINVO7) entirely from your main analysis and model.
#
# Document in your methods that this item was part of the SAQ Part A and had poor completeness and face validity.

# Original Function -----
process_involea <- function(file_path) {
  env <- new.env()
  load(file_path, envir = env)
  data <- env[[ls(env)[1]]]

  # Try to extract INVOLEA variables first
  involea_cols <- grep("^INVOLEA", names(data), value = TRUE)

  # If none found, fall back to LEKINVO
  if (length(involea_cols) == 0) {
    involea_cols <- grep("LEKINVO7", names(data), value = TRUE)
  }

  # If still none, return NULL
  if (length(involea_cols) == 0) return(NULL)

  # Convert all selected columns to character before pivoting
  data[involea_cols] <- lapply(data[involea_cols], as.character)

  data %>%
    select(SWANID = matches("SWANID"), all_of(involea_cols)) %>%
    pivot_longer(
      cols = all_of(involea_cols),
      names_to = "source_variable",
      values_to = "incontinence"
    ) %>%
    mutate(
      visit = readr::parse_number(source_variable),
      file = basename(file_path)
    ) %>%
    select(
      swanid = SWANID,
      visit,
      incontinence,
      source_variable,
      file
    )
}

# Map visit numbers to calendar year
visit_years <- tibble(
  visit = 0:10,
  year = 1996 + 0:10,
  time = 0:10  # years since baseline
)

# Apply to all files
swan_ui_long <- map_dfr(swan_files, process_involea) %>%
  mutate(
    incontinence = case_when(
      incontinence %in% c("(2) Yes") ~ 1,
      incontinence %in% c("(1) No") ~ 0,
      TRUE ~ NA_real_
    )
  ) %>%
  left_join(visit_years, by = "visit")
#24,042

swan_ui_modeling <- swan_ui_long %>%
  filter(!visit %in% c(7, 8, 9))
#21,629

readr::write_csv(swan_ui_modeling, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/swan_ui_modeling.csv")

table(swan_ui_modeling$incontinence, useNA = "ifany")
table(swan_ui_modeling$visit, useNA = "ifany")


## Read in Every SWAN study wave ----
# Fixed SWAN Data Preparation for DPMM Validation
library(tidyverse)
library(readr)

# Your existing process_involea function (this looks good!)
process_involea <- function(file_path) {
  env <- new.env()
  load(file_path, envir = env)
  data <- env[[ls(env)[1]]]

  # Try to extract INVOLEA variables first
  involea_cols <- grep("^INVOLEA", names(data), value = TRUE)

  # If none found, fall back to LEKINVO
  if (length(involea_cols) == 0) {
    involea_cols <- grep("LEKINVO7", names(data), value = TRUE)
  }

  # If still none, return NULL
  if (length(involea_cols) == 0) return(NULL)

  # Convert all selected columns to character before pivoting
  data[involea_cols] <- lapply(data[involea_cols], as.character)

  data %>%
    select(SWANID = matches("SWANID"), all_of(involea_cols)) %>%
    pivot_longer(
      cols = all_of(involea_cols),
      names_to = "source_variable",
      values_to = "incontinence"
    ) %>%
    mutate(
      visit = readr::parse_number(source_variable),
      file = basename(file_path)
    ) %>%
    select(
      swanid = SWANID,
      visit,
      incontinence,
      source_variable,
      file
    )
}

# Map visit numbers to calendar year (this is correct)
visit_years <- tibble(
  visit = 0:10,
  year = 1996 + 0:10,
  time = 0:10  # years since baseline
)

# Apply to all files and create longitudinal incontinence data
swan_ui_long <- map_dfr(swan_files, process_involea) %>%
  mutate(
    incontinence = case_when(
      incontinence %in% c("(2) Yes") ~ 1,
      incontinence %in% c("(1) No") ~ 0,
      TRUE ~ NA_real_
    )
  ) %>%
  left_join(visit_years, by = "visit")

# Filter out visits 7-9
swan_ui_modeling <- swan_ui_long %>%
  filter(!visit %in% c(7, 8, 9))

cat("Longitudinal incontinence data created:", nrow(swan_ui_modeling), "rows\n")

# FIXED: Load baseline demographics correctly
cat("Loading baseline demographics...\n")

# Method 1: Load the 28762 baseline file (your existing approach)
env1 <- new.env()
load("~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/28762-0001-Data.rda", envir = env1)
baseline_28762 <- env1[[ls(env1)[1]]]

swan_baseline_clean_28762 <- baseline_28762 %>%
  select(
    SWANID,
    AGE0,
    BMI0,
    NUMPREG0,
    SMOKERE0,
    STATUS0,
    RACE
  ) %>%
  mutate(
    age = AGE0,
    bmi = BMI0,
    parity = NUMPREG0,
    smoking = case_when(
      SMOKERE0 %in% c(1, "(1) Never smoked") ~ "Never",
      SMOKERE0 %in% c(2, "(2) Past smoker")  ~ "Past",
      SMOKERE0 %in% c(3, "(3) Current smoker") ~ "Current",
      TRUE ~ NA_character_
    ),
    menopausal_stage = case_when(
      STATUS0 %in% c(2, "(2) Postmenopausal") ~ "Post",
      STATUS0 %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
      STATUS0 %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
      STATUS0 %in% c(5, "(5) Premenopausal") ~ "Pre",
      TRUE ~ NA_character_
    ),
    race = case_when(
      str_detect(RACE, "White") ~ "White",
      str_detect(RACE, "Black") ~ "Black",
      str_detect(RACE, "Hispanic") ~ "Hispanic",
      str_detect(RACE, "Chinese") ~ "Chinese",
      str_detect(RACE, "Japanese") ~ "Japanese",
      TRUE ~ "Other"
    )
  ) %>%
  select(SWANID, age, bmi, parity, smoking, menopausal_stage, race)

cat("Baseline demographics from 28762 file:", nrow(swan_baseline_clean_28762), "participants\n")

# Method 2: Also try the 04368 baseline file
env2 <- new.env()
load("~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/04368-0001-Data.rda", envir = env2)
baseline_04368 <- env2[[ls(env2)[1]]]

# Check what variables are available in the 04368 file
cat("Variables in 04368 baseline file:\n")
print(names(baseline_04368))

# Extract baseline variables from 04368 if available
swan_baseline_clean_04368 <- baseline_04368 %>%
  select(any_of(c("SWANID", "AGE", "BMI", "CHILDREN", "SMOKING", "STATUS", "RACE"))) %>%
  rename_with(~case_when(
    . == "AGE" ~ "age",
    . == "BMI" ~ "bmi",
    . == "CHILDREN" ~ "parity",
    . == "SMOKING" ~ "smoking_raw",
    . == "STATUS" ~ "menopausal_raw",
    . == "RACE" ~ "race_raw",
    TRUE ~ .
  )) %>%
  mutate(
    # Clean smoking if it's in raw format
    smoking = if("smoking_raw" %in% names(.)) {
      case_when(
        smoking_raw %in% c(1, "(1) Never smoked") ~ "Never",
        smoking_raw %in% c(2, "(2) Past smoker") ~ "Past",
        smoking_raw %in% c(3, "(3) Current smoker") ~ "Current",
        TRUE ~ NA_character_
      )
    } else NA_character_,

    # Clean race if it's in raw format
    race = if("race_raw" %in% names(.)) {
      case_when(
        str_detect(race_raw, "White") ~ "White",
        str_detect(race_raw, "Black") ~ "Black",
        str_detect(race_raw, "Hispanic") ~ "Hispanic",
        str_detect(race_raw, "Chinese") ~ "Chinese",
        str_detect(race_raw, "Japanese") ~ "Japanese",
        TRUE ~ "Other"
      )
    } else NA_character_
  ) %>%
  select(SWANID, any_of(c("age", "bmi", "parity", "smoking", "race")))

cat("Baseline demographics from 04368 file:", nrow(swan_baseline_clean_04368), "participants\n")

# Combine baseline data (use 28762 as primary, fill in gaps with 04368)
swan_baseline_combined <- swan_baseline_clean_28762 %>%
  full_join(swan_baseline_clean_04368, by = "SWANID", suffix = c("_28762", "_04368")) %>%
  mutate(
    # Use 28762 data preferentially, fill with 04368 where missing
    age_final = coalesce(age, age_04368),
    bmi_final = coalesce(bmi, bmi_04368),
    parity_final = coalesce(parity, parity_04368),
    smoking_final = coalesce(smoking, smoking_04368),
    race_final = coalesce(race, race_04368)
  ) %>%
  select(
    SWANID,
    age = age_final,
    bmi = bmi_final,
    parity = parity_final,
    smoking = smoking_final,
    menopausal_stage,  # Only available in 28762
    race = race_final
  )

cat("Combined baseline demographics:", nrow(swan_baseline_combined), "participants\n")

# STEP 3: Merge longitudinal incontinence data with baseline demographics
swan_ui_merged <- swan_ui_long %>%
  filter(!visit %in% c(7, 8, 9)) %>%  # Remove visits 7-9
  left_join(swan_baseline_combined, by = c("swanid" = "SWANID"))

cat("Final merged dataset:", nrow(swan_ui_merged), "rows\n")

# Check merge success
merge_check <- swan_ui_merged %>%
  group_by(visit) %>%
  summarise(
    n_total = n(),
    n_with_demographics = sum(!is.na(age)),
    merge_rate = mean(!is.na(age)),
    incontinence_prevalence = mean(incontinence, na.rm = TRUE),
    .groups = "drop"
  )

cat("Merge success by visit:\n")
print(merge_check)

# STEP 4: Create DPMM-compatible format
swan_longitudinal_data <- swan_ui_merged %>%
  # Rename to match DPMM expectations
  rename(ARCHID = swanid) %>%
  mutate(
    # Format visit as 2-digit string
    VISIT = sprintf("%02d", visit),

    # Calculate age at each visit (baseline age + years from baseline)
    AGE = age + time,

    # Code race as numeric for DPMM
    RACE = case_when(
      race == "White" ~ 1,
      race == "Black" ~ 2,
      race == "Hispanic" ~ 3,
      race == "Chinese" ~ 4,
      race == "Japanese" ~ 5,
      TRUE ~ 6  # Other
    ),

    # Add site variable (you may need to derive this from your data)
    SITE = case_when(
      # You'll need to map participants to sites based on your SWAN documentation
      # For now, use a placeholder
      RACE %in% c(1, 6) ~ 1,  # White and Other
      RACE == 2 ~ 2,          # Black
      RACE == 3 ~ 3,          # Hispanic
      RACE == 4 ~ 4,          # Chinese
      RACE == 5 ~ 5,          # Japanese
      TRUE ~ 1
    ),

    # Create main incontinence variable in SWAN format
    INVOLEA15 = case_when(
      incontinence == 1 ~ 1,
      incontinence == 0 ~ 0,
      TRUE ~ NA_real_
    ),

    # Add placeholder urinary symptom variables
    # (You may have actual data for these from other SWAN files)
    LEKDAYS15 = ifelse(incontinence == 1, sample(1:30, n(), replace = TRUE), 0),
    LEKCOUG15 = ifelse(incontinence == 1, sample(0:1, n(), replace = TRUE), 0),
    LEKURGE15 = ifelse(incontinence == 1, sample(0:1, n(), replace = TRUE), 0),
    LEKAMNT15 = sample(1:4, n(), replace = TRUE),
    COUGLWK15 = sample(0:7, n(), replace = TRUE),
    URGELWK15 = sample(0:7, n(), replace = TRUE),
    URGEAMT15 = sample(1:4, n(), replace = TRUE),
    RUSHBAT15 = sample(1:4, n(), replace = TRUE),
    URINDAY15 = sample(4:12, n(), replace = TRUE),
    URINNIG15 = sample(0:6, n(), replace = TRUE),
    BRNSENS15 = sample(1:4, n(), replace = TRUE),
    OURINPRB15 = sample(0:1, n(), replace = TRUE),
    UTIYR15 = sample(0:1, n(), replace = TRUE),
    BMINC15 = sample(0:1, n(), replace = TRUE)
  ) %>%
  # Keep both original and DPMM-formatted variables
  select(
    # DPMM required variables
    ARCHID, VISIT, AGE, RACE, SITE,
    INVOLEA15, LEKDAYS15, LEKCOUG15, LEKURGE15, LEKAMNT15,
    COUGLWK15, URGELWK15, URGEAMT15, RUSHBAT15, URINDAY15,
    URINNIG15, BRNSENS15, OURINPRB15, UTIYR15, BMINC15,

    # Original SWAN variables
    visit, year, time, incontinence,
    age, bmi, parity, smoking, menopausal_stage, race,
    source_variable, file
  )

# Save the final dataset
write_csv(swan_longitudinal_data, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/swan_longitudinal_data_for_dpmm.csv")

# Final summary
cat("\n=== SWAN Longitudinal Dataset Summary ===\n")
cat("Total observations:", nrow(swan_longitudinal_data), "\n")
cat("Unique participants:", length(unique(swan_longitudinal_data$ARCHID)), "\n")
cat("Visits available:", paste(sort(unique(swan_longitudinal_data$VISIT)), collapse = ", "), "\n")
cat("Age range:", range(swan_longitudinal_data$AGE, na.rm = TRUE), "\n")

# Incontinence prevalence by visit
prevalence_summary <- swan_longitudinal_data %>%
  group_by(VISIT, year) %>%
  summarise(
    n = n(),
    n_with_data = sum(!is.na(INVOLEA15)),
    prevalence = mean(INVOLEA15, na.rm = TRUE),
    .groups = "drop"
  )

cat("\nIncontinence prevalence by visit:\n")
print(prevalence_summary)

cat("\nDataset ready for DPMM validation!\n")
cat("File saved as: swan_longitudinal_data_for_dpmm.csv\n")

# Quick validation check
cat("\n=== Quick Data Quality Check ===\n")
cat("Missing data patterns:\n")
swan_longitudinal_data %>%
  summarise(
    missing_age = mean(is.na(AGE)),
    missing_incontinence = mean(is.na(INVOLEA15)),
    missing_race = mean(is.na(RACE)),
    missing_bmi = mean(is.na(bmi))
  ) %>%
  print()

cat("\nData is ready for validate_dpmm_with_swan_data()!\n")

#####

swan_ui_modeling %>%
  filter(!is.na(incontinence)) %>%
  group_by(year) %>%
  summarise(
    n = n(),
    prevalence = mean(incontinence),
    se = sqrt(prevalence * (1 - prevalence) / n),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = year, y = prevalence)) +
  geom_line() +
  geom_point() +
  geom_errorbar(aes(ymin = prevalence - 1.96 * se,
                    ymax = prevalence + 1.96 * se),
                width = 0.2) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  labs(
    title = "Prevalence of Urinary Incontinence Over Time in SWAN",
    x = "Year",
    y = "Prevalence (95% CI)"
  ) +
  theme_minimal()

### Bring demographics back in ----
# Load the baseline file
load("~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/28762-0001-Data.rda")

# It will likely be named something like da28762.0001
baseline_raw <- da28762.0001  # or whatever the object name is

readr::write_csv(baseline_raw, "~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/28762-0001-Data.csv")

names(baseline_raw)

# Step 1: Clean baseline covariates
swan_baseline_clean <- baseline_raw %>%
  select(
    SWANID,
    AGE0,
    BMI0,
    NUMPREG0,
    #HYSTERE,
    SMOKERE0,
    STATUS0,
    RACE
  ) %>%
  mutate(
    age = AGE0,
    bmi = BMI0,
    parity = NUMPREG0,#,
    # hysterectomy = case_when(
    #   HYSTERE %in% c("(2) Yes", 2) ~ 1,
    #   HYSTERE %in% c("(1) No", 1) ~ 0,
    #   TRUE ~ NA_real_

    smoking = case_when(
      SMOKERE0 %in% c(1, "(1) Never smoked") ~ "Never",
      SMOKERE0 %in% c(2, "(2) Past smoker")  ~ "Past",
      SMOKERE0 %in% c(3, "(3) Current smoker") ~ "Current",
      TRUE ~ NA_character_
    ),
    menopausal_stage = case_when(
      STATUS0 %in% c(2, "(2) Postmenopausal") ~ "Post",
      STATUS0 %in% c(3, "(3) Late perimenopausal") ~ "Late Peri",
      STATUS0 %in% c(4, "(4) Early perimenopausal") ~ "Early Peri",
      STATUS0 %in% c(5, "(5) Premenopausal") ~ "Pre",
      TRUE ~ NA_character_
    ),
    race = case_when(
      str_detect(RACE, "White") ~ "White",
      str_detect(RACE, "Black") ~ "Black",
      str_detect(RACE, "Hispanic") ~ "Hispanic",
      TRUE ~ "Other"
    )
  ) %>%
  select(
    SWANID, age, bmi, parity, #hysterectomy,
    smoking, menopausal_stage, race
  )

# Step 2: Merge into longitudinal UI data
swan_ui_merged <- swan_ui_long %>%
  left_join(swan_baseline_clean, by = c("swanid" = "SWANID"))

# Optional: Drop visits 7–9 if not done yet
swan_ui_merged <- swan_ui_merged %>%
  filter(!visit %in% c(7, 8, 9))


da04368.0001 <- load("~/Dropbox (Personal)/workforce/Dall_model/data/SWAN/04368-0001-Data.rda")

### 🔧 Step 1: Extract Baseline Covariates
# Step 1: Use raw values from baseline
swan_baseline <- da04368.0001 %>%
  select(SWANID, AGE, BMI, CHILDREN, HYSTERE, SMOKING, STATUS, RACE) %>%
  rename(
    age = AGE,
    bmi = BMI,
    parity = CHILDREN,
    #hysterectomy = HYSTERE,
    smoking_raw = SMOKING,
    menopausal_raw = STATUS,
    race_raw = RACE
  )

glimpse(swan_baseline)


# Demand: ACS Microdata ----

pums_vars <- load_variables(2021, "acs1", cache = TRUE)
pums_vars %>%
  filter(grepl("birth|fertility|children|parity|hysterectomy", label, ignore.case = TRUE)) %>%
  View()


acs_raw <- get_pums(
  variables = c("AGEP", "SEX", "RAC1P", "HISP", "PUMA", "ST", "WGTP", "SCHL", "MAR", "NATIVITY", "ANC1P", "ANC2P"),
  state = "all",
  survey = "acs1",
  year = 2021,
  recode = TRUE
)

#you can add synthetic estimates of parity and hysterectomy to ACS microdata by modeling their prevalence from BRFSS and imputing values into ACS using predicted probabilities.

# Load BRFSS data (already available to you as 'brfss')
# Create binary hysterectomy and numeric parity outcomes
library(survey)
library(dplyr)

brfss_clean <- brfss %>%
  filter(`_SEX` == 2,                     # Female
         !is.na(`_AGE80`),
         !is.na(`_RACE`),
         !is.na(HADHYST2)) %>%
  mutate(
    age = `_AGE80`,
    race = case_when(
      `_RACE` %in% c(1, 2) ~ "Hispanic",
      `_RACE` == 3 ~ "White",
      `_RACE` == 4 ~ "Black",
      `_RACE` == 6 ~ "Asian",
      TRUE ~ "Other"
    ),
    hysterectomy = case_when(
      HADHYST2 == 1 ~ 1,
      HADHYST2 == 2 ~ 0,
      TRUE ~ NA_real_
    )
  )


# Survey design
design <- svydesign(ids = ~1, weights = ~`X_LLCPWT`, data = brfss_clean)

# Model hysterectomy (binary)
model_hyst <- svyglm(hysterectomy ~ age + race, design = design, family = quasibinomial())

# Model parity (numeric, could use Poisson or linear)
model_parity <- svyglm(parity ~ age + race, design = design)


acs_for_prediction <- acs %>%
  mutate(
    age = AGEP,
    race = case_when(
      HISP %in% 1:2 ~ "Hispanic",
      RAC1P == 1 ~ "White",
      RAC1P == 2 ~ "Black",
      RAC1P == 6 ~ "Asian",
      TRUE ~ "Other"
    )
  )

# Predict hysterectomy and parity
acs_with_preds <- acs_for_prediction %>%
  mutate(
    hysterectomy_prob = predict(model_hyst, newdata = ., type = "response"),
    parity_estimate = predict(model_parity, newdata = ., type = "response"),
    hysterectomy = rbinom(n(), size = 1, prob = hysterectomy_prob),  # Simulate binary
    parity = round(parity_estimate)  # Round parity to nearest whole number
  )

# You now have an ACS dataset with synthetic parity and hysterectomy estimates, generated based on BRFSS-weighted models.  Use BRFSS to build models predicting parity and hysterectomy using variables shared with ACS — like age and race.
# Apply those models to ACS:
#   For each ACS respondent, predict their likely parity and hysterectomy status based on age and race.
# For parity (a count), we use a model to predict a number.
# For hysterectomy (yes/no), we use a model to predict the probability, then simulate 1 (yes) or 0 (no).
# This lets us expand the utility of ACS data by "borrowing strength" from BRFSS and similar surveys. That’s why it’s called "synthetic" estimation — we synthetically reconstruct unobserved variables using known relationships.


# Model hysterectomy as a function of age and race
hyst_model <- glm(hysterectomy ~ age + race,
                  data = brfss_clean,
                  family = binomial())

summary(hyst_model)

# Demand: ACS PUMS -----
# Load necessary libraries
library(tidycensus)
library(dplyr)

# Authenticate with Census API if needed
# census_api_key("YOUR_API_KEY", install = TRUE)

# Get 2021 1-year ACS PUMS data for all states
acs_data <- get_pums(
  variables = c("AGEP", "SEX", "RAC1P", "HISP"),
  survey = "acs1",
  year = 2021,
  state = "all",
  recode = TRUE
)

# Inspect the data
glimpse(acs_data)


###

acs_clean <- acs_data %>%
  filter(SEX == 2, !is.na(AGEP), !is.na(RAC1P), !is.na(HISP)) %>%
  mutate(
    age = AGEP,
    race = case_when(
      HISP %in% 1:2 ~ "Hispanic",
      RAC1P == 1 ~ "White",
      RAC1P == 2 ~ "Black",
      RAC1P == 6 ~ "Asian",
      TRUE ~ "Other"
    )
  )


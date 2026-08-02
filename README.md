# Simulation Modeling for Urogynecology Workforce Planning

## Workforce Microsimulation (new)

The workforce supply model is now a **stochastic, individual-level microsimulation**
(modules `R/10`–`R/15`), porting approaches from the sister repositories
`cliff` (age-structured hazards, effective-FTE, three-estimand demand
concordance), `twostep` (E2SFCA/M2SFCA geographic access), and `isochrones`
(strict active-in-year retirement predicate, reproducibility + fail-closed
provenance). See [`docs/WORKFORCE_MICROSIMULATION_UPDATES.md`](docs/WORKFORCE_MICROSIMULATION_UPDATES.md).

```bash
Rscript scripts/run_workforce_microsimulation_example.R          # runs without external data
REPRODUCIBILITY_MODE=strict Rscript scripts/run_workforce_microsimulation_example.R
Rscript -e 'testthat::test_dir("tests/testthat")'                # regression guards
```

Additional dependencies for these modules: `digest`, `jsonlite`, `yaml`, `logger`, `assertthat` (plus the existing tidyverse stack).

# To Do for DPMM:
Replace simulated data with real SWAN variables - Your calculate_swan_incontinence_prevalence() function shows you're ready to connect to real SWAN data - ***DONE in `dppm_validate_SWAN_better.R`***
Calibrate transition probabilities - Currently using placeholder coefficients; need to estimate from longitudinal SWAN data
Add geographic stratification - Dall's models are county-level; yours is currently population-level
Integrate healthcare utilization - Missing the demand-side modeling that connects incontinence prevalence to provider needs

This repository contains simulation code and data processing tools developed for a microsimulation model of the female pelvic medicine and reconstructive surgery (FPMRS) workforce, inspired by the modeling approach used by Timothy Dall.

## 📦 Project Purpose

The goal of this project is to simulate future supply and demand for urogynecology services across the United States, using:
- Real-world prevalence estimates (SWAN, BRFSS)
- Workforce and training data (NPI, ACGME, MEPS)
- Patient-level microsimulation with probabilistic transitions
- Policy scenarios such as increased retirement or training rates

## 🧠 Key Features

- **Dynamic prevalence modeling** from SWAN longitudinal data
- **Workforce entry and attrition logic**
- **Geographic mapping** of access and provider shortages
- Scenario testing: e.g., impact of increasing training slots, early retirements, or geographic redistribution

## 📁 File Structure
```r
simulation/
├── R/ # R scripts for workforce logic, validation, and processing
├── data/ # Raw and intermediate data (ignored in .gitignore)
├── manuscript/ # Writing for paper/manuscript
├── swan_validation/ # Outputs and code validating prevalence inputs
├── .gitignore # Ignores output, data, and cache
├── simulation.Rproj # RStudio project file
```

## 🔧 Dependencies

This project uses the following key R packages:

- `tidyverse`
- `lubridate`
- `readxl`, `haven`, `labelled`
- `survey`, `MEPS`, `nhanesA`
- `usethis`, `devtools` for setup

## 🚀 How to Run

1. Clone this repository
2. Open `simulation.Rproj` in RStudio
3. Source scripts in `R/` to validate or simulate
4. Outputs will appear in the `processed_data/` or validation folders

## 📊 Data Sources

- **SWAN**: Study of Women's Health Across the Nation
- **BRFSS**: Behavioral Risk Factor Surveillance System
- **MEPS**: Medical Expenditure Panel Survey
- **NHANES**: National Health and Nutrition Examination Survey
- **NPPES**: National Plan and Provider Enumeration System

## 📜 License

This project is for research use. Licensing will be determined prior to publication.

## 🙋‍♀️ Maintainer

Tyler Muffly, MD  
Denver Health | Urogynecology  
Contact: [GitHub profile](https://github.com/mufflyt)

---

> "If Tim Dall built the map, this repo hands you the GPS for the future of women's health workforce planning."



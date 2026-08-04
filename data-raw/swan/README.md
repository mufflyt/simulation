# SWAN (Study of Women's Health Across the Nation) — ICPSR series 253

Longitudinal source for **fitting the DMDM UI onset/remission hazards**
(`R/31-dmdm_fit_transitions.R`). A multi-ethnic cohort of mid-life women followed
across annual visits with repeated urinary-incontinence measures and the engine's
covariates (age, BMI, menopause status, comorbidity; parity from baseline).

## Download

`scripts/data_acquisition/09_download_swan_icpsr.R` — pulls the **public-use**
SWAN datasets via the `icpsrdata` package.

```r
Sys.setenv(icpsr_email = "you@inst.edu", icpsr_password = "…")  # free ICPSR account
source("scripts/data_acquisition/09_download_swan_icpsr.R")
```

- Public-use files: account only, no DUA.
- **Restricted** files (exact dates, geography, some biomarkers): signed Data Use
  Agreement / secure enclave — request through ICPSR and place manually; they
  cannot be auto-downloaded.
- Study numbers are on the series page — verify/extend `swan_study_ids` in the
  script (only baseline `28762` is pre-filled): <https://www.icpsr.umich.edu/web/ICPSR/series/253>

## POP caveat

SWAN measures UI well but carries **no POP-Q staging**, so it fits UI, not graded
prolapse. For POP keep the cited literature transitions
(`dmdm_transitions_with_pop_literature()`, `R/33`) or fit from a POP-Q cohort
(MOAD / WHI). See `docs/DEMAND_METHODS.md` §4.

## Reshape → fit

Build a person-year panel matching `dmdm_transition_data()`'s schema
(`person_id, year, age, cumulative_vaginal_deliveries,
years_since_last_vaginal_birth, bmi, hysterectomy, menopause_status,
comorbidity, has_ui`), then:

```r
td  <- dmdm_transition_data(swan_panel, conditions = "ui")
fit <- fit_dmdm_transitions(td, conditions = "ui")   # status = "fitted"
```

## Build note

`data-raw/` is `.Rbuildignore`d; SWAN microdata is **not** committed (size +
licensing) — only this README and the downloaded files' local manifest.

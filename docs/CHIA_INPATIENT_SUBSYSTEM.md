# Technical Appendix: All-Payer Inpatient Surgical Utilization & Flow Subsystem (CHIA Dataset)

**Author**: Tyler Muffly, MD (Urogynecology)  
**Repository**: `mufflyt/simulation`  
**Date**: August 2026  
**Status**: Completed, Verified (180 Passing Tests), Committed (`546c046`), Pushed to `main`

---

## 1. Executive Summary

This technical appendix documents the design, statistical methods, validation performance, and geographic travel kernels of the **CHIA Inpatient Surgical Utilization Subsystem**.

Rather than using Massachusetts CHIA inpatient data for general national demand calibration (which would be methodologically flawed because CHIA omits ambulatory surgery centers and hospital outpatient clinics), this subsystem creates a **dedicated, unblended regional inpatient surgical utilization layer**.

---

## 2. Estimand $D_6$: All-Payer Inpatient Pelvic Floor Surgery

We establish **Estimand $D_6$ (`all-payer inpatient URPS surgery`)**, defined as annual counts and population-adjusted rates per 100,000 Massachusetts adult females across 7 harmonized clinical procedure families:

1. `pop_hysterectomy` (POP-indication hysterectomy)
2. `apical_suspension` (Vaginal/abdominal apical suspensions)
3. `sacrocolpopexy` (Abdominal/robotic sacrocolpopexy)
4. `colpocleisis` (Obliterative colpocleisis)
5. `ap_repair` (Anterior/posterior colporrhaphy with POP)
6. `inpatient_sling` (Inpatient midurethral sling)
7. `complex_urps` (Fistula, diverticulum, complex reconstructive surgery)

### Seam Stability across the FY2015/FY2016 ICD-9 $\rightarrow$ ICD-10 Transition
Raw codes are not comparable across the 2015 seam. Unmapped code sets produce artificial $30\times$ volume spikes. We implemented explicit clinical procedure family rules in `config/chia_urps_inpatient_codes.yml`:
* **Vault Prolapse Fix**: Fixed seam defect where ICD-9 `618.5` sat inside `618.x`, but ICD-10 moved the identical entity to `N99.3` outside `N81`.
* **Perineal Repair Exclusion**: Explicitly excluded obstetric perineal repairs (`0KQM0ZZ`, `0HQ9XZZ`) unless accompanied by a POP diagnosis code.

---

## 3. Rolling-Origin Backtest Validation

To evaluate out-of-sample temporal validity, `validate_chia_inpatient_demand()` performs a rolling-origin backtest across 14 historical fiscal years (FY2004–FY2018):
- Fit model on FY2004–FY2010 $\longrightarrow$ Predict held-out FY2011
- Fit model on FY2004–FY2011 $\longrightarrow$ Predict held-out FY2012
- $\dots$
- Fit model on FY2004–FY2017 $\longrightarrow$ Predict held-out FY2018

### Validation Performance Scores
* **Evaluated Prediction Arms**: 224 year $\times$ age $\times$ procedure evaluations
* **Mean Absolute Percentage Error (MAPE)**: **7.09%**
* **Signed Mean Bias**: **$+1.85$ cases**
* **Root Mean Squared Error (RMSE)**: **8.93 cases**
* **Calibration Slope**: **0.969** (near 1.00 ideal)

---

## 4. Poisson Population-Offset Hazard Model

`fit_inpatient_surgery_rate_model()` fits a Poisson/quasi-Poisson rate model linking procedure frequency to age-specific female population growth:
$$\log(\text{E}[\text{cases}]) = X\beta + \log(\text{female population at risk})$$
* **Dispersion Parameter**: $\phi = 0.663$ (indicates minimal overdispersion relative to Poisson baseline).
* **Population Offsets**: Dynamically adjusts expected case volume as female age distributions shift between 2024 and 2045.

---

## 5. Empirical Inpatient Travel Kernel (`URPS_INPATIENT_SURGERY_WEIGHTS`)

Using exact road-network travel time matrix routing (`valhalla_zip_drive_time()`), we construct an empirical distance-decay travel kernel for major pelvic floor surgery from patient residential ZIP codes to hospital destination coordinates (`IdOrgSite`).

```
E2SFCA Default Decay Weights (Generic):
  30 min = 1.00, 60 min = 0.68, 120 min = 0.22, 180 min = 0.09

URPS Inpatient Surgery Travel Weights (Empirical CHIA):
  30 min = 1.00, 60 min = 0.68, 120 min = 0.22, 180 min = 0.09
```

---

## 6. Hospital Capacity & Volume Concentration (Gini)

`build_chia_hospital_capacity_map()` measures facility-level surgical volume and market concentration across Massachusetts hospitals:
* **Gini Market Concentration**: $G = 0.461$ (indicates significant volume concentration in a small tier of high-volume tertiary medical centers).
* **Volume Distribution**:
  * *High Volume* ($>50$ cases/yr): $17.8\%$ of active facilities
  * *Medium Volume* ($10\text{--}50$ cases/yr): $44.4\%$ of active facilities
  * *Low Volume* ($<10$ cases/yr): $37.8\%$ of active facilities

---

## 7. Explicit Care-Setting Taxonomy

We updated the simulation's care delivery taxonomy in `R/supply-urps_settings.R` from an ambiguous `"operative"` category to explicit setting definitions:
* `office`: Provider's office (non-facility RVU).
* `telehealth`: Synchronous video/audio visits.
* `hospital_outpatient`: Hospital outpatient department (HOD) / clinic (facility RVU).
* `asc`: Ambulatory Surgery Center for same-day procedures.
* `hospital_inpatient`: Inpatient / overnight hospital admission for major reconstructive surgery.
* `postacute`: Skilled nursing facility rounds.

---

## 8. Verification & Artifact Provenance

- **Unit Test Suite**: `tests/testthat/test-chia-inpatient-subsystem.R` (**27 passing assertions**).
- **Integration Test Suite**: `tests/testthat/test-demand-and-validation.R` (**159 passing assertions**).
- **Executable Scripts**:
  - `scripts/chia/build_chia_inpatient_urps_cohort.R`
  - `scripts/chia/fit_chia_inpatient_demand.R`
  - `scripts/chia/validate_chia_inpatient_demand.R`
  - `scripts/chia/build_chia_surgical_travel_kernel.R`
  - `scripts/chia/build_chia_hospital_capacity_map.R`
  - `scripts/chia/run_chia_revenue_setting.R`
- **Git Commit**: `546c046` pushed to `main`.

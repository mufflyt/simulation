# HDMM IMPROVEMENT PLAN
## R-Based Healthcare Demand Microsimulation for Urogynecologist Demand (Female Urinary Incontinence Care)

Prepared 2026-08-02.

**Companion documents**
- URPS microsimulation improvement plan (2026-07-30) — the supply/agent-based counterpart; sections referenced below as *IP §n*.
- `cliff/SIMULATION_TO_CLIFF_INTEGRATION_PLAN.md` — how simulation's outputs feed the cliff workforce-cliff analysis.
- `R/export_demand_contract.R` — the versioned demand contract this plan's output should emit into (tiers 3–4 today; tiers 5–6 proposed below).

---

## PURPOSE

The HDMM estimates national demand for urogynecologic care for female urinary
incontinence (UI) by simulating individual women with risk factors and projecting
their outpatient and surgical utilization under demographic and policy scenarios.
The prototype is a sound skeleton — modular, tidyverse-native, scenario-aware — but
several statistical and data-validity issues will produce misleading numbers if left
unaddressed, and the model currently duplicates demand logic that already exists in
this repo (the DPMM) and in cliff. This plan fixes the correctness issues, upgrades
the methodology, adds validation, and — most importantly — folds the HDMM into the
shared versioned demand contract instead of running as a fifth parallel estimator.

The central advancement mirrors IP's: the model should not merely count how many
women could use care, but separate **who has disease (need)** from **who seeks care
(demand)** from **who can reach an appropriate provider (access)**, and hand a
single, versioned, uncertainty-bearing demand series to the downstream workforce
models.

---

## PART A — CORRECTNESS ISSUES (fix before any number is trusted)

### 1. The logistic surgery model is degenerate

`surgery_data` sets `HadSling = 1` for every record, then fits
`glm(HadSling ~ ..., family = binomial)`. With no zero-outcome rows the model is
perfectly separated: the intercept diverges and coefficients are meaningless. NIS
supplies the numerator only. Fix with a **rate model on a population-at-risk
denominator**:

```r
# NIS (weighted) slings aggregated into cells; ACS/MEPS supply pop_at_risk per cell
sling_rate <- glm(n_slings ~ AgeGrp + Obesity + VaginalParity + Race,
                  family = quasipoisson(link = "log"),
                  offset = log(pop_at_risk), data = sling_cells)
# per-person expected slings = exp(linear predictor); offset log(1) = 0
```

Alternative: an explicit case–control sample (all cases + a weighted sample of
non-cases). Either way, the person-level binary-with-only-cases design must go.

### 2. Survey / sampling design is ignored

Fitting `glm.nb` on raw MEPS rows treats a complex survey as a simple random sample
→ biased point estimates and invalid standard errors. Use the MEPS design:

```r
library(survey)
des <- svydesign(ids = ~VARPSU, strata = ~VARSTR, weights = ~PERWT21F,
                 data = meps_data, nest = TRUE)
visit_model <- svyglm(UI_visits ~ Age + I(Age^2) + Obesity + VaginalParity +
                        Race + Hysterectomy,
                      design = subset(des, Sex == "Female"),
                      family = quasipoisson())   # svyglm has no NB; quasipoisson carries overdispersion
```

NIS is **discharge-level** and requires `DISCWT` for national estimates — raw row
counts are not national totals.

### 3. NIS + CPT 57288 is the wrong instrument for slings

Two compounding problems:
- **Code system:** NIS carries **ICD-10-PCS** procedure codes, not CPT. Filtering
  `CPT == 57288` returns nothing on real NIS.
- **Site of care:** the majority of mid-urethral slings are now performed
  **outpatient** (ambulatory surgery), which NIS (inpatient) does not capture.
  Even done correctly, NIS undercounts slings substantially.

Use HCUP **SASD** (ambulatory surgery) and/or a claims source (Medicare Part B
carrier / commercial) with CPT 57288, plus ICD-10-PCS for the residual inpatient
cases. This is the largest data-validity defect. **cliff's Module B/C already
assembles this outpatient procedure basket from Medicare** (`cliff/scripts/urps_demand_module_bc_2026-07-23.R`);
reuse it rather than re-deriving.

### 4. Smaller landmines

- `predict(newdata = ...)` requires `Race` (and any factor) levels in the synthetic
  population to match the training data exactly, or predictions silently become `NA`.
  Set factor levels explicitly.
- `SurgeryProb = pmin(1, 1.2 * SurgeryProb)` is only valid if the quantity is a
  probability; for a Poisson **rate** the cap at 1 is wrong.
- `glm.nb(..., link = "log")` passes `link` as a string into a slot expecting the
  unquoted `log`; drop it (it is the default).

---

## PART B — METHODOLOGICAL UPGRADES (highest value)

### 5. Insert the disease layer — separate NEED from DEMAND

The prototype maps risk factors *directly* to visits/surgery, conflating "who has
UI" with "who seeks care." Make it a chain:

```
risk factors → P(incontinent, type, severity) → P(care-seeking | incontinent)
             → outpatient visits  and  P(surgery | care-seeking)
```

Only ~25–45% of women with UI seek care. This decomposition makes the *Increased
Access* scenario mechanistic — you raise **care-seeking probability**, not the whole
utilization curve by an arbitrary ×1.2 — and it aligns with IP §10–11 (need vs
realized demand). The first link (prevalence + severity) is exactly what the DPMM
already produces; consume it rather than re-estimating (Part D).

### 6. Calibrated joint risk-factor distribution, not invented marginals

`generate_population()` samples age, then draws obesity, parity, hysterectomy, and
race independently with hand-tuned formulas (e.g. `lambda = (age-25)/10` implies
implausibly high parity in older women; obesity/hysterectomy curves are ad hoc).
Two fixes:
1. Use **real ACS-PUMS women as the base cohort** (person weights `PWGTP`),
   preserving the true joint structure of age × race × education.
2. Impute UI-specific factors (BMI, parity, diabetes, hysterectomy) by
   **raking / hot-deck to BRFSS + NSFG margins**.

Independence assumptions bias utilization because the regressors are correlated
(obesity–diabetes, parity–hysterectomy–age).

### 7. Propagate parameter uncertainty, not just outcome noise

The prototype draws `rnbinom`/`rbinom` (individual stochasticity) but holds
coefficients fixed and runs a single replicate. Draw the coefficients each Monte
Carlo iteration:

```r
beta <- MASS::mvrnorm(1, coef(visit_model), vcov(visit_model))
# rebuild the linear predictor from beta this iteration; repeat 1,000×; take quantiles
```

Report intervals that combine parameter and individual-level uncertainty (IP §5).

### 8. Realistic FTE conversion

`FTE_Total = TotalSurgeries / 50` treats slings as the sole bottleneck.
Urogynecologists' work is mostly **not** slings (POP repair, urodynamics, pessaries,
Botox, PTNS, office E/M), and clinic capacity often binds before OR capacity. A large
share of UI care is delivered by generalist OB/GYN, urology, and primary care, so
demand must be **apportioned by provider type** before converting to urogyn FTE. Use
a multi-service, work-RVU productivity model with age-specific productivity — the
approach already in cliff's Module A (`urps_module_a_effective_supply`) and Module B/C.

### 9. Decide whether the model is dynamic

Independent annual cross-sections are not a microsimulation in the usual sense: there
is no onset, progression, remission, or mortality, and no cohort aging. Either
(a) adopt a true multistate dynamic model — which the DPMM
(`R/05-dppm_50_year_national_incontinence.R`) already prototypes — or (b) rename this
a **static, prevalence-based demand model**. Both are defensible; the label/mechanics
mismatch is the reviewer risk.

---

## PART C — VALIDATION & GOVERNANCE

### 10. Calibrate and back-test

Anchor totals to reality before projecting: national UI office-visit counts from
MEPS, and sling volumes from claims (Wu 2011's ≈ +47% surgeries 2010→2050 is a
calibration target, not merely a citation). Then back-test: fit on ≤2010 data,
project to 2020, compare to observed national totals, and report mean absolute error
and interval coverage (IP §4, the highest value/feasibility item).

### 11. Reproducibility & provenance

- Thread a seed through every `generate_population()` / `simulate_demand()` call.
- Config-driven paths (no `read_csv("hypothetical.csv")`); register inputs the way
  cliff does via `config/cliff_paths.yml` + `wc_path()`.
- Versioned outputs + a provenance manifest with a `calibration_status` guard — reuse
  `R/export_demand_contract.R` (IP §16).
- Unit tests for each module and data-quality checks on each input.

---

## PART D — DO NOT BUILD A FIFTH DEMAND MODEL (the meta-point)

There are now several overlapping demand estimators: this HDMM, the DPMM prevalence
engine (this repo), cliff's Module B/C (Medicare procedure-based required FTE), and
cliff's D1/D2/D3 denominator sensitivity. Unreconciled, they will disagree — the same
divergence seen on the supply side (1,169 / 1,295 / 1,332 / 1,339).

The highest-leverage improvement is to **fold the HDMM into the versioned demand
contract** (`R/export_demand_contract.R`), not run it standalone:

- **Consume** the DPMM's `dpmm_demand_contract_v*.csv` (tiers 3–4: prevalence,
  severity) as the disease layer of §5, instead of re-deriving prevalence.
- **Emit** the HDMM's care-seeking and procedural output as **tiers 5–6** of the same
  contract, carrying the `calibration_status` guard, so cliff's
  `urps_demand_denominators_sensitivity.R` picks it up through the seam already wired
  (`CLIFF_USE_DPMM_DEMAND` + the `dpmm_demand_contract` path).
- **Reconcile** against cliff's Module B/C so procedural-demand numbers agree, or the
  discrepancy is documented in `URPS_DEMAND_DENOMINATOR_SENSITIVITY.md`.

This turns the HDMM from a silo into the demand-tier producer the pipeline is missing.

---

## RECOMMENDED MODULE ARCHITECTURE

1. Base cohort construction (ACS-PUMS + raked risk factors)
2. Disease layer: prevalence / type / severity (consume DPMM contract)
3. Care-seeking layer (need → demand)
4. Utilization: outpatient visits (survey-weighted NB/quasipoisson) + surgery rate (offset Poisson)
5. Provider-type apportionment + work-RVU productivity → urogyn FTE
6. Scenario engine (status quo / increased access / health improvement / training)
7. Parameter-uncertainty Monte Carlo + calibration + back-test
8. Versioned demand-contract export (tiers 5–6) + provenance manifest

Each module gets independent data-quality checks and tests.

---

## PRIORITY SCORECARD

| # | Improvement | Value | Feasibility |
|---|---|---|---|
| 1 | Fix degenerate surgery model (offset Poisson / case-control) | Very high | High |
| 2 | Survey-weighted estimation (MEPS design; NIS DISCWT) | Very high | High |
| 3 | Correct surgery data source (SASD/claims, ICD-10-PCS, outpatient) | Very high | Moderate |
| 4 | Need-vs-demand disease + care-seeking layers | Very high | Moderate |
| 5 | Parameter-uncertainty propagation | Very high | High |
| 6 | Calibration + historical back-test | Very high | Moderate |
| 7 | Calibrated joint risk-factor distribution (PUMS + raking) | High | Moderate |
| 8 | Realistic multi-service FTE conversion + provider apportionment | High | Moderate |
| 9 | Fold into the versioned demand contract (tiers 5–6) | High | High |
| 10 | Dynamic multistate disease model (or rename as static) | Moderate | Low–moderate |

---

## MINIMUM PUBLISHABLE MODEL

Base cohort from ACS-PUMS with raked risk factors; a disease layer consuming the DPMM
prevalence/severity contract; an explicit care-seeking layer; survey-weighted
outpatient utilization; an offset-Poisson surgery-rate model from an
outpatient-inclusive procedure source; provider-type apportionment to a work-RVU FTE
conversion; parameter-uncertainty Monte Carlo with reported intervals; calibration to
MEPS and claims anchors; a 2010→2020 back-test; and a versioned demand-contract export.

---

## CENTRAL ADVANCEMENT

The HDMM should not simply count potential UI care. It should estimate who has
disease, who seeks care, what care they need, which provider type delivers it, and
how much urogynecologist capacity that implies — with honest uncertainty — and hand a
single versioned demand series to the workforce models rather than becoming another
divergent estimate.

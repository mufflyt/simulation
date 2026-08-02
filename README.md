# Simulation Modeling for Urogynecology Workforce Planning

## Workforce Microsimulation

A **stochastic, individual-level microsimulation** on both the supply and the
demand side (modules `R/10`–`R/21`), built to the methodology documented in the
IHS Markit / Dall *Health Workforce Microsimulation Model* (HWMM v5.19.20) and
the published applications of it.

This is an R package, `urpssim`.

```r
# install.packages("pak")
pak::pak("mufflyt/simulation")
library(urpssim)
```

```bash
Rscript scripts/run_workforce_microsimulation_example.R          # no external data needed (~2 min)
REPRODUCIBILITY_MODE=strict Rscript scripts/run_workforce_microsimulation_example.R
```

### Checks

GitHub Actions workflows exist but are **`workflow_dispatch` only** — this account
has exhausted its Actions minutes, so nothing runs automatically. Run the
equivalents locally before merging:

```bash
Rscript -e 'devtools::test()'                                    # 233 regression guards
Rscript -e 'rcmdcheck::rcmdcheck(args = c("--no-manual", "--as-cran"))'
Rscript -e 'pkgload::load_all("."); testthat::test_file("tests/testthat/test-repo-hygiene.R", package = "urpssim")'
```

Re-enable the `push:` / `pull_request:` triggers in `.github/workflows/` when
minutes are available again.

Logging goes through `base::message()`; there is no logging-package dependency.
`mufflyaccess` is in Suggests — the package checks and tests without it, and the
tests that need it skip themselves.

### The four rules this model enforces

1. **The base-year shortfall is estimated, never assumed.** Rebasing supply and
   demand to 1.0 in the base year guarantees adequacy of 1.0 whether or not the
   workforce is short. HWMM names this as a conceptual limitation: base-year
   equilibrium "essentially presents future adequacy relative to current levels."
   `R/18-baseline_gap.R` implements the three sanctioned routes — a provider
   capacity survey, HPSA-removal counts, or an explicitly labelled assumption
   with an evidence ledger. Without one, `REPRODUCIBILITY_MODE=strict` refuses to
   run.
2. **Every supply/demand comparison has FTE on both sides.** Provider FTE divided
   by a count of prevalent cases, consultations, or procedures is dimensionally
   meaningless. `R/17-workload_to_fte.R` converts service volumes to required FTE
   through work RVUs calibrated to a base-year anchor (Dall 2013's approach), a
   staffing-ratio route (Zarek 2025), or a setting time-share route (Dall 2021).
   `compute_demand_coverage()` now errors with an explanation.
3. **FTE is an hours threshold, and hours are modelled by age and sex.** Not a
   hand-picked productivity step function. FTE thresholds are not comparable
   across studies (37.2 / 40 / 42.3 / 70 clinical hrs/wk in the four source
   models), so `restate_fte()` exists to convert between them.
4. **Retirement scenarios shift the age axis.** "Retire two years earlier/later",
   as in every published Dall-family study — not a scalar hazard multiplier,
   which distorts the shape of the curve. The scenario-registry validator rejects
   a `hazard_mult` field outright.

### Module map

| Module | Contents |
|---|---|
| `10-repro_provenance.R` | reproducibility modes, seeding, fail-closed artifact provenance |
| `11-canonical_and_joins.R` | canonical source resolver, join-safety wrappers |
| `12-provider_microsimulation.R` | stochastic supply engine + deterministic mean-field backbone |
| `13-demand_urps.R` | D1/D2/D3 demand estimands with distinct age profiles |
| `14-spatial_access_e2sfca.R` | E2SFCA / M2SFCA geographic access |
| `15-run_workforce_microsimulation.R` | orchestrator |
| `16-provider_lifecycle.R` | roster contract, hours by age×sex, retirement, career change |
| `17-workload_to_fte.R` | service basket, delegation matrix, workload→FTE |
| `18-baseline_gap.R` | base-year supply adequacy |
| `19-scenario_registry.R` | versioned supply and demand scenarios |
| `20-provider_geography.R` | entrant placement, migration, density benchmarks |
| `21-calibration_validation.R` | calibration scalars, two-method agreement, validation report |
| `22-legacy_loader.R` | ordered, collision-reporting loader for `inst/legacy/` |
| `23-cms_rvu.R` | CMS work RVUs, CPT basket, re-derivation helpers |
| `24-ssot.R` | every `mufflyaccess` contract hookup, in one place |
| `00-paths.R` | external-data path resolution (no hardcoded paths anywhere) |

### Single source of truth

`mufflyaccess` owns several quantities this package must not redefine.
`ssot_coverage_report()` lists what is owned and what is local.

| Quantity | Owner | Function |
|---|---|---|
| Base-year supply | `mufflyaccess` | `urps_count()` |
| Supply scenarios | `mufflyaccess` | `urps_scenarios()` v1.0.0 — 9 registered ids |
| Projection output shape | `mufflyaccess` | `urps_projection_schema()`, validated on export |
| PFD prevalence 65+ | `mufflyaccess` | `pfd_prevalence()` |
| Drive-time bands | `mufflyaccess` | `get_canonical_bands()` |
| Rurality | `mufflyaccess` | `rurality_from_ruca()` (RUCA ≥ 4 is rural) |
| Artifact provenance | `mufflyaccess` | `urps_provenance()`, folded into the run manifest |

**Three things the contract does not own**, despite the export names suggesting
otherwise — verified against the installed package, not inferred:

- `pfd_prevalence()` covers **65–79 and 80+ only**. Women under 65 are a large
  share of urogynecologic demand and are not in the contract. Its 65–79 band is
  also *not* this model's old 60–79 band, so the demand age bands were
  restructured to `20-39 / 40-59 / 60-64 / 65-79 / 80+` to align exactly.
  `pfd_prevalence_ownership()` labels which bands are contract values and which
  are local literals.
- `pfd_prevalence_acs_bands()` returns the same 65+ values keyed by ACS variable
  name. It solves ACS joins; it does not supply younger-age prevalence.
- `mc_weighted_ci(access, est, se, ...)` propagates ACS margins of error through
  an access surface. It is not a Monte Carlo replicate summariser and does not
  replace the quantile bands computed over simulation replicates.

The local scenario registry was versioned `1.0.0` — the same string as the
contract's, which is how silent divergence starts. It is now
`2.0.0-local-fallback` and used only when `mufflyaccess` is absent;
`assert_scenarios_registered()` refuses an id the contract does not know.

### Source models

| Source | What was taken |
|---|---|
| IHS Markit / Dall, *HWMM* v5.19.20 (2020) | overall architecture; hours-worked OLS specification (Exhibit 14); physician retirement to age 90; calibration scalars (Exhibit 11); five-step geographic allocation; validation taxonomy; the base-year-equilibrium warning |
| Dall et al., *Neurology* 2013;81:470–478 | work-RVU→FTE calibration; assumed-shortfall route with an evidence ledger; the access double-counting warning |
| Dall et al., *Am J Phys Med Rehabil* 2021;100:877–884 | capacity-survey shortfall (bottom-up FTE route); FTE = 37.2 clinical hrs/wk; setting time-share allocation |
| Forte et al., *Am J Phys Med Rehabil* 2021;100:866–876 | service-level provider-type delegation matrix (Table 4) |
| Zarek et al., *Phys Ther* 2025;105:pzaf014 | capacity-survey adequacy arithmetic; multistate-licensure de-duplication; separate under-50 career-change hazard |
| Fraher & Knapton, UNC Sheps Center (2017) | categorical FTE participation; two-model agreement as real concordance |
| ASN Data Analytics, `wf_supply_modeling` (MIT) | digitised FutureDocs FTE-probability table by single year of age and sex |

### Calibration status

Every input carries one of four tiers, reported by `calibration_status_report()`
and enforced by `assert_publishable_workload()`:

| Tier | Meaning | Gate |
|---|---|---|
| `calibrated` | anchored to an external published source | passes |
| `solved` | determined by an internal constraint, not assumed | passes |
| `derived_by_analogy` | structure from a published study in **another specialty** | needs `allow_analogy = TRUE` |
| `uncalibrated_illustrative` | placeholder | always refused |

| Input | Tier | Source |
|---|---|---|
| Work RVUs | `calibrated` | CMS PFS Relative Value File, RVU25A (2025) |
| Indirect time share (0.271) | `calibrated` | AAN 2010 Practice Profile, n=910 |
| Base-year supply | `calibrated` | `mufflyaccess` URPS contract |
| Hours intercept | `solved` | set so the base-year cohort mean equals 37.2 clinical hrs/wk |
| Service case mix | `derived_by_analogy` | declared CPT mix; replace with claims-derived shares |
| Delegation shares | `derived_by_analogy` | Forte 2021 physiatry shape, level rescaled (see below) |
| Clinical hours schedule | `derived_by_analogy` | HWSM Exhibit 14 (general internal medicine levels) |

Refresh the RVUs from a newer CMS release with `refresh_cms_work_rvu(path)`, and
diff a release against the shipped table with `verify_cms_work_rvu(path)`.

#### Two things calibration exposed

**Placeholder work RVUs were wrong by up to 44%** — cystoscopy (52000) was 2.20
against an actual 1.53, PTNS 0.45 against 0.60, prolapse 18.50 against a
mix-weighted 12.12.

**Forte's physiatry delegation shares do not transfer as a level.** Physiatry is
the primary specialty for its conditions; urogynecology is a small subspecialty
inside a much larger system, and most population-level UI care is delivered by
generalist OB/GYN, urology and primary care. The raw shares imply ~1,306
subspecialists delivering 64.5% of modelled national volume, which solves to
~17,700 work RVUs per clinical FTE — about 2.4× any published benchmark.
`implied_urps_share()` puts the consistent level at 28.0%, so the shares are
rescaled by 0.434 with the cross-service *shape* preserved. Solved productivity
then lands at 7,685 wRVU/FTE, against a benchmark median of 7,500.

`check_productivity_plausible()` enforces this permanently: because the
productivity denominator is *solved* from the base-year volumes, it silently
absorbs any error in them, and a denominator that is too high suppresses
projected demand.

Still needed to move the remaining tiers: a fielded URPS practice-capacity and
hours survey, claims-derived case mix, and national volume anchors (NAMCS/MEPS
office visits; HCUP SASD + Medicare Part B for the procedure basket — **not**
NIS, which is inpatient and carries ICD-10-PCS rather than CPT).

# To Do for DPMM:
Replace simulated data with real SWAN variables - ***DONE in `dppm_validate_SWAN_better.R`***
Calibrate transition probabilities - Currently using placeholder coefficients; need to estimate from longitudinal SWAN data
Add geographic stratification - Dall's models are county-level (HWMM builds a population file for each of the 3,142 counties); the microsimulation modules now carry state-level geography (`R/20-provider_geography.R`) but the DPMM disease layer is still population-level
Integrate healthcare utilization - The workload->FTE chain now exists (`R/17-workload_to_fte.R`); what is missing is the SWAN-derived disease layer feeding it. NOTE: vaginal parity cannot enter through the MEPS utilization regression (MEPS does not carry delivery history, and Dall's models restrict explanatory variables to those present in BOTH MEPS and the population file). Parity must enter through the disease layer instead.

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
├── R/             # package code: microsimulation modules 00, 10-22
├── man/           # roxygen-generated documentation (120 pages)
├── inst/legacy/   # original DPMM/SWAN/workforce scripts, NOT loaded by the package
├── config/        # canonical_sources, service_workload, calibration_targets, paths
├── scripts/       # runnable entry points
├── tests/         # testthat regression guards
├── .github/       # R CMD check, coverage, and repo-hygiene CI
├── DESCRIPTION, NAMESPACE, LICENSE
```

### External data

No filesystem path is hardcoded. `swan_path()`, `data_raw_path()` and
`external_path()` resolve against `SIMULATION_DATA_ROOT`, then
`config/paths.local.yml` (gitignored, per-machine), then `config/paths.yml`.
Run `check_external_data()` to see what is reachable before starting a long job.

### Legacy scripts

`inst/legacy/` holds the original DPMM, SWAN-validation and early-workforce
scripts. They are deliberately outside the package: they interleave function
definitions with top-level analysis code that reads data files. Load them with:

```r
load_legacy()                          # definitions only; touches no files
load_legacy(functions_only = FALSE)    # runs them as scripts; needs external data
```

Fifteen function names are defined in more than one of those files, so source
order used to decide silently which implementation you got. `LEGACY_CANONICAL`
now declares the owner of each, `load_legacy()` reports every redefinition, and a
test verifies the load order delivers what was declared. Within-file duplicates
are gone: identical bodies were deleted, divergent ones renamed to
`<name>_variant1` and flagged. See `inst/legacy/README.md`.

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



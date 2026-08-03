# urpssim — Urogynecology Workforce Supply and Demand Microsimulation

A stochastic, individual-level microsimulation of the supply of and demand for
urogynecology and reconstructive pelvic surgery (URPS) services in the United
States, built to the methodology documented in the IHS Markit / Dall **Health
Workforce Microsimulation Model** (HWMM v5.19.20) and its published applications
in physiatry, neurology and physical therapy.

```r
# install.packages("pak")
pak::pak("mufflyt/simulation")
library(urpssim)
```

```bash
Rscript scripts/run_workforce_microsimulation_example.R   # no external data needed (~2 min)
Rscript scripts/run_backtest_2020_to_2023.R               # historical validation
Rscript scripts/run_demand_lifecourse_example.R           # life-course demand pathway
```

Logging goes through `base::message()`; there is no logging-package dependency.
`mufflyaccess` is in Suggests — the package checks and tests without it, and the
tests that need it skip themselves.

---

## Model architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DEMAND SIDE (HDMM)                              │
│                                                                     │
│  Census NPP ──► age-band female pop ──► D1 Prevalent PFD cases     │
│  (2023 mid/lo/hi)    (5 bands)          D2 New consultations        │
│                                         D3 SUI+POP surgical volume  │
│  BRFSS 2023 ──► population cells ──►   D4 BRFSS UI care-seeking    │
│  (229k women)   (age×race×ins×income)   (survey-weighted prevalence)│
│                                                                     │
│  Reproductive life-course pathway:                                  │
│  vaginal births → PFD risk → care-seeking → referral → visits      │
│                          ▼                                          │
│               service volumes × wRVU basket                         │
│                          ▼                                          │
│              required FTE (solved, not assumed)                     │
└─────────────────────────────────────────────────────────────────────┘
                               │
                    SUPPLY/DEMAND GAP
                     (FTE on both sides)
                               │
┌─────────────────────────────────────────────────────────────────────┐
│                     SUPPLY SIDE (HWSM)                              │
│                                                                     │
│  Provider roster ──► Fraher agent cohort (n≈1306)                  │
│  (ABOG + ABU)         age × sex × census division                   │
│                              ▼                                      │
│  Each annual step:   Weibull survival curve ──► exit draw          │
│                      HRSA hours by age/sex ──► clinical FTE        │
│                      migration matrix ──────► geography             │
│                      +new fellows ──────────► entrant draw          │
│                              ▼                                      │
│              effective supplied FTE (MC median + 95% PI)            │
│                                                                     │
│  Scenario levers: entrant rate / Weibull scale (±2 yr) / hours     │
└─────────────────────────────────────────────────────────────────────┘
```

### Demand pipeline (D1–D4)

```
Census NPP female population (5 age bands, 2025–2050)
    │
    ├─ × PFD prevalence (Nygaard 2008, age-specific)  ──► D1 prevalent cases
    │
    ├─ × consult rate (Kirby 2013, age-specific)       ──► D2 new consultations
    │
    ├─ × surgery rate/1000 (Wu 2011, age-specific)     ──► D3 SUI+POP procedures
    │
    └─ × BRFSS UI prevalence × care-seeking × referral ──► D4 survey-weighted

All four ──► assert_estimands_independent() ──► concordance assessment
D2 + D3 ──► service basket × wRVU ──► required FTE
```

### Supply pipeline

```
ABOG+ABU roster (aggregate counts, 2023)
    │
    └─► certification cohorts ──► Fraher agent table
            651 recent (2014-2023, mean age 39.5)
            655 legacy  (≤2013, mean age 54.4)
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
  Annual advance:         Weibull retirement curve
  age += 1                  shape ~ 2.0, scale ~ 68–70
  HRSA FTE weight           scenarios shift scale ±2 yr
  migration draw
  new entrant draw
         │
         └──► effective_fte (n_active × mean_clinical_fte)
```

---

## The rules this model enforces

**1. The base-year shortfall is estimated, never assumed.** Rebasing supply and
demand to 1.0 in the base year guarantees adequacy of 1.0 whether or not the
workforce is short. The HWMM documentation names this as a conceptual limitation:
base-year equilibrium *"essentially presents future adequacy relative to current
levels."* `R/18-baseline_gap.R` implements the three sanctioned routes — a
provider capacity survey, HPSA-removal counts, or a labelled assumption with an
evidence ledger. Without one, `REPRODUCIBILITY_MODE=strict` refuses to run.

**2. Every supply/demand comparison has FTE on both sides.** Provider FTE divided
by a count of prevalent cases, consultations, or procedures is dimensionally
meaningless. `R/17-workload_to_fte.R` converts service volumes to required FTE
through work RVUs calibrated to a base-year anchor. `compute_demand_coverage()`
now errors with an explanation.

**3. FTE is an hours threshold, and hours vary by age *and* sex.** Not a
hand-picked productivity step function. Thresholds are not comparable across
studies (37.2 / 40 / 42.3 / 70 clinical hrs/wk in the four source models), so
`restate_fte()` exists to convert between them.

**4. Retirement scenarios shift the Weibull scale, not a binary year.** Each
provider exits at a draw from a Weibull survival curve (shape ≈ 2.0, scale ≈
68–70 by sex/board). Scenarios shift the scale parameter (±2 yr = scale ± 2),
preserving the stochastic shape of the curve. The legacy ±2 yr deterministic
shift is available for comparison but fails `assert_survival_curve_used()` in
strict mode. The scenario validator rejects a `hazard_mult` field outright.

**5. Reported intervals must carry forecast uncertainty.** The engine redraws
parameters each Monte Carlo iteration. Running with fixed parameters is refused
in strict mode, because the back-test showed such intervals are 6.5–8.2× too
narrow.

---

## Historical validation

`docs/BACKTEST_2020_TO_2023.md` — fit on information available through 2020 only,
project 2021–2023, score against an observed count the model never saw. Leakage
is prevented mechanically: every contract read is audited and
`assert_no_leakage()` fails if any read reached the validation window.

**The back-test failed in all eight arms.** The best arm predicted **1,195
against an observed 1,306 (−8.5%)**, and the observed value fell outside the 95%
interval everywhere. Two distinct causes, both reported honestly:

- Certification more than doubled in the unseen window (40/48/**10** per year
  pre-cutoff against 81/54/72 after). No model fitted on 2018–2020 could
  anticipate a COVID trough followed by backlog clearance.
- The intervals were far too narrow. Adding parameter uncertainty widened them
  3.7× and improved coverage from ~6.5× too narrow to ~1.7×, **without moving the
  point estimate** — but it still does not cover. The residual is structural
  break, not sampling error.

### Rolling-origin interval coverage

`R/41-interval_coverage.R` implements a rigorous leave-future-out coverage
assessment. Rather than a single train/test split, `rolling_origin_coverage()`
replicates the forecast problem across all available origin windows and measures
empirical interval coverage:

```
observed series:  2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023
fold 1 train: ────────────────────────────────────────────────
              └─── fit through 2017 ───┘ predict 2018-2020 → scored

fold 2 train: ───────────────────────────────────────────────────────
              └─── fit through 2018 ────┘ predict 2019-2021 → scored

fold 3 train: ──────────────────────────────────────────────────────────────
              └─── fit through 2019 ─────┘ predict 2020-2022 → scored
```

`solve_interval_inflation()` finds the smallest inflation factor such that
empirical coverage reaches the nominal level. `assert_interval_coverage_publishable()`
gates on ≥3 folds and a coverage ratio below a ceiling before results may be
reported.

### What the back-test did *not* test

It scored **headcount only**. The deliverable is `fte_gap`, and three of its four
components were never validated:

| Component | Back-tested? |
|---|---|
| Provider headcount | **yes** — −8.5%, outside the 95% interval |
| headcount → supplied FTE | no — the hours schedule is `derived_by_analogy` |
| Required FTE | no |
| The gap itself | no |

---

## Which inputs actually move the answer

Because `wrvu_per_fte` is *solved* against the base-year anchor, several inputs
that look alarming cancel out. Measured, not asserted — `test-workload-to-fte.R`
locks each of these:

| Perturbation | Effect on required FTE |
|---|---|
| All service volumes ×2 or ×0.5 | **exactly none** — bit-identical |
| Uniform 20% cut to every URPS delegation share | **exactly none** |
| Tripling one service (mix shift) | ≤ 0.91% on 25-year growth |
| Base-year adequacy 0.948 → 1.000 | **4.4 pp** on the 2050 gap |
| Supply error of −8.5% (the back-test's) | **6.7 pp** on the 2050 gap |
| Weibull scale ±2 yr (retirement scenario) | **~3–5 pp** on 2050 FTE |

---

## Calibration status

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
| Demand population | `calibrated` | US Census 2023 National Population Projections |
| BRFSS UI prevalence (D4) | `calibrated` | CDC BRFSS 2023 (229,541 women, survey-weighted) |
| PFD prevalence 65+ | `calibrated` | `mufflyaccess::pfd_prevalence()` |
| Indirect time share (0.271) | `calibrated` | AAN 2010 Practice Profile, n = 910 |
| Base-year supply | `calibrated` | `mufflyaccess` URPS contract |
| Hours intercept | `solved` | set so the base-year cohort mean equals 37.2 clinical hrs/wk |
| Productivity (wRVU/FTE) | `solved` | solved from the base-year anchor; **plausibility-checked** |
| Weibull retirement shape | `derived_by_analogy` | HWSM Exhibits 17–18 (general physician curves) |
| Service case mix | `derived_by_analogy` | declared CPT mix; replace with claims-derived shares |
| Delegation shares | `derived_by_analogy` | Forte 2021 physiatry shape, level rescaled |
| Clinical hours schedule | `derived_by_analogy` | HWSM Exhibit 14 (general internal medicine levels) |
| PFD prevalence < 65 | local | not in the contract; Nygaard-derived literals |

---

## Module map

| Module | Contents |
|---|---|
| `00-paths.R` | external-data path resolution (no hardcoded paths anywhere) |
| `10-repro_provenance.R` | reproducibility modes, seeding, fail-closed artifact provenance |
| `11-canonical_and_joins.R` | canonical source resolver, join-safety wrappers |
| `12-provider_microsimulation.R` | stochastic supply engine + `participation_logistic` FTE method |
| `13-demand_urps.R` | D1/D2/D3 demand estimands; `compute_brfss_demand_estimand()` (D4) |
| `13b-obstetric_exposure.R` | birth-cohort vaginal parity, obstetric-exposure estimand |
| `14-spatial_access_e2sfca.R` | E2SFCA / M2SFCA geographic access |
| `15-run_workforce_microsimulation.R` | main orchestrator; `brfss_cells` wires in D4 |
| `16-provider_lifecycle.R` | roster contract, hours by age × sex, retirement, career change |
| `17-workload_to_fte.R` | service basket, delegation matrix, workload → FTE |
| `18-baseline_gap.R` | base-year supply adequacy |
| `19-scenario_registry.R` | versioned supply and demand scenarios |
| `20-provider_geography.R` | empirical-Bayes migration matrix, origin-dependent placement |
| `21-calibration_validation.R` | calibration scalars, two-method agreement, validation report |
| `22-legacy_loader.R` | ordered, collision-reporting loader for `inst/legacy/` |
| `23-cms_rvu.R` | CMS work RVUs, CPT basket, re-derivation helpers |
| `24-ssot.R` | every `mufflyaccess` contract hookup, in one place |
| `25-demand_lifecourse.R` | reproductive life-course demand pathway |
| `26-utilization_models.R` | survey-weighted utilization and offset-Poisson rate models |
| `27-demand_lifecourse_uncertainty.R` | life-course demand prediction intervals |
| `27-workforce_concentration.R` | Herfindahl index and geographic concentration |
| `28-demand_lifecourse_calibration.R` | life-course anchoring to national totals |
| `29-demand_dynamic_multistate.R` | multistate PFD transition model |
| `30-demand_dynamic_open.R` | open-cohort dynamic demand |
| `31-dmdm_fit_transitions.R` | multistate transition fitters |
| `32-geographic_demand.R` | geographic demand apportionment |
| `33-pop_transitions.R` | population transition helpers |
| `33-roster.R` | base-year cohort from the observed certification series |
| `34-backtest.R`, `35-backtest_run.R` | leakage-free historical back-test |
| `36-parameter_uncertainty.R` | per-iteration parameter draws for the supply engine |
| `37-calibration_sources.R` | empirical `cliff` hazards, NRMP entrants, age-productivity curve |
| `38-fraher_agent_supply.R` | Fraher (2024) individual-level agent engine; `initialize_urps_agents()`, `advance_urps_agents()` |
| `38-backtest_status.R` | back-test status reporting |
| `39-cliff_retirement_hazard.R` | `build_urps_exit_hazard()` — Gompertz fit from cliff or Fraher fallback |
| `40-hrsa_fte_calibration.R` | `apply_hrsa_surgical_fte()` — HRSA hours by age/sex → relative FTE |
| `41-interval_coverage.R` | rolling-origin coverage, interval inflation solver, publication gate |
| `42-swan_incontinence_panel.R` | SWAN visit harmonisation, evidence-gated crosswalk (DAYSLEA/LEKDAYS) |
| `43-severity_sandvik.R` | Sandvik Incontinence Severity Index (frequency × amount) |
| `44-urps_population.R` | HWMM-style population file: BRFSS cells, DEMAND_AGE_BAND crosswalk, D4 prevalence weights |
| `urps_flows.R` | URPS patient flow functions for demand modeling |
| `urps_prevention.R` | DPMM-lite: conservative management diversion multipliers (PT / pessary) |
| `partial_pooling_hazard.R` | empirical-Bayes partial pooling for sparse hazard cells |
| `psa.R`, `psa_workforce.R` | joint Monte-Carlo + PRCC/SRRC global sensitivity analysis |

> The numeric prefix identifies a module uniquely within a branch. Keep it that
> way when adding one — parallel branches each taking "the next number" is how
> four of them previously collided.

---

## Single source of truth

`mufflyaccess` owns several quantities this package must not redefine.
`ssot_coverage_report()` lists what is owned and what is local.

| Quantity | Function |
|---|---|
| Base-year supply | `urps_count()` — national 1,306 / CONUS 1,303 (2023, ABOG+ABU) |
| Supply scenarios | `urps_scenarios()` v1.0.0 — 9 registered ids |
| Projection output shape | `urps_projection_schema()`, validated on export |
| PFD prevalence 65+ | `pfd_prevalence()` |
| Drive-time bands | `get_canonical_bands()` |
| Rurality | `rurality_from_ruca()` (RUCA ≥ 4 is rural) |
| Artifact provenance | `urps_provenance()`, folded into the run manifest |

---

## The base-year cohort

The contract ships **aggregate counts only** — no age, sex or state, with
`n_retired = 0` in every row — so a real roster must still come from outside it.
Two populations sit inside the 2023 total of 1,306:

| | n | Share | Mean age | Basis |
|---|---:|---:|---:|---|
| Certified 2014–2023 | 651 | 49.8% | 39.5 | **Observed** — fellowship graduates |
| Certified by 2013 | 655 | 50.2% | 54.4 | **Assumed** — initial backlog clearance |

`cohort_provenance()` refuses to call the result a roster. `initialize_urps_agents()`
builds the Fraher-style agent table from these two sub-cohorts with realistic
age × sex × census-division distributions.

---

## Retirement modeling

Retirement is drawn from a **Weibull survival curve** (`R/38-fraher_agent_supply.R`,
`R/39-cliff_retirement_hazard.R`), not a binary age-shift:

```
P(still active at age a) = exp(−(a / scale)^shape)

ABOG female:  shape ≈ 2.1,  scale ≈ 68.5  (peak exit ~65–67)
ABOG male:    shape ≈ 1.9,  scale ≈ 70.2
ABU mixed:    shape ≈ 2.0,  scale ≈ 66.0  (mixed urology practice exits earlier)
```

Scenario levers shift the `scale` parameter (±2 yr = scale ± 2), which moves
the median retirement age while preserving the stochastic spread of the curve.
The cliff DuckDB, when available, fits the shape and scale from observed ABOG
departure events; otherwise the published HWSM Exhibit 17–18 analogy values are
used with `derived_by_analogy` tier.

```
Survival probability by age (schematic):

P(active)
1.0 ┤
    │▓▓▓▓▓▓▓▓▓▓▓▓▓
0.8 ┤             ▓▓▓▓
    │                 ▓▓▓
0.6 ┤                    ▓▓
    │                      ▓▓
0.4 ┤                        ▓▓
    │                          ▓▓
0.2 ┤                            ▓▓▓
    │                               ▓▓▓▓
0.0 ┤                                   ▓▓▓▓▓▓
    └───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬─▶ age
       40  45  50  55  60  65  70  75  80  85

    ── baseline (scale=68.5)
    ·· early retirement (scale=66.5, −2 yr)
    ── delayed retirement (scale=70.5, +2 yr)
```

---

## BRFSS population cells (D4)

`R/44-urps_population.R` implements the HWMM population-file architecture:

```
BRFSS 2023 (229,541 women 18+, survey-weighted)
    │
    └─► build_urps_population_cells()
         │
         ├─ age_group × race_eth × insurance × income_tier × metro × bmi_class
         ├─ pop_weight (sum of survey weights ∝ US population)
         ├─ pct_smoker, mean_children
         └─ ui/pop/fi prevalence (observed or Nygaard 2008 imputed)
                    │
         brfss_pfd_prevalence_for_demand_bands()
                    │  crosswalk: URPS bands → DEMAND bands
                    ▼
         compute_brfss_demand_estimand()
                    │  × care_seeking_rate × referral_rate × NPP population
                    ▼
              D4 time series (2025–2050)

DEMAND_AGE_BANDS crosswalk (approximate, year-width splits):
  "20-39" ← "18-34" (1.0)
  "40-59" ← "35-44" (0.5) + "45-64" (0.75)
  "60-64" ← "45-64" (0.25)
  "65-79" ← "65-74" (1.0)
  "80+"   ← "75+"   (1.0)
```

The BRFSS 2023 core file does not include the state-optional UI/POP/FI module
(BLADCON/URINCON), so `pfd_source = "imputed_nygaard_wu"` for the 2023 run. D4
still adds value: it uses BRFSS survey-weighted demographics (BMI, smoking,
income, insurance) to adjust the population weights, and will use observed
prevalence when a module-year is supplied.

---

## Prevention model (DPMM-lite)

`R/urps_prevention.R` applies conservative-management diversion multipliers to
service volumes before `convert_workload_to_fte()`, following the IHS Markit
DPMM architecture:

```
service_volumes (from example_service_volumes or lifecourse_demand_trajectory)
    │
    └─► apply_prevention_multipliers(ui_uptake, pop_uptake)
         │
         ├─ diverted UI patients: no sling, no consultation → pessary_care or ptns
         └─ diverted POP patients: no prolapse procedure, no consultation
                    │
              net service volumes → wRVU → required FTE
```

A higher `ui_uptake` reduces surgical demand but creates conservative-care visit
volume. The net FTE effect depends on the relative wRVU weights of surgery vs.
conservative care.

---

## Test suite

```
[ PASS 1197 | FAIL 0 | SKIP 2 | WARN 0 ]   (42 test files)

Key test files:
  test-38-fraher-agent-supply.R     Fraher agent engine (13 tests)
  test-interval-coverage.R          rolling-origin coverage, inflation solver (14 tests)
  test-urps-population.R            BRFSS cells, D4, crosswalk (28 tests)
  test-urps-prevention.R            DPMM-lite prevention multipliers
  test-workload-to-fte.R            sensitivity invariants (inputs that cancel)
  test-retraction-guards-10-errors.R  5 critical regression guards
  test-backtest.R                   leakage-free historical validation
```

Run locally:

```bash
Rscript -e 'devtools::test()'
Rscript -e 'rcmdcheck::rcmdcheck(args = c("--no-manual", "--as-cran"))'
```

---

## Repository layout

```
simulation/
├── R/                # 48 modules (numbered + named)
├── man/              # roxygen-generated documentation
├── inst/legacy/      # original DPMM/SWAN/workforce scripts (NOT loaded by package)
├── inst/extdata/     # cited obstetric reference data, SWAN variable map
├── config/           # canonical_sources, service_workload, calibration_targets
├── data-raw/
│   ├── brfss/        # BRFSS 2023 (manifest tracked; XPT gitignored)
│   └── nhamcs/       # NAMCS 2019 (readme tracked; data gitignored)
├── scripts/
│   ├── data_acquisition/   # 01_download_brfss.R, 02_download_acs.R, 03_download_mcbs.R, 04_download_nhamcs_namcs.R
│   └── run_*.R             # runnable entry points
├── tests/            # 1,197 testthat regression guards across 42 files
├── artifacts/        # frozen back-test outputs + provenance manifest
├── figures/          # generated figures
├── docs/             # back-test report and module documentation
└── .github/          # R CMD check, coverage, repo-hygiene CI
```

### External data

No filesystem path is hardcoded. `swan_path()`, `data_raw_path()` and
`external_path()` resolve against `SIMULATION_DATA_ROOT`, then
`config/paths.local.yml` (gitignored, per-machine), then `config/paths.yml`.
Run `check_external_data()` before starting a long job.

---

## Data sources

| Source | Use |
|---|---|
| CMS Physician Fee Schedule RVU file | work RVUs for the service basket |
| US Census 2023 National Population Projections | demand denominator by age band (D1–D3) |
| CDC BRFSS 2023 (229,541 women) | D4 survey-weighted UI prevalence; population cell table |
| `mufflyaccess` URPS contract | base-year supply, scenarios, PFD prevalence, provenance |
| CDC/NCHS natality and Census fertility series | birth-cohort vaginal parity |
| NAMCS 2019 | utilization anchors (download script provided) |
| SWAN (Study of Women's Health Across the Nation) | incontinence panel (`R/42-swan_incontinence_panel.R`) |
| MEPS / NHAMCS / HCUP SASD | procedure anchors (**download scripts provided; not yet wired**) |

---

## What is still missing

Ordered by how much each actually moves the deliverable:

1. **No URPS capacity survey.** The base-year adequacy is a physical-therapy
   distribution, and it passes straight through to the headline gap with a
   coefficient of one. Highest-value missing input by a wide margin.
2. **The headcount → FTE step is unvalidated.** The hours schedule comes from
   general internal medicine and drifts FTE-per-head ~3% over the horizon.
3. **No individual provider roster.** The contract ships aggregate counts only,
   so half the base cohort's ages are assumed.
4. **Weibull shape/scale unvalidated for URPS.** Currently `derived_by_analogy`
   from HWSM general physician curves. ABOG departure data would sharpen both
   parameters and `hazard_cv`.
5. **BRFSS UI/POP/FI module absent in 2023 core.** D4 uses imputed Nygaard
   prevalence. Wiring a module-year (e.g., 2016 or a state that opted in) would
   move D4 to `brfss_observed`.
6. **Service volumes and the case mix are illustrative** — but see the sensitivity
   table above: the *level* cancels entirely and a mix shift moves 25-year growth
   by under 1%.

---

## Source models

| Source | What was taken |
|---|---|
| IHS Markit / Dall, *HWMM* v5.19.20 (2020) | architecture; hours-worked OLS (Exhibit 14); Weibull retirement (Exhibits 17–18); five-step geographic allocation; base-year-equilibrium warning |
| Dall et al., *Neurology* 2013;81:470–478 | work-RVU → FTE calibration; assumed-shortfall route; access double-counting warning |
| Dall et al., *Am J Phys Med Rehabil* 2021;100:877–884 | capacity-survey shortfall; FTE = 37.2 clinical hrs/wk |
| Forte et al., *Am J Phys Med Rehabil* 2021;100:866–876 | service-level provider-type delegation matrix (Table 4) |
| Zarek et al., *Phys Ther* 2025;105:pzaf014 | capacity-survey adequacy arithmetic; multistate-licensure de-duplication |
| Fraher & Knapton, UNC Sheps Center (2017) | categorical FTE participation; individual-level agent engine |
| ASN Data Analytics, `wf_supply_modeling` (MIT) | digitised FutureDocs FTE-probability table by age and sex |
| Nygaard et al., *JAMA* 2008;300:1311 | PFD prevalence by age band (D1, D4 imputation fallback) |
| Wu et al., *Obstet Gynecol* 2014;123:697 | PFD prevalence forecasting; surgery rates (D3) |
| Sandvik et al., *Scand J Prim Health Care* 1993 | Incontinence Severity Index (frequency × amount) |

---

## Citation

`citation("urpssim")` returns the software entry plus the four methodology papers
it implements. If you report a projection or a supply/demand gap, state the
calibration tier of the inputs — `calibration_status_report()` prints it.

## License

MIT © 2026 Tyler Muffly. See `LICENSE.md`.

## Maintainer

Tyler Muffly, MD — Denver Health | Urogynecology
[github.com/mufflyt](https://github.com/mufflyt)

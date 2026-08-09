# Architecture & Code Map

A developer- and reviewer-oriented map of the `urpssim` package: where every
piece of code lives, how a run threads through it, and where to start reading.

This is the **code view**. For the *scientific* view — what the model claims,
which coefficient came from which paper, and how the gap is defined — read the
[README](../README.md) "Orientation" and "Model architecture" sections and
[`DEMAND_METHODS.md`](DEMAND_METHODS.md). This document assumes you have skimmed
those and now want to find the function.

---

## 1. The one-paragraph mental model

The package answers one question — *will the US supply of urogynecology / URPS
providers keep pace with demand, and where will access fall short?* — by
projecting **supply** and **demand** in the **same FTE units** and reporting the
gap with an uncertainty interval, then optionally distributing that gap over
**geography**. So the code has three substantive layers feeding a fourth:

```
  supply-*   ──►  provider FTE supply (Monte-Carlo median + 95% PI)
  demand-*   ──►  required FTE from service volumes × wRVU basket        ─┐
  geography-*──►  where supply and demand sit, and drive-time access      ├─► gap
                                                                          │   +
  reporting-*──►  the gap, baseline adequacy, scenarios, export contract ─┘  access
```

Everything else is support: `core-*` (paths, provenance, the SSOT contract,
the orchestrator), `calibration-*` (anchoring the free parameters to published
data and quantifying their uncertainty), `validation-*` (the leakage-free
back-test and the pre-registered scoring protocol), and `data-*` (bundled
reference inputs — RVUs, SWAN, MEPS, population cells).

---

## 2. Entry points

Start a run through one of these. Everything downstream is a library call.

| You want to… | Call / run | Notes |
|---|---|---|
| A full supply projection, no external data | `scripts/run_supply_microsimulation_example.R` | ~2 min, bundled params |
| The direct supply engine | `run_supply_microsimulation()` | `R/supply-provider_microsimulation.R` |
| The whole supply+demand orchestrator | `run_workforce_microsimulation()` | `R/core-run_workforce_microsimulation.R:241` |
| A life-course demand pathway | `scripts/run_demand_lifecourse_example.R` | births → PFD → visits → FTE |
| The demand pipeline end-to-end | `scripts/run_demand_pipeline.R` | D1–D4 |
| The historical back-test | `scripts/run_backtest_2020_to_2023.R` | leakage-free 2020→2023 |
| Global sensitivity (PSA) | `scripts/run_psa_example.R` | PRCC/SRRC |
| A run against a real roster | `scripts/run_with_production_roster.R` | needs `mufflyaccess` |

All logging is `base::message()`; there is no logging dependency. `mufflyaccess`
is in **Suggests** — the package checks and tests without it, and the tests that
need it skip themselves.

---

## 3. The nine code families

82 files in `R/`, prefixed by family. The **numeric-free conceptual prefix is
the unit** — keep new files inside a family, and **never** reintroduce a numeric
prefix (parallel branches each grabbing "the next number" is how four modules
previously collided).

### `core-*` — orchestration, paths, provenance, the contract (7)

| File | Purpose |
|---|---|
| `core-run_workforce_microsimulation.R` | **Main orchestrator** — `run_workforce_microsimulation()`; wires D4 `brfss_cells` in, loads modules in dependency order |
| `core-ssot.R` | Every `mufflyaccess` contract hookup in one place — `has_mufflyaccess()`, `ssot_coverage_report()`, the single source of truth for owned quantities |
| `core-canonical_and_joins.R` | Fail-closed canonical-source resolver (`resolve_canonical()`) + join-safety wrappers |
| `core-repro_provenance.R` | Reproducibility modes (`strict`/`relaxed`), seeding, fail-closed artifact provenance |
| `core-paths.R` | External-data path resolution — no hardcoded paths anywhere in the tree |
| `core-legacy_loader.R` | Ordered, collision-reporting loader for `inst/legacy/` |
| `core-contract_pin.R` | The `mufflyaccess` commit the build is pinned to (checked against DESCRIPTION) |

### `supply-*` — the provider-supply engine (18)

| File | Purpose |
|---|---|
| `supply-provider_microsimulation.R` | Stochastic supply engine — `run_supply_microsimulation()`, per-subspecialty baseline retirement |
| `supply-fraher_agent_supply.R` | Fraher (2024) individual-agent engine — `initialize_urps_agents()`, `advance_urps_agents()` |
| `supply-provider_lifecycle.R` | Roster contract, FTE definition, hours by age×sex, retirement, career change, FutureDocs participation |
| `supply-provider_state_machine.R` | Career-state machine (Resident→Fellow→Early→Mid→Late→Retired), per-transition calibration tier, `career_state_of()` |
| `supply-retirement_hazard.R` | Weibull discrete annual exit probabilities — `build_urps_exit_hazard()` (Gompertz fit from cliff or Fraher fallback) |
| `supply-partial_pooling_hazard.R` | Empirical-Bayes partial pooling for sparse hazard cells |
| `supply-roster.R` | Base-year cohort from the observed certification series |
| `supply-roster_capacity.R` | Load the URPS provider roster into a capacity table |
| `supply-capacity_hierarchy.R` | Tiered capacity: headcount → clinical FTE → wRVU → accessible |
| `supply-workload_to_fte.R` | Service basket, delegation matrix, workload → FTE |
| `supply-medicare_capacity.R` | Annual Medicare work-RVU totals for a known roster; realized-care crosswalk & comparison |
| `supply-delegation_evidence.R` | Advanced-practice-provider taxonomy in Medicare; delegation evidence |
| `supply-entrant_regime.R` | Annual entrant counts, audited against the back-test leakage log |
| `supply-entrant_trajectory.R` | Entrant-series trajectory helpers (CAGR between endpoints) |
| `supply-acgme_fellows.R` | ACGME URPS fellow counts by academic year and parent specialty |
| `supply-review_followups.R` | Observed NRMP-match → board-certification conversion |
| `supply-urps_flows.R` | URPS patient-flow functions; `supply_p_active()` logistic coefficients |
| `supply-urps_settings.R` | Default care-delivery setting mix per service; shared `.msg_*` / `resolve_canonical` helpers |

### `demand-*` — required-FTE from disease burden and utilization (14)

| File | Purpose |
|---|---|
| `demand-urps.R` | D1/D2/D3 estimands — PFD prevalence, consults, surgical volume; `compute_brfss_demand_estimand()` (D4) |
| `demand-lifecourse.R` | Reproductive life-course demand pathway (literature-anchored risk coefficients) |
| `demand-lifecourse_uncertainty.R` | Life-course demand prediction intervals |
| `demand-obstetric_exposure.R` | Birth-cohort vaginal parity / cesarean-rate obstetric-exposure estimand |
| `demand-utilization_models.R` | Survey-weighted utilization; offset-Poisson surgery-rate models |
| `demand-namcs_visit_equations.R` | NAMCS URPS-condition visit equations (ICD-10-CM prefixes) |
| `demand-condition_service_pathway.R` | Condition → service pathway stages, in cascade order |
| `demand-severity_sandvik.R` | Sandvik Incontinence Severity Index (frequency × amount) on a SWAN panel |
| `demand-prevention.R` | DPMM-lite — conservative-management diversion multipliers (PT / pessary) |
| `demand-dynamic_multistate.R` | Multistate PFD transition model (DMDM) |
| `demand-dynamic_open.R` | Open-cohort dynamic demand simulation |
| `demand-dmdm_fit_transitions.R` | Multistate transition fitters (panel → at-risk rows) |
| `demand-pop_transitions.R` | Cited POP transition parameters (onset + staged progression/regression) |
| `demand-transition_registry.R` | Demand-transition coefficient registry |

### `geography-*` — where supply and demand sit; drive-time access (9)

| File | Purpose |
|---|---|
| `geography-spatial_access_e2sfca.R` | E2SFCA / M2SFCA geographic access; canonical drive-time bands |
| `geography-spatial_access_data.R` | Real tract demand denominator (female 65+ with centroids) |
| `geography-demand.R` | Distribute pelvic-floor NEED across isochrone travel-time bands |
| `geography-provider_geography.R` | Empirical-Bayes migration matrix, origin-dependent placement |
| `geography-provider_coordinates.R` | Load URPS provider point locations |
| `geography-urps_migration.R` | Annual cross-state migration hazards for URPS subspecialists |
| `geography-access_clearing.R` | Clear demand against accessible capacity → patient-experienced outcomes |
| `geography-access_severity.R` | Severity/priority-stratified clearing against shared capacity |
| `geography-telemedicine_reach.R` | Telemedicine geographic-reach uplift to nonmetro catchments |

### `reporting-*` — the gap, adequacy, scenarios, export contract (8)

| File | Purpose |
|---|---|
| `reporting-baseline_gap.R` | Base-year supply adequacy — `capacity_survey_adequacy()`, `published_baseline_gaps()` |
| `reporting-baseline_gap_reporting.R` | Full provenance for a headline baseline-gap estimate |
| `reporting-scenario_registry.R` | Versioned supply & demand scenario registry (SSOT-backed + local fallback) |
| `reporting-urps_projection.R` | Validate a gap-projection data frame against the extended contract |
| `reporting-access_outcomes.R` | National roll-up of access outcomes (A1–A5) |
| `reporting-workforce_statistics.R` | Wilson-score CIs and workforce summary statistics |
| `reporting-workforce_concentration.R` | Gini / Herfindahl geographic concentration |
| `reporting-export_demand_contract.R` | Export the DPMM demand trajectory as a versioned downstream contract |

### `calibration-*` — anchor free parameters; quantify their uncertainty (9)

| File | Purpose |
|---|---|
| `calibration-sources.R` | Empirical `cliff` departure hazards, NRMP entrants, age-productivity curve |
| `calibration-validation.R` | Multiplicative calibration scalars against independent national anchors; two-method agreement |
| `calibration-parameter_uncertainty.R` | Per-iteration parameter draws for the supply engine (SE of a mean from a series) |
| `calibration-hrsa_fte.R` | `apply_hrsa_surgical_fte()` — HRSA hours by age/sex → relative FTE |
| `calibration-demand_lifecourse.R` | Anchor life-course service volumes to national totals |
| `calibration-namcs_demand.R` | Provenance of the NAMCS demand anchor |
| `calibration-psa.R` | Probabilistic sensitivity analysis — uniform PSA input definitions |
| `calibration-psa_workforce.R` | Default PSA input set for the workforce-2050 gap |
| `calibration-psa_reporting.R` | Summarise PSA outputs |

### `validation-*` — honest forward-looking evaluation (9)

| File | Purpose |
|---|---|
| `validation-backtest.R` | Leakage-free historical back-test (target-year scoring) |
| `validation-backtest_run.R` | Run one back-test arm |
| `validation-backtest_status.R` | Frozen record of the prespecified 2020→2023 back-test |
| `validation-interval_coverage.R` | Rolling-origin coverage, interval-inflation solver, publication gate |
| `validation-preregistration.R` | Preregister (freeze + hash) a model specification |
| `validation-geographic_holdout.R` | Geographic (spatial) held-out cross-validation of predicted vs observed stock |
| `validation-forecast_scorecard.R` | Weighted interval score (WIS) for quantile forecasts |
| `validation-forecast_probabilities.R` | Probability summaries of a Monte-Carlo output — `workforce_gap_probabilities()` |
| `validation-access.R` | Access-outcome external-validation targets |

### `data-*` — bundled reference inputs (7)

| File | Purpose |
|---|---|
| `data-cms_rvu.R` | CMS work RVUs, the URPS CPT basket, re-derivation helpers |
| `data-urps_population.R` | HWMM-style population file: BRFSS cells, demand age-band crosswalk, D4 weights; `project_urps_demand()` |
| `data-meps_care_seeking.R` | MEPS pelvic-floor-condition ICD-10 prefixes and care-seeking |
| `data-practice_survey.R` | The item set a fielded URPS practice survey must collect |
| `data-swan_incontinence_panel.R` | Harmonised SWAN incontinence state panel (evidence-gated DAYSLEA/LEKDAYS crosswalk) |
| `data-swan_dmdm_panel.R` | SWAN visit-to-variable crosswalk for the DMDM covariates |
| `data-swan_archive.R` | Reference checksums for the validated SWAN archive files |

### package doc

| File | Purpose |
|---|---|
| `urpssim-package.R` | Package-level roxygen (`@keywords internal`) |

---

## 4. How a supply run threads through the code

```
run_supply_microsimulation()                     supply-provider_microsimulation.R
  │
  ├─ initialize_provider_agents(n, subspec, yr)   supply-fraher_agent_supply.R
  │     └─ two sub-cohorts (recent / legacy) with age×sex×division
  │
  ├─ param_spec = supply_parameter_spec(...)       calibration-parameter_uncertainty.R
  │     └─ per-iteration entrant-rate draws (quantified 95% band component)
  │
  ├─ retirement_schedule = urps_empirical_...()    calibration-sources.R
  │     └─ cliff hazards → Weibull/Gompertz exit   supply-retirement_hazard.R
  │
  └─ for each of n_iterations, for each year:
        age += 1
        exit draw     ← retirement hazard          supply-retirement_hazard.R
        FTE weight    ← HRSA hours by age×sex       calibration-hrsa_fte.R
        migration     ← empirical-Bayes matrix      geography-provider_geography.R
        + entrants    ← entrant draw                supply-entrant_regime.R
        └─► effective_fte = n_active × mean_clinical_fte
  ▼
sim$summary (median + 95% PI)  ─►  workforce_gap_probabilities()   validation-forecast_probabilities.R
                               ─►  baseline adequacy               reporting-baseline_gap.R
                               ─►  gap projection (validated)      reporting-urps_projection.R
```

The demand side runs in parallel (`project_urps_demand()` → service volumes ×
wRVU basket → required FTE) and the two meet in the same FTE units; the
`geography-*` layer then distributes the gap across drive-time bands.

---

## 5. The contract boundary (`mufflyaccess` SSOT)

Several quantities are **owned** by the private `mufflyaccess` data package and
must not be redefined here. They are all reached through `core-ssot.R`;
`ssot_coverage_report()` lists what is owned vs local.

| Owned quantity | Accessor |
|---|---|
| Base-year supply (national 1,306 / CONUS 1,303, 2023 ABOG+ABU) | `urps_count()` |
| Supply scenarios (v1.0.0, registered ids) | `urps_scenarios()` |
| Projection output shape | `urps_projection_schema()` |
| PFD prevalence 65+ | `pfd_prevalence()` |
| Drive-time bands | `get_canonical_bands()` |
| Rurality (RUCA ≥ 4 = rural) | `rurality_from_ruca()` |
| Artifact provenance | `urps_provenance()` |

When `mufflyaccess` is absent, the scenario registry falls back to a local
definition (`supply_scenario_registry(..., prefer_ssot = FALSE)`) and the
SSOT-dependent tests skip themselves.

---

## 6. Reading paths

**Lost in the jargon?** [`GLOSSARY.md`](GLOSSARY.md) defines the workforce,
epidemiology, and credentialing terms (FTE, wRVU, E2SFCA, HWSM, DMDM,
calibration tiers, SSOT, PFD/SUI/POP, …) the code and help pages assume.

**New scientist / reviewer** — README "Orientation" → "Model architecture" →
[`DEMAND_METHODS.md`](DEMAND_METHODS.md) → [`BENCHMARKS.md`](BENCHMARKS.md) (the
values the model must reproduce) → [`BACKTEST_2020_TO_2023.md`](BACKTEST_2020_TO_2023.md).

**New developer** — this doc → §2 entry points → run
`scripts/run_supply_microsimulation_example.R` → read
`supply-provider_microsimulation.R` following §4 → `core-ssot.R` for the
contract boundary → `tests/testthat/` for the executable spec.

**"Where does this number come from?"** — README "Single source of truth" and
`ssot_coverage_report()` for owned quantities; `published_baseline_gaps()` /
[`BENCHMARKS.md`](BENCHMARKS.md) for the published anchors; the per-coefficient
citations in `inst/extdata/` for demand.

---

## 7. Conventions that bite if ignored

- **Numeric-free family prefix is the unit.** Add files inside a family; never
  reintroduce a numeric prefix. Parallel branches taking "the next number"
  collided four times.
- **Uncalibrated coefficients refuse to be reported.** Calibration tiers
  (`calibrated` / `solved` / `derived_by_analogy` / `uncalibrated_illustrative`)
  gate what may appear as a result; the publication guards enforce this.
- **Fail-closed provenance.** Derived artifacts carry a provenance sidecar and
  reject on load if inputs, code, or content hashes drift (`core-repro_provenance.R`).
- **roxygen is hand-synced here.** The pinned roxygen2 cannot regenerate `.Rd`
  in this environment, so `#'` blocks in `R/` and the `man/*.Rd` files are edited
  **together** by hand. After editing an `.Rd`, validate it with
  `tools::checkRd("man/<file>.Rd")` — codoc compares `\usage` to the function
  formals, so keep those in lockstep.
- **CI is two gates.** `R-CMD-check` (`R CMD check --as-cran`, `error_on:
  warning` — a WARNING fails) and `repo-hygiene` (`test-repo-hygiene.R`: no
  duplicate definitions, skip budget, etc.). CI checks the PR **merged with
  latest main**, so rebase before trusting a green.

---

*Keep this map current when you add or move a module — a stale map sends the
next reader to a file that no longer exists.*

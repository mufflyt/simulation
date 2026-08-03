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

**4. Retirement scenarios shift the age axis.** "Retire two years earlier/later",
as in every published Dall-family study — not a scalar hazard multiplier, which
distorts the shape of the curve. The scenario validator rejects a `hazard_mult`
field outright.

**5. Reported intervals must carry forecast uncertainty.** The engine redraws
parameters each Monte Carlo iteration. Running with fixed parameters is refused
in strict mode, because the back-test showed such intervals are 6.5–8.2× too
narrow.

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

Read projected intervals from this engine with that result in mind.

### What the back-test did *not* test

It scored **headcount only**. The deliverable is `fte_gap`, and three of its four
components were never validated:

| Component | Back-tested? |
|---|---|
| Provider headcount | **yes** — −8.5%, outside the 95% interval |
| headcount → supplied FTE | no — the hours schedule is `derived_by_analogy` and drifts FTE-per-head ~3% over the horizon |
| Required FTE | no |
| The gap itself | no |

So the back-test validates roughly one component of four. Treating it as a
verdict on the whole model overstates what it covers.

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

Two consequences worth stating plainly:

**The base-year gap is a pass-through, not a model output.** `gap% = −(1 −
adequacy)` to the decimal. The headline base-year number *is* the capacity-survey
estimate, with a coefficient of one — and that estimate is currently a
**physical-therapy** distribution standing in for urogynecology. It is the single
largest unmeasured input.

**The 0.434 delegation rescaling does not move the gap.** It matters for
interpretability and for making the solved productivity plausible, but a uniform
rescaling cancels through the solved denominator. The same goes for the level of
the illustrative service volumes.

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
| PFD prevalence 65+ | `calibrated` | `mufflyaccess::pfd_prevalence()` |
| Indirect time share (0.271) | `calibrated` | AAN 2010 Practice Profile, n = 910 |
| Base-year supply | `calibrated` | `mufflyaccess` URPS contract |
| Hours intercept | `solved` | set so the base-year cohort mean equals 37.2 clinical hrs/wk |
| Productivity (wRVU/FTE) | `solved` | solved from the base-year anchor; **plausibility-checked** |
| Service case mix | `derived_by_analogy` | declared CPT mix; replace with claims-derived shares |
| Delegation shares | `derived_by_analogy` | Forte 2021 physiatry shape, level rescaled |
| Clinical hours schedule | `derived_by_analogy` | HWSM Exhibit 14 (general internal medicine levels) |
| PFD prevalence < 65 | local | not in the contract; Nygaard-derived literals |

Refresh RVUs from a newer CMS release with `refresh_cms_work_rvu(path)`; diff a
release against the shipped table with `verify_cms_work_rvu(path)`.

### Two things calibration exposed

**Placeholder work RVUs were wrong by up to 44%** — cystoscopy (52000) was 2.20
against an actual 1.53, PTNS 0.45 against 0.60, prolapse 18.50 against a
mix-weighted 12.12.

**Forte's physiatry delegation shares do not transfer as a level.** Physiatry is
the primary specialty for its conditions; urogynecology is a small subspecialty
inside a much larger system. The raw shares imply ~1,306 subspecialists
delivering 64.5% of modelled national volume, solving to ~17,700 work RVUs per
clinical FTE — about 2.4× any published benchmark. `implied_urps_share()` puts
the consistent level at 28.0%, so the shares are rescaled by 0.434 with the
cross-service *shape* preserved. `check_productivity_plausible()` enforces this
permanently: the productivity denominator is *solved*, so it silently absorbs any
error in the volumes, and one that is too high suppresses projected demand.

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

**Three things the contract does not own**, despite the export names suggesting
otherwise — verified against the installed package, not inferred:

- `pfd_prevalence()` covers **65–79 and 80+ only**. Demand age bands were
  restructured to `20-39 / 40-59 / 60-64 / 65-79 / 80+` to align exactly;
  `pfd_prevalence_ownership()` labels which bands are contract values.
- `pfd_prevalence_acs_bands()` returns the same 65+ values keyed by ACS variable
  name. It solves ACS joins, not younger-age prevalence.
- `mc_weighted_ci()` propagates ACS margins of error through an access surface.
  It is not a Monte Carlo replicate summariser.

The local scenario registry was versioned `1.0.0` — the same string as the
contract's, which is how silent divergence starts. It is now
`2.0.0-local-fallback`; `assert_scenarios_registered()` refuses an unregistered id.

## The base-year cohort

The contract ships **aggregate counts only** — no age, sex or state, with
`n_retired = 0` in every row — so a real roster must still come from outside it.
But the certification series supports far better than a normal draw. Two
populations sit inside the 2023 total of 1,306:

| | n | Share | Mean age | Basis |
|---|---:|---:|---:|---|
| Certified 2014–2023 | 651 | 49.8% | 39.5 | **Observed** — fellowship graduates |
| Certified by 2013 | 655 | 50.2% | 54.4 | **Assumed** — initial backlog clearance |

One `rnorm(52, 9)` was wrong for both. `cohort_provenance()` refuses to call the
result a roster. The same series also reconciles the entrant rate:
`implied_gross_entrants()` warns when the shipped assumption is more than 15% off,
which it currently is.

## Module map

| Module | Contents |
|---|---|
| `00-paths.R` | external-data path resolution (no hardcoded paths anywhere) |
| `10-repro_provenance.R` | reproducibility modes, seeding, fail-closed artifact provenance |
| `11-canonical_and_joins.R` | canonical source resolver, join-safety wrappers |
| `12-provider_microsimulation.R` | stochastic supply engine + deterministic backbone |
| `13-demand_urps.R` | D1/D2/D3 demand estimands with distinct age profiles |
| `13b-obstetric_exposure.R` | birth-cohort vaginal parity, obstetric-exposure estimand (D4) |
| `14-spatial_access_e2sfca.R` | E2SFCA / M2SFCA geographic access |
| `15-run_workforce_microsimulation.R` | orchestrator |
| `16-provider_lifecycle.R` | roster contract, hours by age × sex, retirement, career change |
| `17-workload_to_fte.R` | service basket, delegation matrix, workload → FTE |
| `18-baseline_gap.R` | base-year supply adequacy |
| `19-scenario_registry.R` | versioned supply and demand scenarios |
| `20-provider_geography.R` | entrant placement, migration, density benchmarks |
| `21-calibration_validation.R` | calibration scalars, two-method agreement, validation report |
| `22-legacy_loader.R` | ordered, collision-reporting loader for `inst/legacy/` |
| `23-cms_rvu.R` | CMS work RVUs, CPT basket, re-derivation helpers |
| `24-ssot.R` | every `mufflyaccess` contract hookup, in one place |
| `25-demand_lifecourse.R` | reproductive life-course demand pathway |
| `26-utilization_models.R` | survey-weighted utilization and offset-Poisson rate models |
| `27-demand_lifecourse_uncertainty.R` | life-course demand prediction intervals |
| `28-demand_lifecourse_calibration.R` | life-course anchoring to national totals |
| `33-roster.R` | base-year cohort from the observed certification series |
| `34-backtest.R`, `35-backtest_run.R` | leakage-free historical back-test |
| `36-parameter_uncertainty.R` | per-iteration parameter draws for the supply engine |
| `37-calibration_sources.R` | empirical `cliff` hazards, NRMP entrants, age-productivity curve |

> `25`-`32` are the demand life-course chain and its dynamic extensions, which
> read as a sequence; `33`+ are the supply-side roster, back-test and
> uncertainty modules. The prefix identifies a module uniquely — keep it that
> way when adding one, since parallel branches each taking "the next number" is
> how four of them previously collided.

## Source models

| Source | What was taken |
|---|---|
| IHS Markit / Dall, *HWMM* v5.19.20 (2020) | architecture; hours-worked OLS (Exhibit 14); physician retirement to age 90; calibration scalars (Exhibit 11); five-step geographic allocation; validation taxonomy; the base-year-equilibrium warning |
| Dall et al., *Neurology* 2013;81:470–478 | work-RVU → FTE calibration; assumed-shortfall route; the access double-counting warning |
| Dall et al., *Am J Phys Med Rehabil* 2021;100:877–884 | capacity-survey shortfall; FTE = 37.2 clinical hrs/wk |
| Forte et al., *Am J Phys Med Rehabil* 2021;100:866–876 | service-level provider-type delegation matrix (Table 4) |
| Zarek et al., *Phys Ther* 2025;105:pzaf014 | capacity-survey adequacy arithmetic; multistate-licensure de-duplication; separate under-50 career-change hazard |
| Fraher & Knapton, UNC Sheps Center (2017) | categorical FTE participation; two-model agreement as real concordance |
| ASN Data Analytics, `wf_supply_modeling` (MIT) | digitised FutureDocs FTE-probability table by single year of age and sex |

## Repository layout

```
simulation/
├── R/             # package code: 27 modules
├── man/           # roxygen-generated documentation (198 pages)
├── inst/legacy/   # original DPMM/SWAN/workforce scripts, NOT loaded by the package
├── inst/extdata/  # cited obstetric reference data
├── config/        # canonical_sources, service_workload, calibration_targets, paths
├── data-raw/      # Census NPP inputs (build-ignored, git-tracked)
├── artifacts/     # frozen back-test outputs + provenance manifest
├── figures/       # generated figures
├── docs/          # back-test report and module documentation
├── scripts/       # runnable entry points
├── tests/         # 566 testthat regression guards across 18 files
└── .github/       # R CMD check, coverage, and repo-hygiene CI
```

### Checks

GitHub Actions workflows exist but are **`workflow_dispatch` only** — this
account has exhausted its Actions minutes, so nothing runs automatically. Run the
equivalents locally before merging:

```bash
Rscript -e 'devtools::test()'                                    # 566 regression guards
Rscript -e 'rcmdcheck::rcmdcheck(args = c("--no-manual", "--as-cran"))'
Rscript -e 'pkgload::load_all("."); testthat::test_file("tests/testthat/test-repo-hygiene.R", package = "urpssim")'
```

Re-enable the `push:` / `pull_request:` triggers in `.github/workflows/` when
minutes are available.

### External data

No filesystem path is hardcoded. `swan_path()`, `data_raw_path()` and
`external_path()` resolve against `SIMULATION_DATA_ROOT`, then
`config/paths.local.yml` (gitignored, per-machine), then `config/paths.yml`.
Run `check_external_data()` before starting a long job.

### Legacy scripts

`inst/legacy/` holds the original DPMM, SWAN-validation and early-workforce
scripts. They sit outside the package because they interleave function
definitions with top-level analysis code that reads data files.

```r
load_legacy()                          # definitions only; touches no files
load_legacy(functions_only = FALSE)    # runs them as scripts; needs external data
```

Fifteen function names are defined in more than one of those files, so source
order used to decide silently which implementation you got. `LEGACY_CANONICAL`
declares the owner of each and a test verifies the load order delivers it.
Within-file duplicates are gone: identical bodies deleted, divergent ones renamed
to `<name>_variant1` and flagged. See `inst/legacy/README.md`.

## Data sources

| Source | Use |
|---|---|
| CMS Physician Fee Schedule RVU file | work RVUs for the service basket |
| US Census 2023 National Population Projections | demand denominator by age band |
| `mufflyaccess` URPS contract | base-year supply, scenarios, PFD prevalence, provenance |
| CDC/NCHS natality and Census fertility series | birth-cohort vaginal parity |
| MEPS / NAMCS / NHAMCS / HCUP SASD | utilization and procedure anchors (**not yet wired**) |
| SWAN, BRFSS, NHANES | legacy DPMM disease layer (`inst/legacy/`) |

## What is still missing

Ordered by how much each actually moves the deliverable, which is not the order
you would guess:

1. **No URPS capacity survey.** The base-year adequacy is a physical-therapy
   distribution, and it passes straight through to the headline gap with a
   coefficient of one. Highest-value missing input by a wide margin.
2. **The headcount → FTE step is unvalidated.** The hours schedule comes from
   general internal medicine and drifts FTE-per-head ~3% over the horizon. The
   back-test never scored it.
3. **No individual provider roster.** The contract ships aggregate counts only,
   so half the base cohort's ages are assumed.
4. **Retirement and hours uncertainty are unquantified.** Both are published as
   point estimates with no standard errors; `hazard_cv` defaults to zero and
   says so, rather than inventing a spread.
5. **Service volumes and the case mix are illustrative** — but see the
   sensitivity table above: the *level* cancels entirely and a mix shift moves
   25-year growth by under 1%. Worth fixing for credibility; not what is
   distorting the number. NIS is the wrong instrument for outpatient slings
   (inpatient, ICD-10-PCS not CPT) when they are replaced.

## Citation

`citation("urpssim")` returns the software entry plus the four methodology papers
it implements. If you report a projection or a supply/demand gap, state the
calibration tier of the inputs — `calibration_status_report()` prints it.

## License

MIT © 2026 Tyler Muffly. See `LICENSE.md`.

## Maintainer

Tyler Muffly, MD — Denver Health | Urogynecology
[github.com/mufflyt](https://github.com/mufflyt)

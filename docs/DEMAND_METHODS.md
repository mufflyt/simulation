# URPS Demand Model — Methods

A manuscript-oriented description of the demand side of `urpssim`. The demand
model estimates the need for, and utilization of, urogynecologic / reconstructive
pelvic surgery (URPS) care for pelvic-floor disorders (urinary incontinence [UI],
pelvic organ prolapse [POP], anal/fecal incontinence [AI]) and converts it to
required full-time-equivalent (FTE) providers in the same units as the supply
projection.

> Status: the coefficient tables shipped in the package are explicit
> placeholders (`calibration_status = "placeholder_uncalibrated"`). This document
> describes the *methods and their data provenance*; numbers become results only
> after the transition equations are fitted and the base year is calibrated.

## 1. Architecture (Zarek 2025 / Dall HWMM)

Demand follows the health-workforce-demand architecture of Dall's IHS Markit
Health Workforce Microsimulation Model, as applied by Zarek et al. (2025):

    population → predicted service use → staffing conversion → provider FTE

Crucially, provider demand is **not** read off disease prevalence. Prevalence
passes through a care pathway to service use, and service use is converted to FTE
through a work-RVU / staffing model. The package carries this out in two
complementary ways — a reproductive **life-course** pathway and a **dynamic
multistate** disease model — which are cross-checked for concordance against the
published aging-population denominators.

## 2. Primary exposure: the obstetric life course

The organizing variable is **cumulative vaginal-delivery exposure**, not BMI.
Vaginal delivery is the dominant modifiable generator of pelvic-floor disease
burden; BMI, age, hysterectomy, menopause and comorbidity are risk *modifiers*.

- `R/13b-obstetric_exposure.R` derives mean vaginal/cesarean deliveries per woman
  by birth cohort from CDC/NCHS cesarean-by-year and Census/NCHS
  completed-parity-by-cohort series, and forms an obstetric-exposure-weighted
  prevalent-case denominator (estimand **D4**).
- Dose–response of disease on obstetric exposure follows Gyhagen 2013 (POP/UI
  after vaginal vs cesarean delivery), Rortveit 2003 (NEJM; UI and delivery
  mode), the Women's Health Initiative (Hendrix; POP), Wu 2009/2011, Mant, and
  LaCross 2015; the coefficient table lives in
  `inst/extdata/obstetric/parity_disease_dose_response.csv`.

## 3. Life-course demand pathway (`R/25`)

For each woman-year the pathway is:

    risk (vaginal deliveries [primary], age, BMI, hysterectomy, menopause, comorbidity)
      → P(UI / POP / AI)
      → recognition → P(care-seeking | access) → P(referral) → P(treated)
      → expected service units by service line (new/return visits, urodynamics,
        cystoscopy, PTNS, Botox, sling, prolapse repair, pessary care)

Service volumes are handed to the work-RVU conversion in `R/17-workload_to_fte.R`
(`convert_workload_to_fte()`), which apportions across provider types via the
Forte 2021 delegation matrix and divides by a base-year-calibrated work-RVU-per-FTE
(Dall 2013 calibration approach; CMS RVU25A work RVUs in `R/23`; AAN 2010
indirect-time share; MGMA-range productivity guardrail). Scenarios: baseline,
changing mode of delivery, reduced barriers to care, and prevention (the only
place BMI-reduction interventions enter).

## 4. Dynamic multistate disease model (DMDM, `R/29`–`R/31`)

A longitudinal microsimulation follows each woman year by year through onset,
remission and death, so prevalence emerges from within-person dynamics rather
than a static risk equation.

- **Closed cohort** (`R/29`) and **open population** (`R/30`, with entrant
  replenishment) engines; the open engine reaches a quasi-steady population
  prevalence and can be **reweighted to Census projections** so counts match
  official demography while the model supplies the rates.
- **Fitting** (`R/31`): `dmdm_transition_data()` reshapes a longitudinal panel
  (SWAN is the intended source) into at-risk transition rows; `fit_dmdm_transitions()`
  fits per-condition onset logistics and remission rates. `build_swan_dmdm_panel()`
  (`R/47`) builds that panel from the wide SWAN frame, and
  `scripts/run_swan_dmdm_fit.R` runs the whole path — fit the UI hazards, assemble
  a full transition object (UI `fitted`, POP `derived_by_analogy`, AI
  `placeholder`; object status = the weakest), and emit the caveats that must
  travel with it (SWAN carries no delivery mode, so parity proxies vaginal parity;
  it has no POP-Q staging and does not follow AI).
- **Prolapse is different** (`R/33`): pelvic organ prolapse is *graded* (POP-Q
  stage 0–4) and it *regresses* — mild prolapse resolves spontaneously at a high
  annual rate, unlike incontinence, which is persistent once established. Two
  additions handle this. (1) A cited, literature-derived POP transition set
  (`pop_transition_parameters()`, from `inst/extdata/pop/`) supplies onset
  log-odds (cumulative vaginal deliveries primary; SWEPOP/Gyhagen 2013, WHI/
  Hendrix 2002, Mant 1997, MOAD/Blomquist 2018) plus explicit per-stage
  progression (stage k → k+1) and regression (stage k → k−1) probabilities
  (WHI natural-history cohorts: Handa 2004; Bradley 2007);
  `dmdm_transitions_with_pop_literature()` overlays these onto the POP row and
  marks the object `calibration_status = "derived_by_analogy"` (one notch above
  the placeholder, still below fitted), leaving UI/AI untouched and recording the
  mixed pedigree in `provenance`. (2) When a graded stage column is available,
  `dmdm_transition_data(stage_cols = c(pop = "pop_stage"))` carries
  `from_stage`/`to_stage`, and `fit_dmdm_transitions(stage_conditions = "pop")`
  fits the per-stage progression/regression hazards from data. The staged fields
  are inert for the two-state engine and available to a staged consumer.

## 5. Uncertainty, calibration and validation

- **Parameter uncertainty** (`R/27`): risk coefficients and care-pathway
  probabilities are drawn each Monte Carlo iteration; reported intervals combine
  parameter and cohort-sampling uncertainty (Dall HWMM; a zero-width interval
  across varying draws is refused as a defect).
- **Calibration** (`R/28`): base-year service volumes are anchored to independent
  national totals — HCUP SASD + Medicare Part B carrier (CPT 57288 slings; NIS is
  inpatient/ICD-10-PCS and undercounts outpatient slings), NAMCS/MEPS office
  visits — via multiplicative scalars (HDMM Exhibit 11: scalar = observed /
  predicted). A model with no anchor is treated as uncalibrated.
- **Back-test** (`R/28`): fit through a cutoff year, project to a held-out year,
  and score MAPE against observed totals — the credibility check the Dall-family
  models stop short of.
- **Runner** (`scripts/run_demand_calibration_backtest.R`): one command runs both
  steps — builds life-course service volumes, loads the independent anchors from
  `data/anchors/` (produced by `10_ingest_hcup_nass.R`; NAMCS/MEPS for office
  visits), fits the base-year scalars, and back-tests to the held-out year from
  `config/calibration_targets.yml`. Falls back to illustrative anchors (loudly
  flagged) when the files are absent, so it runs before the pulls land but never
  passes placeholder output off as a result.
- **External validity vs software validity.** A green R CMD check validates the
  *software*, not the *science*. Temporal back-tests are additionally weakened
  where a model component was specified after inspecting the miss it is scored
  against (e.g. the entrant-regime model and the 2021–2023 miss), which is model
  selection on the test set. `geographic_holdout_cv()`
  (`R/geographic_holdout_validation.R`) adds a genuinely out-of-sample check
  along a dimension that played *no* part in that selection: refit on a training
  set of geographies, predict the held-out ones' observed stock, and score OOS
  (MAPE, out-of-sample R², calibration slope, Spearman) via
  leave-one-geography-out, leave-one-region-out, or k-fold. Leakage-free by
  construction — each fold's fit never sees the held-out rows. Feed it observed
  provider counts by geography (ABOG/NPPES state distribution) plus a
  demand/population predictor. It does not repair the temporal contamination; it
  supplies an independent, uncontaminated external signal alongside it.
- **Preregistered rolling-origin** (`R/preregistration.R`). The procedural cure
  for the temporal contamination going forward: `preregister_spec()` freezes the
  model specification into an immutable, hashed record (`spec_hash` + freeze date,
  as diffable text under `inst/extdata/preregistration/`), and
  `rolling_origin_evaluation()` refits only parameters on data up to each origin,
  scores one-/h-step-ahead against the strictly-future target, and — when handed
  the preregistration — refuses to run unless the live spec still matches the
  frozen hash (`assert_spec_matches_prereg()`). Any post-hoc change to the model
  form flips the hash and the guard fails, so "designed after the miss" cannot
  recur silently. Leakage-free by construction (an origin's fit sees only rows at
  or before it). This does not retroactively clean the 2021–2023 comparison; it
  makes every *future* origin a clean test. A genuinely prospective validation
  against an untouched vintage remains the strongest step, and the preregistration
  is exactly what makes that vintage's evaluation clean when it lands. Runner:
  `scripts/run_preregistered_rolling_origin.R`.
- **Model-comparison scorecard** (`R/forecast_scorecard.R`). Coverage alone is a
  broken success measure: a deliberately wide interval "passes" 95% coverage while
  saying almost nothing, so a 292-provider band can beat a sharp, informative model
  merely because the truth fell somewhere inside it. `forecast_scorecard()` reports
  the full suite instead — MAPE and RMSE, signed and signed-percent bias, empirical
  coverage, mean interval width, the mean **interval score** (Gneiting & Raftery
  2007: width plus a shortfall penalty, so among covering intervals the *sharper*
  one wins), and the calibration slope. `weighted_interval_score()` is the proper
  multi-level generalization (Bracher et al. 2021). `compare_forecasts()` ranks
  competing models, expresses accuracy as **skill vs a simple benchmark**
  (`1 - model / benchmark`, so "better than green CI" becomes "better than a naive
  forecast"), and measures **rank stability across cutoffs** — a model that only
  wins on average but swings between best and worst across origins is not a reliable
  forecaster. This is how a candidate model is judged: not by a single coverage
  number, but by sharpness-penalized accuracy that is stable across back-test
  origins and beats a naive benchmark.
- **Applied to this project's own out-of-sample runs**
  (`scripts/diagnostics/interval_honesty_scorecard.R`, on committed artifacts; no
  private data). Three real evaluations of the certification stock over 2021-2023,
  and coverage ranks them exactly backwards:

  | evaluation | coverage | mean width | interval score |
  |---|---:|---:|---:|
  | rolling-origin (wide) | 100% | 1466 | 1466 |
  | sharp, attrition ON (definition mismatch) | 0% | 92 | 1732 |
  | sharp, no-attrition (definition-matched) | 67% | 97 | **137** |

  Coverage crowns the **wide** model (100%), whose intervals are so wide one lower
  bound is negative (impossible for a cumulative stock). The **interval score**
  crowns the definition-matched sharp model (137, an order of magnitude better),
  which coverage ranks only second -- same data, opposite verdict, and the proper
  score is the right one: it cannot be gamed by widening (you pay the width) or by
  narrowing without fixing the centre (you pay the `(2/alpha)` miss penalty). Two
  further lessons fall out. First, **most of the headline miss is a definition
  error, not calibration**: applying career attrition to a *cumulative* certification
  count (nobody exits it) drags the forecast low -- fixing it moves coverage
  0%->67%, interval score 1732->137, bias -87->-31. Second, the residual low bias
  (-31; +55/yr predicted vs +69/yr observed) is the genuine **entrant-regime**
  question -- decomposed in `scripts/diagnostics/entrant_regime_bias_decomposition.R`
  -- and it is a point-forecast problem no interval width can fix. Report the
  coverage result this way, not as "100% coverage validates the intervals."

## 6. Denominator hierarchy and concordance

Multiple demand estimands are carried side by side and checked for concordance
(agreement of the qualitative conclusion across independent definitions;
Fraher & Knapton 2017) rather than blended:

| Estimand | Definition | Source |
|---|---|---|
| D1 | Prevalent PFD cases (age-specific) | Nygaard 2008 / Wu 2009 (`R/13`) |
| D2 | New specialty consultations | Kirby 2013 (`R/13`) |
| D3 | SUI + POP surgical volume | Wu 2011 (`R/13`) |
| D4 | Obstetric-exposure-weighted prevalent PFD | `R/13b` (cohort vaginal parity) |
| D5 | Life-course *service* demand (care-pathway) | `R/25` (`lifecourse_demand_estimand()`) |

D1–D4 are denominators; D5 is a service-demand series downstream of the care
pathway. Because their generators differ they are not proportional rescalings, so
their concordance is informative rather than tautological.

## 7. Geography (isochrone demand, `R/32`)

The demand complement to the E2SFCA supply access in `R/14`: pelvic-floor need is
distributed across 30/60/120/180-minute travel-time (isochrone) bands, giving the
need within each band, the need effectively unreachable (beyond the largest band),
a **need-weighted** access ratio, and accessible-capacity-vs-need by geography.
This is the demand half of the "demand–supply–isochrones" question; production use
requires tract-level population, provider locations and drive-time isochrones.

`tract_need_from_population()` is the bridge from demographics to that need: it
turns a tract table of female population by demand age band — as produced by
`scripts/data_acquisition/08_download_acs_tracts.R` — into expected prevalent PFD
cases per tract using the SSOT age-band prevalence (`pfd_prevalence_by_band()`,
i.e. the D1 rates applied to *local* population). Joined to tract centroids and a
nearest-provider drive time, the result feeds `geographic_demand_summary()`;
`isochrone_demand_from_tracts()` runs both steps in one call.

## 8. Downstream contract

All demand outputs are emitted into a single versioned demand contract
(`R/export_demand_contract.R`) — tiers 3–4 (prevalence/symptomatic, DPMM), tiers
5–6 (care-seeking/procedural, life-course), and dynamic prevalence (DMDM) — with a
provenance manifest and a `calibration_status` guard, so downstream repositories
(cliff, twostep, isochrones) consume the same artifacts rather than rebuilding the
epidemiology. Provenance is carried **per tier**: passing the transition object
used for a DMDM run (e.g. `dmdm_transitions_with_pop_literature()`) to
`export_dmdm_demand_contract(transitions = )` stamps a `tier_calibration_status`
column, so `dmdm_pop` reads `derived_by_analogy` while `dmdm_ui`/`dmdm_ai` remain
placeholders and the any-PFD `tier3` takes the weakest input status. A consumer
gates on the provenance of the specific tier it reads, not a single artifact-level
flag.

## Key references

Dall TM et al., IHS Markit Health Workforce Microsimulation Model (HWMM);
Zarek et al. 2025 (physical-therapy workforce demand);
Gyhagen 2013 (SWEPOP); Rortveit et al. 2003 (NEJM); Hendrix et al. 2002 (WHI);
Mant et al. 1997 (Oxford FPA); Handa et al. 2004 and Bradley et al. 2007 (WHI POP
natural history); Blomquist et al. 2018 (MOAD, JAMA); Wu et al. 2009, 2011;
Kirby et al. 2013; Nygaard et al. 2008; Fraher & Knapton 2017; Forte GJ et al.
2021 (delegation); AAN 2010 Practice Profile; CMS Physician Fee Schedule RVU25A.

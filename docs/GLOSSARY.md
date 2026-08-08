# Glossary

The jargon in this package comes from three worlds — health-workforce
microsimulation, pelvic-floor epidemiology, and the US physician-credentialing
system — and a newcomer hits all three at once. This defines the terms the code,
the roxygen pages, and the other docs assume you already know.

It is a companion, not a starting point. Read first: the README "Orientation",
`vignette("getting-started")` (how to run it), and
[`ARCHITECTURE.md`](ARCHITECTURE.md) (where the code lives). This says *what the
words mean*; those say *what to do* and *where things are*.

Cross-references use `family` / `concept` — the two roxygen tags that make the
400+ exports navigable (`help.search("geography", package = "urpssim")`, or the
"See also" block on any `?function` page).

---

## The eight layers (the `@concept` groups)

Every export is tagged with one, and they match the `R/` filename prefixes.

| Layer | What it computes | Entry points |
|---|---|---|
| **supply** | Provider FTE over time (stochastic, agent-level) | `run_supply_microsimulation()` |
| **demand** | Required FTE from disease burden and utilization | `project_urps_demand()` |
| **geography** | Where supply and demand sit; drive-time access | `compute_access()` |
| **reporting** | The gap, base-year adequacy, scenarios, export contract | `baseline_gap()` |
| **calibration** | Anchoring free parameters to published data; their uncertainty | `fit_calibration_scalars()` |
| **validation** | The leakage-free back-test and the scoring protocol | `validation_report()` |
| **core** | Orchestration, paths, provenance, the `mufflyaccess` contract | `run_workforce_microsimulation()` |
| **data** | Bundled reference inputs (RVUs, SWAN, MEPS, population) | — |

---

## Workforce & supply

- **FTE (full-time equivalent).** The unit both sides of the model are expressed
  in. Here it is an **hours threshold**, not a headcount: `1.0 FTE = 37.2 clinical
  hours/week` (`URPS_FTE_CLINICAL_HOURS_PER_WEEK`, the Dall-2021 physiatry mean).
  A count of FTE at one threshold is **not** comparable to one at another —
  `restate_fte()` guards that trap.
- **wRVU (work Relative Value Unit).** CMS's measure of physician work per
  procedure. The demand side converts service volumes to required FTE through a
  wRVU basket (`data-cms_rvu.R`, `convert_workload_to_fte()`).
- **Headcount vs effective FTE.** Headcount is the size of the active set;
  effective FTE weights each active provider by age/sex hours. The engine reports
  both — only effective FTE moves with the hours schedule.
- **Base-year cohort.** The 2023 starting workforce: **1,306** national
  (ABOG + ABU, board-certified active), **1,027** ABOG-only, **1,303** CONUS.
  These are owned by the contract (see **SSOT**), not hard-coded.
- **Entrant.** A newly board-certified provider joining the workforce each year
  (from the NRMP → certification pipeline). Its rate is a drawn parameter, not a
  point estimate (`supply_parameter_spec()`).
- **Retirement, and the boundary rule.** Retirement is drawn from a **Weibull
  survival curve** (shape ≈ 2, scale ≈ 66–70), not a fixed age. `retirement_year`
  is stored as **first-inactive** (`last_active + 1`), and *"active in year Y"* ⟺
  `retirement_year > Y` (strict `>`). Getting that boundary wrong is an off-by-one
  in every downstream cohort.
- **Career-change (early-exit) hazard.** A *separate*, roughly age-flat attrition
  process for providers under 50 (labour-force separation), distinct from the
  age-graded retirement curve. Burnout is modelled here, not as a retirement
  shift — applying a retirement-shaped gradient to a 38-year-old is a category
  error. `concept: supply`, `family: provider lifecycle`.

---

## Demand & disease

- **PFD (pelvic floor disorder).** The umbrella condition class the demand side
  sizes. Concentrates in older women, which is why "women 65+" is the coarse
  demand denominator (and its known limitation — it understates parous women in
  their 40s–60s).
- **SUI / POP / UI.** Stress Urinary Incontinence, Pelvic Organ Prolapse, Urinary
  Incontinence — the specific conditions behind the surgical-volume (D3) and
  care-seeking (D4) estimands.
- **D1–D4 (the demand estimands).** Four *independent* demand definitions carried
  side by side so their **concordance** is the robustness check, never a blended
  number: D1 prevalent PFD cases (Nygaard), D2 new consultations (Kirby), D3
  SUI+POP surgical volume (Wu 2011), D4 BRFSS survey-weighted care-seeking. Each
  carries its own age profile so they are not proportional rescalings of one
  series (`assert_estimands_independent()`).
- **Life-course pathway.** The reproductive-exposure route to demand: vaginal
  births → PFD risk → care-seeking → referral → visits (`demand-lifecourse.R`).
- **Conservative management / diversion.** Non-surgical care (physical therapy,
  pessary) that diverts demand away from surgery — the DPMM-lite prevention
  multipliers (`demand-prevention.R`).

---

## Geographic access

- **Isochrone.** A polygon of everywhere reachable from a point within a given
  drive time. Generated (expensively, on EC2 via Valhalla) in `mufflyt/isochrones`
  for the four bands **30 / 60 / 120 / 180 minutes**.
- **Membership / catchment.** The compact table the access layer consumes —
  `(demand_id, provider_id, band)` — saying which provider is reachable from which
  demand tract within which band. Built by
  `scripts/data_acquisition/12_build_provider_isochrone_membership.R`.
- **2SFCA / E2SFCA / M2SFCA.** Two-Step Floating Catchment Area, the standard
  spatial-access method. **E2SFCA** (enhanced) distance-weights the bands;
  **M2SFCA** (modified, `step2_power = 2`) additionally squares the cumulative
  band weights to penalise supply that exists but is poorly located. The M2SFCA
  mean can never exceed the E2SFCA mean (`compare_access_methods()`).
- **The ordering trap.** Running the access layer *before* real isochrones exist
  makes it fall back to state-level geometry and emit a plausible access ratio
  that means nothing. `geographic_access_status()` and `validation_report()`'s
  geographic gate refuse it: no polygons, no membership, no number.
- **RUCA / rurality / HPSA / CONUS.** RUCA = Rural–Urban Commuting Area codes
  (RUCA ≥ 4 is rural, `rurality_from_ruca()`); HPSA = Health Professional Shortage
  Area (designated for primary care/dental/mental health, **not** subspecialties —
  so there is no URPS HPSA input); CONUS = the contiguous 48 states + DC, the
  study scope.

---

## Calibration, validation & uncertainty

- **Calibration tier.** Every reported quantity declares how well-anchored it is,
  and uncalibrated numbers refuse to be reported as results. Four tiers, strongest
  first: **`calibrated`** (measured on urogynaecologists), **`solved`** (derived
  from an equation, not assumed), **`derived_by_analogy`** (a value borrowed from a
  related specialty), **`uncalibrated_illustrative`** (a placeholder). See the
  getting-started vignette, §"Every input declares a calibration tier".
- **Calibration scalar.** A multiplicative correction that anchors a model
  prediction to an independent national total (`scalar = observed / predicted`,
  HDMM Exhibit 11). `fit_calibration_scalars()`.
- **Base-year gap / adequacy.** The shortfall in the starting year, estimated (not
  assumed to be zero). Published anchors: Zarek 2025 **0.948** adequacy / 5.2%
  gap; Dall 2021 **940 FTE** / 10.6%; Dall 2013 **1,814 FTE** / 11.0%. Rebasing
  supply and demand to 1.0 in the base year would *assume* adequacy — the model
  refuses to.
- **Back-test.** The out-of-sample check: fit through a cutoff, project forward,
  score against what actually happened (2020 → 2023). **Leakage-free** = the model
  never sees the target period.
- **Rolling-origin.** Repeating the back-test over successive cutoffs to test
  interval calibration over many origins, not one.
- **Interval score / WIS.** Proper scoring rules for a *distributional* forecast
  that reward a narrow interval only when it also covers the truth: the interval
  score (Gneiting & Raftery 2007) and the Weighted Interval Score (Bracher et al.
  2021). Coverage alone is not enough — a 100%-wide interval "covers" everything.
- **Prediction interval vs Monte-Carlo noise.** The engine's spread must describe
  *forecast* uncertainty (redrawn coefficients per iteration), not just
  sampling noise within one fixed parameter set — the defect the 2020→2023
  back-test exposed.
- **Two-method agreement.** Evidence is agreement across *genuinely independent*
  methods (Fraher & Knapton's nephrology comparison), not two rescalings of one
  population series (which is arithmetic).

---

## Reproducibility & provenance

- **SSOT (single source of truth).** Quantities owned by the private
  `mufflyaccess` data package that this package must not redefine: base-year
  supply, the scenario registry, PFD prevalence 65+, drive-time bands, rurality,
  the projection schema. Reached only through `core-ssot.R`;
  `ssot_coverage_report()` lists what is owned vs local. When `mufflyaccess` is
  absent, the SSOT-dependent tests skip themselves.
- **Reproducibility mode.** `strict` (manuscript: fail closed on any provenance
  drift) vs `relaxed` (development: warn). `resolve_reproducibility_mode()`.
- **Cohort provenance.** The refusal to call an aggregate-count-derived cohort a
  "roster": `cohort_provenance()` records whether ages were observed or assumed so
  a synthetic cohort cannot be mistaken for a real one.
- **Fail-closed provenance.** Derived artifacts carry a provenance sidecar and
  reject on load if input, code, or content hashes drift.

---

## Source models (the acronyms)

- **HWSM / HWMM / HDMM.** The IHS Markit / Dall **Health Workforce (Supply)
  Microsimulation Model** and its **Demand** counterpart (HWMM v5.19.20) — the
  published methodology this package is built to, across physiatry, neurology, and
  physical therapy applications (Dall 2013/2021, Zarek 2025).
- **Fraher agent / FutureDocs.** Fraher & Knapton's UNC individual-level physician
  microsimulation, the source of the agent-based supply structure and the
  categorical (full/part/none) participation FTE (`supply-fraher_agent_supply.R`).
- **DMDM.** Dynamic Multistate Disease Model — the multistate PFD-transition model
  on the demand side (`demand-dynamic_multistate.R`).
- **DPMM.** The disease/prevention microsimulation the conservative-management
  diversion multipliers come from ("DPMM-lite", `demand-prevention.R`).

---

## Data sources & credentials

| Acronym | What it is | Used for |
|---|---|---|
| **ABOG / ABU** | American Boards of Obstetrics & Gynecology / Urology | subspecialty certification → the base-year cohort |
| **ACOG / AUGS** | professional societies (Ob-Gyn / urogynecology) | scope, prior workforce studies |
| **NRMP** | National Resident Matching Program | entrant pipeline (match → certification) |
| **ACGME** | residency/fellowship accreditor | URPS fellow counts (`supply-acgme_fellows.R`) |
| **NPPES** | NPI registry | provider locations, taxonomy |
| **NAMCS / NHAMCS** | National Ambulatory/Hospital Medical Care Surveys | office-visit demand anchors |
| **BRFSS** | Behavioral Risk Factor Surveillance System | D4 survey-weighted care-seeking |
| **MEPS** | Medical Expenditure Panel Survey | care-seeking, utilization |
| **SWAN** | Study of Women's Health Across the Nation | incontinence-transition hazards |
| **HCUP / NASS** | Healthcare Cost & Utilization Project / Nationwide Ambulatory Surgery Sample | base-year procedure anchors |
| **ACS** | American Community Survey | tract demand denominators |
| **Census NPP** | National Population Projections | the female-population demand driver |
| **HRSA** | Health Resources & Services Administration | clinical-hours-by-age/sex schedule, HPSA designations |

---

*Missing a term? If the code uses a word this doesn't define, that's a gap —
add it here rather than leaving the next newcomer to reverse-engineer it.*

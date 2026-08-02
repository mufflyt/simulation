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
  fits per-condition onset logistics and remission rates.

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

## 8. Downstream contract

All demand outputs are emitted into a single versioned demand contract
(`R/export_demand_contract.R`) — tiers 3–4 (prevalence/symptomatic, DPMM), tiers
5–6 (care-seeking/procedural, life-course), and dynamic prevalence (DMDM) — with a
provenance manifest and a `calibration_status` guard, so downstream repositories
(cliff, twostep, isochrones) consume the same artifacts rather than rebuilding the
epidemiology.

## Key references

Dall TM et al., IHS Markit Health Workforce Microsimulation Model (HWMM);
Zarek et al. 2025 (physical-therapy workforce demand);
Gyhagen 2013; Rortveit et al. 2003 (NEJM); Hendrix et al. (WHI);
Wu et al. 2009, 2011; Kirby et al. 2013; Nygaard et al. 2008;
Fraher & Knapton 2017; Forte GJ et al. 2021 (delegation); AAN 2010 Practice
Profile; CMS Physician Fee Schedule RVU25A.

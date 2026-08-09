# URPS Clinical Productivity and Case-Mix Study Specification

**Working title.** Clinical Productivity and Case Mix of the U.S. Urogynecology
Workforce: A Multicenter Observational Study.

---

## Why this study, and why now

Among the empirical uncertainties evaluated to date, **productivity and case mix
are the only ones spanning a range wide enough to reverse the projected 2050
workforce balance.** Direct measurement is therefore necessary to translate
projected utilization into workforce FTE requirements without relying on
productivity assumptions borrowed from other specialties.

The measured position, for context:

| uncertainty | plausible range | effect on the 2050 gap | reverses sign? |
|---|---|---|---|
| **Productivity / case mix** | 3,500–12,000 wRVU/FTE | required FTE 872 → 2,989 | **yes** |
| Baseline adequacy (donor specialty) | 0.890–0.948 | +433 to +537 | no (breakeven 0.708) |
| Entrant pipeline (conversion-adjusted) | 60.0–63.7/yr | +260 to +348 | no (breakeven 49.1) |
| Hours-curve age gradient | 0–1.5× published | ±~180 FTE supply | no |
| Retirement timing | ±10 years | +452 to +537 | no |
| Monte Carlo error (n=30) | — | ±55 FTE | no |

Everything other than productivity lives inside a band of roughly ±250 FTE.
Productivity spans 2,100.

### Why it stayed hidden

`calibrate_wrvu_per_fte()` solves productivity so that base-year required FTE
equals supply ÷ adequacy. Under that construction productivity *cannot* be
wrong — it is defined to fit — which is why `delegation_matrix` and
`demand_calibration` are both recorded as `cancels_out = TRUE`. A 2.1× correction
to the NAMCS scalar moved 2050 required FTE by 0.25%.

That cancellation is real but **conditional on the anchored regime**. Ask what
FTE the population would actually use, the anchor goes, and productivity becomes
the dominant term. This study exists to measure the thing the anchor has been
concealing.

---

## Two constraints that govern the design

These are not preferences. A study that violates either does not answer the
question.

### 1. Do not invent a composite productivity unit at the outset

Collect the raw case-mix components first. A composite can always be constructed
later once defensible weights exist; **the original clinical composition cannot
be recovered after collapsing everything prematurely.** Assigning weights merely
to force office visits, procedures, and operations onto a common scale
manufactures the very assumption the study is meant to replace.

If no defensible common weight exists, retain the vector of case-mix-specific
productivity measures and propagate them separately through the workforce model.

### 2. Extract from site all-payer systems, never from public Medicare files

**Requirement, with its reason attached** — because a well-meaning site will
otherwise "simplify" by pulling a public file and silently destroy the numerator.

Open Payments and the Medicare Physician & Other Practitioners PUF are **FFS
Medicare only**. They exclude private insurance *and* Medicare Advantage — the
latter being roughly half of Medicare enrollment and wholly invisible in FFS
files. A wRVU numerator built from them captures an unknown slice of each
physician's practice and then needs a per-physician all-payer coverage fraction
to scale: a quantity that varies enormously by practice, is not observable in the
source, and would reintroduce on the numerator exactly the uncertainty this study
exists to remove. `medicare_work_rvu_by_provider()` already documents its
`coverage_col` as "a sensitivity input, not estimated here."

Extraction is therefore from **site EHR, billing, or practice-management systems**,
which are all-payer, using a common data dictionary and standardized extraction
specification at every site. Not physician recall.

---

## Scientific purpose

Estimate how much clinical care is produced by one clinical full-time equivalent
(FTE) urogynecologist, and how that productivity varies by practice setting,
physician characteristics, and case mix.

This study does **not** estimate whether current national capacity is adequate.
See "Relationship to the capacity survey" below.

## Primary objective

Annual clinical output per 1.0 clinical FTE among practicing U.S.
urogynecologists. Primary unit of analysis: the **physician-year**.

## Secondary objectives

1. Distribution of clinical productivity across physicians and practices.
2. Clinical case mix contributing to total output.
3. Whether productivity differs by practice setting, academic status, geography,
   physician age, and clinical FTE.
4. Productivity separately for major categories of urogynecologic care.
5. An empirical productivity distribution suitable for the workforce
   microsimulation.
6. How replacing borrowed productivity assumptions with observed urogynecology
   productivity changes projected workforce balance through 2050.

## Design

Multicenter retrospective observational study using one complete year of
physician-level clinical activity, extracted per the constraint above.

## Target population

Actively practicing U.S. physicians whose practice includes urogynecology,
FPMRS, or URPS. Physicians contribute data only for periods of active practice.

## Sampling strategy

The objective is **not** a convenience sample of high-volume academic
urogynecologists. Recruitment deliberately spans: academic medical centers;
community or health-system employed practices; private practices; safety-net
systems; urban and nonurban markets; practices with and without fellows or
residents; practices with and without APP support.

Maximize heterogeneity in practice structure rather than physician count. Each
practice reports **all** eligible urogynecologists where feasible, rather than
selecting high performers.

## Observation period

One complete recent calendar year preferred. Sites unable to provide a full year
may contribute ≥6 continuous months with the period and denominator recorded
precisely. Partial-year observations are annualized only after accounting for
active clinical time. Vacation, parental, medical, and administrative leave are
**not** productive clinical time.

## Clinical FTE denominator

The central denominator, and measured rather than inferred from total employment
FTE wherever possible. For each physician collect: total employment FTE;
formally assigned clinical FTE; clinical half-days or sessions per typical week;
OR sessions per month; ambulatory clinic sessions per week; procedure sessions
per month; weeks clinically active during the year; formally protected research,
education, administrative, or leadership effort.

Primary denominator: reported institutional clinical FTE. A secondary empiric
measure is computed from scheduled clinical sessions to test whether reported
clinical FTE is comparable across institutions.

## Primary productivity outcome

Annual unique patient-care encounters per clinical FTE — interpreted together
with the component outcomes below, never as a standalone measure of capacity.

## Required case-mix outcomes

Counts per physician per observation period: new outpatient urogynecology
evaluations; return outpatient visits; pessary encounters; urodynamic studies;
office cystoscopy; intradetrusor botulinum toxin injections; PTNS encounters;
sacral neuromodulation evaluations and procedures; other office procedures;
operating-room cases; major prolapse procedures; urinary incontinence
procedures; reconstructive procedures; other urogynecologic operations.

Surgical procedures are additionally classified into mutually interpretable
groups rather than represented only by total CPT volume.

## Additional productivity measures

Total encounters per clinical FTE; new patients per clinical FTE; surgical cases
per clinical FTE; office procedures per clinical FTE; operative days per clinical
FTE; **work RVUs per clinical FTE**; clinical hours per clinical FTE; patients
treated per scheduled clinical session.

wRVU is retained as a secondary measure rather than the primary definition of
productivity, because reimbursement weighting is not identical to clinical
capacity. See the integration decision below — this choice has a model-side
consequence that must be settled before fielding.

## Practice-level and physician-level variables

**Practice.** Type; academic affiliation; hospital/health-system employment;
number of urogynecologists; number and clinical role of APPs; resident
involvement; fellow involvement; examination rooms available; dedicated
nursing/MA support; procedure-room access; OR-time access; **typical new-patient
appointment wait**; payer mix where available.

**Physician.** Age or age category; years since fellowship; sex if scientifically
justified and available; board-certification status; clinical FTE; practice
setting; years at current practice. Collect nothing merely because it is
available.

## Core estimands

1. **Overall clinical productivity.** Mean annual output per 1.0 clinical FTE.
   Report mean with SD *and* median with IQR — productivity is expected to be
   skewed.
2. **Case-mix-specific productivity.** Annual count of each major service
   category per clinical FTE.
3. **Productivity distribution.** The empirical between-physician distribution
   after accounting for observation time and clinical FTE. This distribution,
   not a national mean, is the preferred simulation input.
4. **Practice-setting differences.** Adjusted differences by academic status, APP
   support, trainee involvement, procedural access. Explanatory, **not** to be
   read causally.

## Primary analysis

Per physician: `productivity = observed annual clinical output / observed
clinical FTE`. For partial observation, exposure time enters explicitly rather
than assuming a typical year.

Describe with mean and SD; median and IQR; 5th and 95th percentiles; empirical
density plots. The national estimate **preserves between-physician variability**
rather than collapsing to pooled-volume ÷ pooled-FTE. Where physicians cluster
within practices, uncertainty accounts for practice-level clustering.

## Sensitivity analyses

Prespecified: institutional clinical FTE vs scheduled sessions; excluding vs
including encounters predominantly by trainees or APPs; academic vs nonacademic;
with vs without fellowship trainees; full-year observations only; productivity as
encounters vs wRVU vs major service categories; trimming vs retaining extreme
values; equal physician weighting vs weighting to the estimated national
practice-setting distribution.

## Missing data

Missing productivity outcomes are **not** imputed from other volume measures.
Missing clinical-FTE is especially consequential and triggers site clarification
or exclusion from FTE-normalized analyses. Amount and pattern of missingness
reported for every core variable.

## Data-quality checks

Physician identifiers unique within the analytic data; observation periods do not
overlap incorrectly; clinical FTE > 0 and within plausible bounds; clinical
volume covers the same period as FTE; procedure categories mutually
interpretable; APP/trainee encounters handled consistently; annualization
reproducible; site totals reconcile with source-system totals.

---

## Integration into the workforce model — a decision to settle before fielding

The observed physician-level distribution replaces the borrowed productivity
distribution in the demand-to-FTE conversion, and the model is rerun by
**sampling** productivity from the empirical distribution rather than assigning a
fixed value.

**The gap that must be closed first.** The model is wRVU-denominated end to end:
`convert_workload_to_fte(volumes, wrvu_per_fte = ...)` takes a scalar, and
`calibrate_wrvu_per_fte()` returns one. The study's *primary* outcome is
encounters per FTE. As written, the primary estimand cannot be consumed by the
model while the secondary one can. Two honest options:

- **Co-primary.** Make encounters/FTE and wRVU/FTE both primary. Cheap; wRVU/FTE
  drops straight into the existing conversion.
- **Propagate the case-mix vector.** Better science, and what constraint 1 above
  is preserving. Requires model work: the conversion must accept a case-mix
  vector rather than a scalar, and productivity must enter the PSA inputs, which
  currently vary entrants, retirement source, population series, and base
  adequacy — but not productivity.

Whichever is chosen must be stated in the protocol, because option 2 carries a
dependency that is otherwise unnamed.

**Report at minimum:** base-year FTE demand; projected 2050 FTE demand; projected
2050 balance; probability the balance is below zero; the productivity level at
which the balance reverses; comparison with the borrowed-productivity model.

Principal validation question: **does the empirically supported productivity
distribution remain wide enough to reverse the direction of the 2050 balance?**

---

## Relationship to the capacity survey

Different quantities, and neither substitutes for the other:

- **Productivity study** — "How much clinical care does one clinical
  urogynecology FTE produce?" Fixes the **demand-to-FTE conversion**.
- **Capacity survey** — "How much additional care could the current system
  provide, and how much of any constraint is physician capacity?" Fixes the
  **base-year adequacy anchor**.

**A caveat that makes them less independent than they look.** Measured
productivity is *realized* output, bounded by demand at that practice. A
urogynecologist with unfilled clinic slots shows low productivity, not low
capacity. Using realized output as a capacity denominator embeds the assumption
that physicians are demand-saturated — which is precisely what the capacity
survey exists to test.

Mitigation: elevate **typical new-patient appointment wait** from a practice
covariate to a prespecified effect modifier, and add one per-physician capacity
item (*could you have absorbed additional volume within your existing
sessions?*). Without this, a low productivity estimate is ambiguous between "this
is what an FTE produces" and "this practice had spare capacity."

---

## Feasibility: range collapse and sign resolution are different deliverables

A first-stage pilot of ~10–15 practices and 30–50 physicians establishes whether
clinical FTE can be measured consistently, whether the service categories extract
reliably, the approximate between-physician variation, which practice
characteristics explain meaningful variation, and whether a larger national study
is needed. The definitive sample size then uses the **observed** between-physician
and between-practice variance, not an assumed effect size.

Set expectations up front, so a pilot result is not misread as failure:

| goal | requirement | achieves |
|---|---|---|
| **Range collapse** | n ≈ 50 (at CV 30%) | 3,500–12,000 → ~7,100–8,400. Transformative |
| **Baseline sign resolution** | n ≈ 231 (CV 25%), 333 (30%), 591 (40%) | separates 7,500 from p\* |

The baseline decision boundary is **p\* = 8,009 wRVU per clinical FTE per year** —
where required FTE equals current supply (1,306) on a base-year workload of 10.46 M
wRVU. The benchmark median of 7,500 sits only **6.8% below** it and implies
required = 1,395 FTE, a baseline shortage. Separating those two needs precision an
n≈50 pilot will not deliver.

Both CV figures are **assumed, not measured** — nothing in the repository
estimates between-physician variation in wRVU/FTE, which is why pinning the CV is
the pilot's first job. The arithmetic also holds case mix fixed; if the mix is
being re-estimated simultaneously, effective precision is worse than shown.

## Success criterion

The study succeeds if it replaces borrowed productivity assumptions with an
empirically measured urogynecology productivity distribution precise enough to
determine **whether productivity uncertainty still spans both sides of the
projected 2050 balance**.

It does not need to prove one productivity value correct. Its purpose is to
reduce and characterize the uncertainty enough that the projection communicates
what the evidence can actually support.

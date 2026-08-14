# CHIA technical appendix

Massachusetts Center for Health Information and Analysis (CHIA) Case Mix data as
an input to `urpssim`. This appendix records what the source can and cannot
observe, what had to be repaired to use it, and which claims the resulting
numbers support.

Source database: `chia_cadr.duckdb` (4.5 GB, external drive, not in this repo).
Build and validation code: `scripts/chia/`. Full data dictionary:
`chia_cadr.README.md` alongside the database.

---

## 1. What CHIA observes

CHIA Case Mix comprises **three** databases: Hospital Inpatient Discharge
(HIDD), Outpatient Emergency Department, and Outpatient Observation (OOD).

**There is no ambulatory-surgery database.** 957 CMR 8.00 binds *acute care
hospitals*, so freestanding ambulatory surgery centres never submit at all. A
woman who has surgery at a hospital and goes home the same day appears in
**none** of the three files — she surfaces only if converted to observation or
admitted. The guide's own source-of-visit codes confirm the boundary: `T` =
"Transfer from Another Institution's Ambulatory Surgery (SDS)", `Y` = "Within
Hospital Ambulatory Surgery Transfer". Same-day surgery exists in CHIA only as
*where a patient came from*.

We hold 2 of the 3 databases — 16 `Discharge` files and 15 `Observation` — and
**no APCD**. Nothing APCD-shaped exists in `meta._load_manifest`.

**Consequence.** Every CHIA figure in `urpssim` is conditional on admission.
Most urogynaecologic surgery is now ambulatory and is structurally invisible
here. CHIA is a *regional external validation* source. It is never a supply
input and never a total-volume denominator.

### Why the inpatient series cannot back-test demand

Inpatient hysterectomy in the study cohort falls **10,984 (FY2004) → 2,269
(FY2018)**, a 79% decline, while **median length of stay never moves off 2.0
days**. The cases did not get shorter; they crossed out of the inpatient
definition. An LOS ≥ 2 floor does not rescue the series — it falls *more*
steeply (9,641 → 1,459, 85%).

A rolling-origin back-test scored against this series would grade the demand
model against setting migration, not disease. Do not build it without an
ambulatory denominator.

**UB-04 revenue-center evidence settles which it is.** Two hypotheses fit a
falling count with flat LOS: the cases left the inpatient setting, or they were
reclassified inside HIDD. Revenue codes name the hospital cost centres that
participated, so they separate the two. For POP-indication hysterectomy
(`R/data-chia_revenue_setting.R`, FY2015-2018 — the years `hdd_service` covers):

| FY | Encounters | 036x OR | 037x anaesthesia | 071x recovery | Inpatient bed | 049x ambulatory | LOS median |
|---|---|---|---|---|---|---|---|
| 2015 | 617 | 99.5% | 95.6% | 99.0% | 99.7% | 0% | 1 |
| 2016 | 523 | 99.6% | 95.8% | 98.7% | 100.0% | 0% | 1 |
| 2017 | 448 | 99.8% | 95.1% | 98.9% | 100.0% | 0% | 1 |
| 2018 | **358** | 98.3% | 95.8% | 96.4% | 98.9% | 0% | 1 |

**Volume falls 42% while every marker of the inpatient surgical pathway holds
between 95% and 100%.** Operating room, anaesthesia, recovery room and
inpatient bed are all stable; ambulatory-surgery cost-centre revenue is **zero
throughout**. The encounters that remain are the same operation on the same
pathway — the missing volume genuinely left the dataset rather than being
reclassified within it.

This is the strongest available confirmation that the decline is setting
migration, and it closes the question the LOS evidence could only raise. Note
the design choice in that module: a 049x or 0762 code on an HIDD record is
evidence about which cost centre was involved, **not** proof the encounter was
ambulatory or observation. The classification is conservative and the raw flags
are preserved.

Coverage limit: `hdd_service` exists FY2015-FY2018 only (33.9M service lines),
so this test cannot reach the FY2004-2014 portion of the decline.

---

## 2. Repairs required before the data could be used

Each was found by a specific failure, and each is now covered by a gate in
`scripts/chia/test_chia.py` (71 checks, 63 gates).

### 2.1 Schema drift — most columns do not exist in most years

| Family | Distinct columns | Present in **all 15** years |
|---|---|---|
| `hdd_discharge` | 296 | **30** |
| `ood_observation` | 140 | **2** |

A query written for one era returns `NULL` for the other without erroring. This
is the dataset's dominant failure mode. Confirmed renames include
`Sex`→`SexLDS`, `Age`→`AgeLDS`, `Race`→`Race1`, `SiteOrgID`→`IdOrgSite`,
`CPT1-5`→`CPTCode1-5`, `ProcedureCode1`→`PrincipalProcedureCode`, and
`TotalChargeSpecial`→`TotalChargе**s**Special` — a one-letter difference whose
plural form returns zero non-null rows before FY2015.

Registered in `meta.column_rename`; resolved by `v_hdd_discharge_canonical` and
`v_ood_observation_canonical`; detected by `scripts/chia/column_audit.py`.

### 2.2 "Has a procedure code" is not "had an operation"

ICD procedure codes cover vaccination, transfusion, imaging and injection.
Before classification, the highest-volume "surgeons" of FY2015 were:

| Specialty | Cases |
|---|---|
| Internal Medicine | 2,882 |
| Pediatrics | 2,222 |
| Internal Medicine | 1,352 |

**26–29% of "surgical" discharges are not operations.** After classification the
top of the table is orthopaedic surgeons at 520–697 cases/year. The rule is
structural and needs no licensed grouper: ICD-9-CM `00–86` operative / `87–99`
not; ICD-10-PCS character 1 = section, `0` Medical & Surgical, `1` Obstetrics,
all else non-operative. Gate W1.

### 2.3 Newborn stays carry the mother's obstetrician

`AdmissionType = '4'` records name the delivering obstetrician as operating
physician. Counting them made **circumcision the single most common FY2018
"URPS" procedure** (146 cases, all male, age 0) and inflated URPS operative
volume by **41%** in FY2018. Held separately as `newborn_cases`. Gates W5–W6.

### 2.4 The `-` sentinel

Before FY2015, "no procedure" is written `-` (322,309 FY2010 discharges); from
FY2015 it is `NULL`. Treating `-` as a value marks every discharge surgical and
drags apparent NPI reach from 92% down to ~52%. Gate D11.

### 2.5 Physician reporting cliffs

- **OOD, FY2016**: a growing set of hospitals (14, then 17, then 20) stopped
  populating physician identifiers while volume held. Verified against the raw
  Access files — the blanks are CHIA's, not ours; `PhysicianNumber` is still
  declared `Text (50)` and shipped empty. Site-year filtering does **not** repair
  it (survivors are a non-random 15–22 of ~70 sites), so the OOD physician window
  is closed at FY2015. Gate D8.
- **HIDD, FY2018**: four UMass Memorial sites reported zero physicians on
  ~17,855 operative discharges. Recorded as a per-year completeness factor in
  `v_surgeon_year_completeness` (93.3% for FY2018, 98.6–99.9% elsewhere), **not**
  as a dropped year.

### 2.6 Encoding

39 rows across 12 tables were silently lost to `ignore_errors=true` on
Windows-1252 content. Repaired by transcoding; this also corrected an earlier
false claim that the BORIM roster had two disagreeing vintages — it was the
loader dropping 20 rows.

---

## 3. Study cohort: female, 18 and over

`chia_casemix.v_cohort_female_adult`, ~385k–423k discharges/year. Gates C1–C4.

Two traps:

1. **`age >= 18` deletes the oldest women.** FY2015+ top-codes age 90+ to `999`
   (NULL after canonicalisation), so a plain numeric filter drops **36,610–38,335
   records a year** — the peak-prevalence group for prolapse and incontinence.
   The cohort retains them via `age_top_coded`.
2. **The eras measure age differently.** Pre-FY2015 ages are explicit to
   **110–115**; FY2015+ stops at 89. Age-stratified series must use
   **`age_capped`** (90 = "90 or older"). The 90+ share runs 5.9% → 6.2% across
   the boundary, i.e. continuous.

---

## 4. Attribution to NPI

Chain: CHIA physician ID → BORIM licence → NPI, via
`chia_provider.borim_stdrel_npi_straight_from_cd`. The crosswalk is 1:1 (36,448
licences, none with two NPIs); 11 NPIs hold two licences and are collapsed. All
NPIs are well-formed 10-digit **individual** identifiers — no organisational NPIs.

**80–92% of operative discharges reach an NPI in every year, FY2004–2018**
(gate D10). `chia_casemix.surgeon_year_volume` is the resulting surgeon-year
table: 169,628 surgeon-years, 24,083 surgeons.

**No age is available.** `birthdate` is 100% empty across every provider table.
And an exit from CHIA is **not** a retirement — a surgeon shifting to ambulatory
practice is indistinguishable from one retiring, precisely the distinction the
ambulatory blind spot forecloses. `supply-retirement_hazard.R` must therefore
stay `derived_by_analogy`; feeding CHIA exits into it would encode outpatient
migration as retirement.

---

## 5. Surgical travel — the calibration instrument for the E2SFCA decay weights

**This is what CHIA is for.** `mufflyt/twostep` weights supply by generic Luo/Qi
distance decay (`E2SFCA_DEFAULT_WEIGHTS`: 30 = 1.00, 60 = 0.68, 120 = 0.22,
180 = 0.09) — a general-accessibility default carried into a subspecialty
surgical model with no measurement behind it. CHIA replaces the assumption with
an observation for the urogynaecologic case.

Code: `R/geography-chia_inpatient_flows.R`. Builders:
`scripts/chia/build_chia_surgical_travel_kernel.R` (all surgery),
`scripts/chia/build_chia_urogyn_travel_kernel.R` (urogynaecology).

### 5.1 The supply set must be urogyn-capable, not all hospitals

Only **18–30 of ~76** Massachusetts acute hospitals host any URPS operation in a
given year; only **4–16** reach ten cases. Measuring against all hospitals is the
single largest error available here:

| Distance to nearest… | p50 | p75 | p90 | p95 |
|---|---|---|---|---|
| **any** acute hospital | 2.9 | 5.1 | **8.2** | 12.1 |
| **urogyn-capable** hospital | 5.3 | 10.8 | **22.4** | 34.1 |
| where women actually went | 8.4 | 17.8 | 31.8 | 50.5 |

Using all hospitals overstates urogynaecologic accessibility by **~3× in the
tail**. An E2SFCA surface built that way reports adequate access across every
region that has a hospital but no urogynaecologist — the exact failure a
workforce access model exists to detect.

The capability threshold is not delicate: median nearest-capable distance is
4.4 / 5.3 / 6.1 miles at thresholds of 1 / 10 / 25 annual cases.

### 5.2 The calibrated kernel

A decay weight answers a conditional question: *given* supply at distance *n*,
how much of it is used at distance *d*? Marginal band shares cannot answer it —
they are dominated by where hospitals happen to be. Restricting to women whose
nearest urogyn-capable site is **within 5 miles** (n = 4,342) isolates choice
from availability:

| Actual distance travelled | Share | **Calibrated weight** | Luo/Qi at comparable band |
|---|---|---|---|
| 0–5 mi | 67.8% | **1.000** | 1.00 |
| 5–10 mi | 14.9% | **0.219** | ~0.68 |
| 10–25 mi | 16.0% | **0.236** | ~0.68–0.22 |
| 25–50 mi | 1.3% | **0.019** | ~0.22 |
| >50 mi | 0.05% | **0.0007** | ~0.09 |

Two results, both material:

**(a) This kernel is conditional on urban availability, and must not be read as
the population decay.** Restricting to women whose nearest capable site is
within 5 miles isolates choice from availability — but it selects women who
*have* close options, whose revealed decay is necessarily tight. Fitting a
Gaussian to this stratum alone gives σ ≈ 5 miles (~10 minutes) and appears to
show the 60-minute default overstating reach five- to sevenfold. **That is a
selection artefact.** See §5.2a for the stratified fit, which is the
trustworthy aggregate.

**(b) The decay is not monotonic.** The 10–25 mile weight (0.236) *exceeds* the
5–10 mile weight (0.219). Women bypass nearer capable hospitals to reach farther
ones — 20.2% travelled more than 10 miles past their nearest capable site. A
strictly decreasing distance-decay function cannot represent this; it is
regionalisation toward higher-volume centres, and it means the *functional form*
in the E2SFCA layer, not merely its parameters, is mis-specified for
subspecialty surgery.

Kernel bands are in **miles**, deliberately. Converting to the minute-denominated
Luo/Qi bands requires a speed assumption that swings the answer by 14 points
(§5.3), so the calibrated weights are published in the unit that was measured.
At ~40 mph the 30-minute boundary falls inside the 10–25 mile band.

### 5.2a Calibrating σ — the default is sound, but σ is not a constant

`E2SFCA_DEFAULT_WEIGHTS` is not a raw Luo/Qi table: it is a **Gaussian with
σ = 60 minutes** evaluated at the band edges and normalised to the 30-minute
band. σ is therefore the single free parameter, and it has been an assumption.

Fitting σ within strata defined by **how far the nearest capable hospital
actually is** (n = 9,081):

| Nearest capable hospital | Cases | σ (miles) |
|---|---|---|
| ≤ 5 mi (urban) | 4,342 | **5.0** |
| 5–10 mi | 2,259 | 22.1 |
| 10–25 mi | 1,662 | 27.4 |
| > 25 mi (rural) | 818 | **108.7** |
| **Case-weighted global** | **9,081** | **22.7** |

**22.7 miles is 44 minutes at 40 mph and 59 minutes at 30 mph — the 60-minute
default sits inside that range.** At 30 mph the calibrated weights reproduce the
default almost exactly (1.000 / 0.679 / 0.144 / 0.011 against 1.000 / 0.687 /
0.153 / 0.013). **This measurement confirms the existing parameter for aggregate
use rather than overturning it.**

The substantive result is the **twenty-fold spread**. Women adapt travel to what
exists: those with a hospital nearby rarely pass it, those without travel as far
as needed. A fixed-σ Gaussian cannot express that, so any single global value
understates access in dense areas and overstates burden in sparse ones. The
stratified vector (`URPS_INPATIENT_SIGMA_BY_AVAILABILITY` in
`mufflyt/twostep`) is the more faithful object.

Wired into twostep as `urps_inpatient_band_weights()`, which routes through
`gaussian_band_weights()` and is monotone by construction — the raw empirical
kernel is not, and is correctly rejected by `e2sfca_band_weights()`.

Builder: `scripts/chia/fit_urps_sigma.R`; output
`data-raw/chia/urps_sigma_by_nearest_distance.csv`.

---

### 5.3 Distance is measured; drive time is not

**There is no routing engine in this pipeline.** Where minutes appear they are
`miles × 1.3 circuity ÷ 40 mph`, and both constants are choices that dominate:

| Assumed speed | ≤30 min share (all surgery) |
|---|---|
| 30 mph | 0.646 |
| 40 mph | 0.731 |
| 50 mph | 0.790 |

A 14-point swing from the speed constant alone — wider than most effects this
kernel would be used to detect. Minute-denominated figures exist only for
comparability with the Luo/Qi band structure and are never reported as
observations. `chia_travel_kernel("drivetime")` emits a `warning()`. Real drive
times require the HERE isochrone pipeline in `mufflyt/isochrones`, and
substituting them is the single highest-value improvement to this layer.

### 5.4 Stratification by patient demographics

n = 9,081 URPS operations, FY2007–2018. `pct_beyond_10mi` is the share
travelling more than ten miles past their nearest urogyn-capable site — the
access-relevant quantity, because it isolates travel not explained by geography.

**By payer:**

| Payer | Cases | p50 mi | p90 mi | Nearest capable, p50 | Beyond 10 mi |
|---|---|---|---|---|---|
| Commercial | 306 | 12.4 | 52.0 | 6.8 | **32.0%** |
| Blue Cross | 564 | 10.2 | 44.1 | 6.5 | 24.1% |
| Medicare | 2,470 | 9.4 | 46.8 | 6.0 | 25.1% |
| Blue Cross MC | 1,478 | 9.6 | 30.5 | 6.1 | 20.4% |
| Commercial MC | 300 | 9.0 | 31.6 | 6.4 | 19.0% |
| HMO | 1,591 | 8.1 | 26.7 | 5.4 | 16.3% |
| Medicare MC | 606 | 7.7 | 25.7 | 5.0 | 16.7% |
| Medicaid MC | 523 | 4.5 | 22.8 | 3.3 | 12.2% |
| **Medicaid** | 410 | **3.8** | 18.7 | 2.9 | **11.2%** |

A monotone gradient. Commercially insured women travel **3.3× the median
distance** of Medicaid women (12.4 vs 3.8 miles) and bypass their nearest capable
hospital **nearly three times as often** (32.0% vs 11.2%).

Part of this is residence — Medicaid women live closer to capable hospitals (2.9
vs 6.8 miles median). But the bypass column conditions on that, and the gradient
survives. The reading that fits both columns: **women with commercial coverage
exercise choice across the capable-hospital set; women on Medicaid largely use
the nearest one.** Managed-care variants sit below their fee-for-service
counterparts throughout (HMO 16.3% vs Commercial 32.0%), consistent with network
restriction.

This matters for the access surface directly: an E2SFCA model with one decay
kernel for all women will overstate effective access for Medicaid enrollees and
understate it for the commercially insured.

**By age:**

| Age band | Cases | p50 mi | p90 mi | Beyond 10 mi |
|---|---|---|---|---|
| 18–49 | 3,047 | 7.0 | 26.6 | 16.7% |
| 50–64 | 2,897 | 9.1 | 31.5 | 20.2% |
| 65–79 | 2,552 | 10.1 | 40.9 | **24.8%** |
| 80+ | 585 | 6.5 | 31.4 | 18.3% |

Travel rises with age to 65–79, then falls at 80+. The inversion is the
interesting part: the oldest women travel *less* despite the highest disease
prevalence — consistent with the oldest patients being least able to travel for
subspecialty care, and therefore most likely to receive it locally or not at all.

**By race:**

| Race | Cases | p50 mi | p90 mi | Nearest capable, p50 | Beyond 10 mi |
|---|---|---|---|---|---|
| White (R5) | 7,243 | 9.7 | 35.5 | 6.4 | 21.6% |
| Asian (R2) | 239 | 6.2 | 22.8 | 3.6 | 12.6% |
| Other (R9) | 457 | 4.3 | 20.3 | 2.8 | 11.2% |
| Black/African American (R3) | 629 | **3.0** | 16.5 | 1.8 | **11.6%** |

White women travel **3.2× the median distance** of Black women and bypass nearly
twice as often. As with payer, residence explains part of it (1.8 vs 6.4 miles to
nearest capable) and the bypass column does not.

**Interpret these three tables together, and cautiously.** Payer, race and
residence are heavily confounded in Massachusetts, this is one state, and CHIA
observes only admitted surgery. What the data support is that **travel for
urogynaecologic surgery is not uniform across the population**, which is
sufficient to establish that a single population-wide decay kernel is
mis-specified. What they do not support is a causal claim about which factor
drives it.

### 5.4a Access to a hospital that does urogynaecology AND takes your insurance

The supply set is **payer-specific**. A hospital that performs urogynaecologic
surgery but does not serve Medicaid patients is not available to a Medicaid
patient, and treating it as available is the central error in an access surface
built on one facility list. Pooled FY2007-2018, a site counts as
capable-for-payer when it performed >= 10 URPS operations for that payer group.

**The supply sets differ by roughly half:**

| Payer group | Capable hospitals |
|---|---|
| Private (BCBS, commercial, HMO, and their managed-care variants) | **25** |
| Medicaid (FFS + managed care) | **13** |

**Distance to the nearest hospital that does urogynaecology and takes your
insurance** (measured, miles):

| | Medicaid (n=933) | Private (n=4,239) |
|---|---|---|
| Median | 4.2 | 5.3 |
| 90th percentile | 16.1 | 19.4 |

**The insurance penalty** — extra distance to the nearest *in-network* capable
hospital, over the nearest capable hospital of any kind:

| | Medicaid | Private |
|---|---|---|
| Median | **+1.3 mi** | +0.1 mi |
| 90th percentile | **+3.2 mi** | +0.4 mi |

**The insurance constraint binds for Medicaid and is essentially inert for
private coverage.** A privately insured woman's nearest urogynaecologic hospital
is almost always in network; a Medicaid enrollee must pass roughly one
additional hospital, and at the 90th percentile three miles more.

**Drive time to the nearest in-network urogynaecologic hospital** (approximated
— no routing engine; see §5.3):

| | Medicaid | Private |
|---|---|---|
| Median | 8–11 min | 8–14 min |
| 90th percentile | 25–42 min | 30–50 min |

**Actual travel** (measured, miles):

| | Medicaid | Private |
|---|---|---|
| Median | 5.3 | 10.6 |
| 90th percentile | 24.8 | 34.6 |

Read the three tables together. Medicaid women live **closer** to urogynaecologic
hospitals (2.8 vs 5.2 miles to the nearest capable site of any kind) — urban
concentration. Their insurance then removes about half the supply set, costing
1.3 miles at the median. Privately insured women face no such constraint and
travel farther anyway (10.6 vs 5.3 median), which is **choice, not distance**.

Threshold sensitivity is modest: Medicaid median nearest in-network is 3.7 / 4.2
/ 4.2 miles at capability thresholds of 5 / 10 / 20 pooled cases.

#### Two limitations that bound what this can claim

**1. Acceptance is revealed, not contractual.** "Takes your insurance" here means
"performed >= 10 urogynaecologic operations for that payer group," which is
observed behaviour, not a network contract. A hospital may hold a Medicaid
contract and still appear incapable through low volume.

**2. This measures women who GOT surgery — the ones turned away are invisible.**
Every woman in this cohort has a discharge record, so by construction she
reached a hospital that operated on her. Women who could not obtain surgery
produce no record at all. If Medicaid coverage creates barriers that prevent
surgery outright, those women are absent from the numerator *and* the
denominator, which would bias the observed Medicaid distances **downward** —
only the successful, likely those nearest a willing hospital, are counted.

The second limitation is the important one: **this is a lower bound on the
Medicaid access burden, not an estimate of it.** Measuring the unmet portion
requires a denominator of women who needed surgery, which CHIA does not observe.
Builder: `scripts/chia/build_payer_specific_access.R`.

---

### 5.4b Massachusetts health reform is the backdrop to every payer comparison

Chapter 58 of the Acts of 2006 — "RomneyCare" — took effect across FY2007–2008
and is **visible in the CHIA payer taxonomy itself**. Three payer-type codes are
reform artefacts: `Q` Commonwealth Care/ConnectorCare and `H` Health Safety Net
were created by it, and `9` Free Care is the pre-reform uncompensated-care
category it replaced.

**Payer mix, female adult inpatient cohort (% of discharges):**

| FY | Self-pay | Free Care | Health Safety Net | Commonwealth Care | Medicaid | Private |
|---|---|---|---|---|---|---|
| 2004 | 1.36 | 1.90 | — | — | 11.73 | 37.11 |
| 2006 | 0.99 | 2.00 | — | — | 13.35 | 35.63 |
| **2007** | 0.85 | 1.66 | — | **0.31** | 14.17 | 35.28 |
| 2008 | 0.67 | 0.97 | **0.07** | 1.01 | 14.11 | 35.38 |
| 2010 | 0.64 | 0.31 | 0.77 | 1.60 | 14.16 | 33.70 |
| 2013 | 0.79 | 0.30 | 0.89 | 1.94 | 15.40 | 31.65 |
| **2015** | 0.55 | 0.09 | 0.40 | 0.99 | **18.09** | 29.86 |
| 2018 | 1.19 | **0.02** | 0.58 | 1.40 | 17.20 | 28.34 |

Free Care collapses from 2.0% to 0.02% exactly as Commonwealth Care (FY2007) and
Health Safety Net (FY2008) appear. Self-pay falls ~60%. Medicaid climbs
11.7% → 18.5%, with visible steps at FY2007 (Chapter 58) and FY2014–15 (ACA
expansion). Private coverage declines throughout, 37.1% → 28.3%.

#### What this does and does not do to §5.4a

**It is context, not cause.** The travel analysis runs FY2007–2018 — the
implementation year onward — so the payer gap reported there is measured
entirely within the post-reform regime. It is not a pre/post artefact.

**But the gap is closing, monotonically:**

| Era | Medicaid p50 | Private p50 | Gap |
|---|---|---|---|
| 2007–09 (early reform) | 5.2 mi | 12.5 mi | **7.3** |
| 2010–13 (mature) | 5.5 mi | 11.4 mi | **5.9** |
| 2014–18 (post-ACA) | 6.7 mi | 9.8 mi | **3.1** |

Convergence from both directions: Medicaid travel rises while private falls.

#### Why this cannot be attributed to the reform

**There is no patient geography before FY2007.** `PermanentPatientZIPCode`
begins in FY2007; FY2004–2006 discharges carry no patient ZIP at all. A genuine
pre/post comparison of travel is therefore **impossible with this extract** —
the reform's implementation year is the first year travel can be measured. What
the payer gap looked like under the pre-reform Free Care regime is unobservable.

Two rival explanations fit the convergence at least as well as improved access:

1. **Composition.** Medicaid grew 57% over the period. Expansion populations
   typically resemble the general population more than the original eligibility
   group did, which would pull Medicaid travel toward the private mean without
   any change in access.
2. **Private attrition.** Private coverage fell 37% → 28%, so its travel median
   is drifting for compositional reasons of its own.

Separating these needs either the pre-2007 geography CHIA does not have, or a
within-area design — tracking ZIP-level Medicaid travel among areas whose
coverage mix was stable. The second is buildable from what is held here and has
not been done.

**How to read the payer stratification given this.** The FY2007–2018 pooled
figures in §5.4a average across a regime that was still changing. The
insurance-penalty result (Medicaid +1.3 mi, private +0.1 mi) is a period average,
not a steady state, and the era table above suggests the penalty was larger
earlier. Any forward projection should use the post-ACA era rather than the
pooled value, and should not assume the convergence continues.

---

### 5.4c Within-ZIP design: the convergence is not composition

§5.4b showed the Medicaid–private travel gap closing (7.3 → 5.9 → 3.1 miles)
and named composition as a rival explanation: Medicaid grew 57%, and expansion
populations resemble the general population more than the original eligibility
group did, which would narrow the gap without any change in access.

That is testable without the pre-2007 geography CHIA lacks. Classify origin ZIPs
by whether their **coverage mix actually changed** — measured on the full
female-adult cohort (large n), then applied to URPS travel — and compare the gap
trend within each group. Composition predicts the narrowing concentrates where
the Medicaid population changed.

596 ZIPs have ≥100 cohort discharges in both the FY2007–09 and FY2014–18 windows:
**335 stable** (Medicaid share moved <3 pp) and **239 rising** (≥3 pp).

| ZIP group | Era | Medicaid p50 | Private p50 | Gap |
|---|---|---|---|---|
| **Stable coverage mix** | 2007–10 | 3.4 | 9.1 | **5.7** |
| | 2011–14 | 5.1 | 9.0 | **3.9** |
| | 2015–18 | 6.7 | 9.2 | **2.5** |
| **Rising Medicaid share** | 2007–10 | 3.6 | 9.1 | 5.5 |
| | 2011–14 | 3.8 | 8.0 | 4.2 |
| | 2015–18 | 4.5 | 8.6 | 4.1 |

**The result runs against composition.** The gap closes **3.2 miles in stable
ZIPs against 1.4 miles in changing ones** — the opposite of the compositional
prediction. In stable ZIPs private travel is flat across the whole period
(9.1 / 9.0 / 9.2) while Medicaid travel rises 3.4 → 6.7 miles. The convergence
is driven entirely by Medicaid women travelling farther in areas whose coverage
mix did not change.

#### Two cautions on reading it

**Farther is not self-evidently better.** A narrowing gap means Medicaid women
travel more like privately insured women. Whether that is improved choice or
increased burden depends on whether their nearest in-network option receded or
their chosen destination improved. The §5.4a figures favour choice — median
nearest in-network capable site is 4.2 miles while median actual travel reaches
6.7 in the last era, so these women are bypassing available nearer hospitals
rather than being pushed past them. That is suggestive, not settled.

**The cells are thin.** Stable-ZIP Medicaid counts are 112 / 109 / 125 per era.
Medians from ~110 observations carry real sampling noise, and no interval is
attached here. Treat the direction as informative and the magnitude as
provisional.

Builder: `scripts/chia/build_within_zip_payer_gap.R`.

---

### 5.5 Comparison: all inpatient surgery

For context, the same measurement across all 1,639,630 admitted operations on
adult women (99.0% geocoded): median 7.2 miles, p75 17.2, p90 36.2, p95 58.3.
Urogynaecologic patients travel modestly farther at the median (8.4 vs 7.2) but
face a supply set roughly a third the size.

### 5.6 A geocoding bias that was nearly shipped

Seven hospitals hold **unique institutional ZIPs with no ZCTA**: Baystate
(01199), Lahey Burlington (01805), UMass University (01655), Mercy Springfield
(01102), Lawrence General (01842), Cooley Dickinson (01061), Noble (01086).
They silently dropped **263,745 cases (15.9%)**, and they are western and
central Massachusetts — exactly where travel is longest, so the loss biased the
kernel toward short trips. A ZIP3-area centroid fallback took geocoding from
82.7% to 99.0%.

### 5.7 How to use this

1. Restrict the E2SFCA supply set to **urogyn-capable** facilities (§5.1).
2. Keep σ = 60 min for aggregate work — the stratified fit confirms it (§5.2a).
   Where the analysis can carry it, use
   `URPS_INPATIENT_SIGMA_BY_AVAILABILITY` instead of any single σ.
3. Treat the non-monotonicity as a finding about functional form, not noise.
4. Stratify the kernel by payer where the analysis supports it (§5.4).
5. Replace the drive-time approximation with HERE isochrones before publishing
   any minute-denominated figure (§5.3).

**Scope.** Massachusetts, admitted surgery, FY2007–2018, operator-defined
urogynaecology. `role: regional_external_validation`. It calibrates the shape of
the decay for this subspecialty; it does not make Massachusetts national.

---

## 6. What CHIA must not be used for

| Not this | Why |
|---|---|
| Total POP or sling volume | Increasingly ambulatory; inpatient is a shrinking, selected slice |
| Care seeking | Most pelvic-floor care never produces an admission |
| Appointment probability, wait time | Not observable in discharge data |
| Medicaid acceptance | Realised utilisation is not an acceptance probability |
| Office / pessary / PT / Botox utilisation | Invisible |
| National FTE requirement | Case counts are activity, not FTE; convert through wRVU and staffing |
| Retirement hazard | No age; and exit ≠ retirement under the ambulatory blind spot |
| Replacing `E2SFCA_DEFAULT_WEIGHTS` | See §5.2 and §5.3 |

## 7. What CHIA is good for

- **Regional external validation** of the accessibility surface — observed
  origin→destination flows for major surgery (§5).
- **Complex, admission-requiring case mix.** Inpatient prolapse falls 3,924 →
  974 (FY2004→2018) while **fistula holds flat at ~460** — CHIA sees the acuity
  tier that cannot migrate to an ASC.
- **Substitution**: URPS vs general ob/gyn vs urology performing the same
  inpatient operations, by NPI.
- **Facility-level surgical capacity and its geographic migration**, FY2004–2018.

Massachusetts is one state and `urpssim` is national. CHIA belongs in the
validation layer, with `role: regional_external_validation`, never replacing a
national estimand.

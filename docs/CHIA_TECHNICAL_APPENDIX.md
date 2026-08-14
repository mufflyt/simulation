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

**(a) The true decay is far steeper than Luo/Qi.** Beyond the immediate band,
observed use falls to ~0.22 where the generic weight assumes 0.68 — roughly a
threefold overstatement of how much distant supply is actually reachable. An
E2SFCA surface using Luo/Qi credits catchments with urogynaecologic capacity
that women demonstrably do not use.

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
2. Replace the Luo/Qi weights with `chia_urogyn_travel()` / the conditional
   kernel of §5.2 for urogynaecologic access, retaining
   `E2SFCA_DEFAULT_WEIGHTS` as the generic-accessibility scenario.
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

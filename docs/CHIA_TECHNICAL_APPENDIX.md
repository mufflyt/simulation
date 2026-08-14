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

## 5. Surgical travel (`R/geography-chia_inpatient_flows.R`)

n = 1,639,630 admitted operations on adult women, FY2007–2018, 99.0% geocoded.
Patient residential ZIP → facility ZIP (`ref.chia_facility_guide`, all 77 sites),
both to ZCTA centroids from `mufflyt/twostep`.

### 5.1 Measured — straight-line distance

| Band (miles) | Share | Cumulative |
|---|---|---|
| 0–5 | 40.6% | 40.6% |
| 5–10 | 20.3% | 60.9% |
| 10–25 | 23.5% | 84.5% |
| 25–50 | 9.6% | 94.1% |
| 50–100 | 3.8% | 97.9% |
| >100 | 2.1% | 100% |

Median **7.2 miles**; p75 17.2; p90 36.2; p95 58.3; p99 192.6.

### 5.2 Assumed — drive time

**There is no routing engine in this pipeline.** Drive time is
`miles × 1.3 circuity ÷ 40 mph`, and both constants are choices. They dominate:

| Assumed speed | ≤30 min share |
|---|---|
| 30 mph | 0.646 |
| 40 mph | 0.731 |
| 50 mph | 0.790 |

A **14-point swing** from the speed constant alone — wider than most effects this
kernel would be used to detect. Drive-time bands exist only for comparability
with the Luo/Qi band structure and must not be reported as observations. Real
drive times require the HERE isochrone pipeline in `mufflyt/isochrones`.

### 5.3 Against the generic E2SFCA weights

| Band | Nearest hospital available | Actually went | Luo/Qi | Observed rel. ≤30 |
|---|---|---|---|---|
| ≤30 min | 95.4% | 73.1% | 1.00 | 1.000 |
| 31–60 | 2.1% | 15.1% | 0.68 | 0.207 |
| 61–120 | 0.8% | 7.4% | 0.22 | 0.101 |
| 121–180 | 0.4% | 2.2% | 0.09 | 0.030 |

**33.7% of women travelled more than 15 minutes past their nearest hospital.**

The middle column is why these numbers do **not** replace
`E2SFCA_DEFAULT_WEIGHTS`. Read as raw shares, observed decay looks ~3× steeper
than Luo/Qi. Read as observed-versus-available, the 61–120 band is used ~9×
*more* than nearest-hospital assignment predicts. Those readings point opposite
ways and marginal shares cannot adjudicate: 95.4% of these women had a hospital
within 30 minutes, so the near-band share measures hospital placement as much as
willingness to travel. A substitute kernel needs a choice model over each
patient's full option set.

The kernel drifts outward over time — ≤30 falls 74.8% (FY2007) → 71.1%
(FY2018) while 61–120 rises 6.6% → 8.6% — consistent with regionalisation of
complex surgery.

### 5.4 Urogynaecology specifically — the supply set is not all hospitals

The figures above measure travel against **all** acute hospitals. For
urogynaecology that overstates availability: only **18–30 of ~76** Massachusetts
hospitals host any URPS operation in a given year, and only **4–16** reach 10
cases.

Restricting to operations by board-certified URPS surgeons (n = **9,081** at 38
sites, FY2007–2018, 100% geocoded):

| Band (miles) | Where patients went | Nearest **urogyn-capable** | Nearest **any** hospital |
|---|---|---|---|
| 0–5 | 33.9% | 47.8% | **73.9%** |
| 5–10 | 21.7% | 24.9% | 18.6% |
| 10–25 | 28.9% | 18.3% | 5.5% |
| 25–50 | 10.4% | 6.8% | 0.7% |
| 50–100 | 3.9% | 1.2% | 0.4% |
| >100 | 1.2% | 1.0% | 1.0% |

Distance to nearest facility, miles:

| | p50 | p75 | p90 | p95 |
|---|---|---|---|---|
| Actual travel | 8.4 | 17.8 | 31.8 | 50.5 |
| Nearest urogyn-capable | 5.3 | 10.8 | **22.4** | 34.1 |
| Nearest any hospital | 2.9 | 5.1 | **8.2** | 12.1 |

**Using all hospitals as the supply set overstates urogynaecologic accessibility
by roughly 3× in the tail** (8.2 vs 22.4 miles at p90). Any E2SFCA surface for
this subspecialty must restrict supply to facilities that actually perform the
surgery — otherwise it will report adequate access in regions that have a
hospital but no urogynaecologist.

**20.2%** of women travelled more than 10 miles past their nearest
urogyn-capable site.

The capability threshold is not delicate: median nearest-capable distance is
4.4 / 5.3 / 6.1 miles at thresholds of 1 / 10 / 25 annual cases
(`urogyn_site_threshold_sensitivity.csv`).

Note the definition is **operator-based** — an operation on the female-adult
cohort performed by a board-certified URPS surgeon. A procedure-code definition
awaits `config/chia_urps_inpatient_codes.yml`; see §2.1 for why hand-rolled code
families are not used.

---

### 5.4 A geocoding bias that was nearly shipped

Seven hospitals hold **unique institutional ZIPs with no ZCTA**: Baystate
(01199), Lahey Burlington (01805), UMass University (01655), Mercy Springfield
(01102), Lawrence General (01842), Cooley Dickinson (01061), Noble (01086).
They silently dropped **263,745 cases (15.9%)**, and they are western and
central Massachusetts — exactly where travel is longest, so the loss biased the
kernel toward short trips. A ZIP3-area centroid fallback took geocoding from
82.7% to 99.0%.

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

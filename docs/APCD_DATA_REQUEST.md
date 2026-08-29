# APCD / longitudinal all-payer claims — data request specification

**Purpose: measure `annual_first_urps_entry_rate` for UI, POP and AI, which is
the last unresolved input to the canonical demand pipeline.**

This is the rate-limiting item, and it is now a *data* problem rather than a
modelling one. The estimand was settled on 2026-08-17 in
[`PATHWAY_STATE_TRANSITION_AUDIT.md`](PATHWAY_STATE_TRANSITION_AUDIT.md) §7–§8,
the estimator is pre-registered in
[`INCIDENT_ENTRY_ESTIMAND.md`](INCIDENT_ENTRY_ESTIMAND.md), and the function
exists as `annual_first_urps_entry_rate()` in `R/demand-first_entry_rate.R`.
Nothing about the analysis may be adjusted after seeing the data.

---

## 0. The estimand this request must serve

```
                unique women with a first qualifying observed URPS entry in year t
q_pop(c,a,t) = ------------------------------------------------------------------
                ALL eligible prevalent women in condition c, age band a, year t
                          — regardless of prior care history —
```

The quantity being requested is the **annual population-level first-entry
rate**. Any framing that conditions on prior non-entry names a different
estimand with a different denominator, and this request does not ask for it.

**It is a population-level RATE, not a conditional hazard.** Women who entered
care in earlier years stay in the denominator and cannot appear in the
numerator, so historical depletion is embedded empirically in the measured rate.
No depletion correction is applied on top of it.

Two consequences govern every requirement below, and they pull in opposite
directions, which is why the request is easy to get wrong:

| | numerator | denominator |
|---|---|---|
| **source** | claims | the model's own prevalence science |
| **prior care history** | **required** — it is how "first observed" is established | **irrelevant** — must not restrict membership |
| **continuous enrolment** | **required** — distinguishes "no event" from "not observable" | **must not be required** |
| **provenance** | this APCD extract | external prevalence estimates |

Prior care history and continuous enrolment are numerator-classification tools.
Using either to define who belongs in the denominator converts the quantity into
the conditional never-entered hazard that §8 explicitly rejected.

> **This section is a correction, and the correction is the point.** An earlier
> revision of §3 of this document required subjects to be *"not already in the
> pathway at the start of the observation year"* and estimated an *"annual
> transition probability"* on that denominator. That is precisely the
> never-entered risk set §8 ruled out: it is not directly observable, it would
> require persistent per-woman state, and an APCD numerator over an external
> prevalence denominator does not estimate it. Left unrepaired, the protocol
> would have obtained exactly the right claims and then asked them the wrong
> question — after the multi-month latency of a DUA amendment.

---

## 1. What is blocked, and why existing data cannot unblock it

`condition_service_pathway()` ships `per_entering = 1.00` for
`new_consultation` on **all three limbs** — `ui`, `pop` and `ai`. That treats a
prevalence **stock** as an annual **flow**: every prevalent treated patient is
counted as newly presenting each year. Each limb refuses independently, and
`declared_invalid_parameters` marks all three rows INVALID in the pathway table.

| dataset | can it measure first entry? | why |
|---|---|---|
| **CHIA inpatient extract** (held) | **No** | inpatient-only; first entry is an outpatient consultation, so the numerator is the wrong care setting |
| MEPS | No | panel too short and sample too thin for condition-specific annual entry, especially POP and AI |
| Medicare FFS alone | Partial | age-restricted; pelvic-floor care-seeking spans pre-Medicare ages. Valuable as **independent replication at 65+** (§6) |
| **APCD / all-payer longitudinal claims** | **Yes** | patient-year histories with continuous-enrolment observability across payers and care settings |

## 2. Minimum viable specification

Split into what the primary estimator **cannot run without** and what
strengthens **replication and generalisability**. Conflating the two is how a
usable extract gets rejected for lacking something the estimator never needed.

### 2a. Required for the primary estimator

| requirement | value | why it is required, not preferred |
|---|---|---|
| **care setting** | outpatient **and** inpatient | the event of interest is an outpatient consultation; inpatient-only is what makes the held CHIA data unusable here |
| **longitudinal** | patient-level linkage across years | "first observed" is undefined without prior history |
| **continuous enrolment** | monthly eligibility/enrolment spans | establishes **numerator observability and washout** — it distinguishes "no event" from "not observable". It does **not** define denominator membership |
| **minimum span** | **≥ 4 consecutive years** | 24-month washout **plus** ≥ 2 observation years |
| **clinical detail** | ICD-10 diagnosis, CPT/HCPCS procedure, service date, place of service | needed to define a qualifying UI / POP / AI encounter |
| **rendering provider NPI** | present and usable | the index event is roster-linked to FPMRS clinicians in preference to payer taxonomy (§4 of the estimand contract) |
| **demographics** | age (or birth year), sex | age-band stratification |
| **geography** | state; county or ZIP if permitted | denominator alignment (§3c) and the geographic access layer |

### 2b. Payer coverage — required floor vs. desired

**Required floor:** broad multi-payer coverage of the **under-65** population,
where the entry process is least observable elsewhere. Commercial and Medicaid
are the load-bearing components.

**Desired, NOT disqualifying:** Medicare fee-for-service.

> **An earlier revision listed Medicare FFS inside an "all-payer" requirement,
> which disqualified this document's own recommended source.** MA APCD covers
> commercial, MassHealth and Medicare Advantage but **not** Medicare FFS, and
> some self-insured commercial coverage has been absent since *Gobeille* —
> stated plainly in `INCIDENT_ENTRY_ESTIMAND.md` §4. Read literally, the old
> minimum spec rejected the extract the same document recommended requesting.
> Medicare FFS is pursued as **independent replication at 65+** (§6), not as an
> eligibility bar on the primary extract.

Payer mix must still be **reported**, not assumed away: it is a stratification
variable (§9 of the estimand contract) and a known transport threat, since
pelvic-floor burden is concentrated in older women.

## 3. Analytic sample definition (fixed in advance)

Numerator and denominator are specified **separately**, from **different
sources**, and are never derived from one another.

### 3a. Numerator — from these claims

Unique women with a **first qualifying observed URPS entry** during an
observation year:

1. Female, age ≥ 18.
2. Continuously enrolled through the **24-month washout** preceding the
   observation year, with **no** qualifying encounter for that condition during
   the washout. This is what makes the entry *first observed*.
3. Continuously enrolled during the observation year, so that "no event" is
   distinguishable from "not observable".
4. An index event during the observation year: the first outpatient evaluation
   by a roster-linked urogynecology/FPMRS clinician carrying the relevant
   condition diagnosis.

**Count unique women, not visits.** Ten visits by one newly entered patient are
one entry; visits-per-entered-patient is a separate quantity measured
separately.

**Do not use CPT "new patient" codes.** A woman can be clinically new to
urogynecology yet fail the billing definition because of prior care in the same
group.

Three parallel cohorts — **UI, POP and AI** — each with its own condition
definition. Not two.

### 3b. Denominator — from the model's prevalence science, NOT from these claims

```
P(c,a,t) = N(a,t) × Pr(eligible prevalent symptomatic c | a)
```

All eligible prevalent women in that condition, age band and year, **regardless
of prior care history and regardless of enrolment status**. It mixes
never-treated women, previously-treated-but-still-symptomatic women, and women
with recurrent disease — deliberately.

**Explicitly NOT applied to the denominator:**

- ~~not already in the pathway at the start of the year~~ — the rejected
  never-entered risk set
- ~~continuously enrolled~~ — an observability condition for the numerator
- ~~no qualifying event during washout~~ — a first-observed classification rule

A claims-derived rate such as *"1.8 new POP consultations per 1,000
women/year"* is **not** the estimand. It is the numerator only.

### 3c. Population alignment — geography, years, age bands, AND payer/coverage universe

A **state** APCD numerator divided by a **national** prevalence denominator is a
category error, and a single ratio makes it invisible. **So is a
payer-restricted numerator divided by an all-population denominator**, and that
one is easier to miss because the geography and years match.

#### The bias this closes

MA APCD covers commercial, MassHealth and Medicare Advantage. It does **not**
include Medicare fee-for-service, and some self-insured commercial coverage has
been absent since *Gobeille*. Dividing what it observes by every eligible
prevalent woman in Massachusetts gives

```
      first entries observable in APCD-covered payers
  ----------------------------------------------------
        ALL eligible prevalent women in the state
```

which is **not** the canonical rate. It is biased **downward** by exactly the
entries occurring outside the coverage universe — and because pelvic-floor
burden is concentrated in older women, the missing stratum is Medicare FFS,
i.e. the one with the highest expected entry rate. This is a material bias, not
a rounding concern. `INCIDENT_ENTRY_ESTIMAND.md` §4 already flags the coverage
gap; this section is what stops it from silently entering the arithmetic.

Note that §2b's ruling and this one are consistent, not in tension: Medicare FFS
is **not** an eligibility bar on the extract, **and** its absence must be
carried explicitly through the denominator rather than ignored. Relaxing the
requirement without this section would have converted a stated limitation into
an unstated bias.

#### The requirement

Estimate **payer/coverage-stratified** rates, each with a denominator drawn
from the same target population as its numerator:

```
                  first observed entrants in payer/coverage stratum p
q(c,a,t,p) = -----------------------------------------------------------
              eligible prevalent women in the SAME c, a, t, p population
```

| numerator stratum | denominator must be |
|---|---|
| commercial APCD | commercial-covered eligible prevalence |
| MassHealth | MassHealth-covered eligible prevalence |
| Medicare Advantage | MA-covered eligible prevalence |
| Medicare FFS (§6) | separately obtained FFS denominator |

Then **standardise or transport** the stratum-specific rates into the desired
state or national target population, as a separate and argued step.

#### What continuous enrolment still is, and is not

Aligning the payer universe does **not** reintroduce an enrolment restriction on
the denominator. Continuous enrolment remains a **numerator observability and
washout** rule. The denominator is the eligible prevalent women *in that
coverage population*, whether or not any individual was continuously enrolled.

#### If full payer alignment is unavailable

Payer-specific prevalence may not be estimable. That is an acceptable outcome
and an unacceptable silence. In that case:

- the result is labelled an **"APCD-covered-population estimate"**, never the
  complete Massachusetts `annual_first_urps_entry_rate`;
- the transport assumption is **stated explicitly**, including the expected
  direction of the bias (downward);
- the label travels with the number into the model, so a coverage-limited
  estimate cannot be adopted as the canonical rate by inheritance.

#### Mechanics

- `annual_first_urps_entry_rate()` **returns `entrants_n`, `eligible_prevalent_n`
  and the rate separately**, and requires `numerator_source` and
  `denominator_source`. It refuses to run without both — precisely so a
  numerator and denominator drawn from different populations cannot be collapsed
  into one unquestionable number.
- The denominator must be constructed for **the same state, years, age bands and
  payer/coverage universe** as the claims extract. The first three are necessary
  and **not sufficient**.
- Any transport to a national estimate is a **separate, argued step**, recorded
  as such.

Estimation is by **Wilson interval**, stratified by age band and payer where
cell counts permit, with index-month right-censoring handled explicitly — the
same error corrected in the MEPS Panel 27 re-estimation.

## 4. Disclosure and governance

- **Aggregate outputs only.** Nothing patient-level leaves the analytic
  environment.
- **Small-cell suppression at n < 11.** The CHIA aggregates already tracked in
  this public repository hold to that (minimum observed cell 110), and the same
  bar applies here.
- Restricted source files stay gitignored; only estimates and their
  denominators are committed.
- Existing DUA constraints are unchanged: CHIA `.mdb` files and documentation
  remain restricted, and no CHIA, CADR, sling-claims or mystery-caller data is
  used to define prevalence.

## 5. Candidate sources

| source | note |
|---|---|
| **MA CHIA APCD** (outpatient release) | we already hold a CHIA DUA; an outpatient/all-payer amendment is the shortest path. No Medicare FFS — see §2b; this does **not** disqualify it |
| CO APCD | mature, well-documented research release |
| Other state APCDs | viable; generalisability to national estimates needs an explicit transport argument (§3c) |
| HCCI / MarketScan | commercially licensed, longitudinal, but commercial-only — payer-mix bias against exactly the older women who carry most of the burden |

**Recommended first action: amend the existing CHIA DUA to add the outpatient
all-payer file.** It reuses governance already in place, which is where most of
the latency usually sits.

## 6. Medicare FFS — independent replication, in parallel

Run the same algorithm in Medicare FFS (beneficiary-level enrolment via the
Master Beneficiary Summary File) and compare age-standardised entry rates at 65+
against the APCD estimate. Concordance between two independent populations is
far stronger evidence than fitting the parameter until national utilization
comes out right.

CMS RIF processing commonly takes **3–5 months**. Start it now, in parallel — it
must not become the critical path, and under §2b it is not a precondition.

## 7. What happens when the data arrive

The pre-registered estimator runs as written, into
`annual_first_urps_entry_rate()`, across the sensitivity matrix in
`INCIDENT_ENTRY_ESTIMAND.md` §8 (washout 12/24/36 months, index-event and
case-definition variants, enrolment-gap tolerance).

If the denominator cannot be aligned, or the 24-month washout cannot be
satisfied, the parameter **stays unresolved and the pipeline stays refusing**. A
number that cannot be defined is not preferable to no number.

The **0.297 utilization ratio is a holdout**, compared once, without adjusting.
Only after the rate is sourced does it become meaningful to ask whether the
8.51× POP discrepancy collapses. Answering that by tuning this parameter is what
the guard exists to prevent.

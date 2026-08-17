# APCD / longitudinal all-payer claims — data request specification

**Purpose: unblock `per_entering` for `new_consultation`, which is currently
refusing the production demand pipeline.**

This is the rate-limiting item. Access latency is outside our control, so the
request should move before any further modelling work on the POP cascade. The
estimand and estimator are already fixed in
[`INCIDENT_ENTRY_ESTIMAND.md`](INCIDENT_ENTRY_ESTIMAND.md), deliberately, so
that nothing about the analysis can be adjusted after seeing the data.

---

## 1. What is blocked, and why existing data cannot unblock it

`condition_service_pathway()` ships `per_entering = 1.00` for
`new_consultation` on both the `pop` and `ui` limbs. That treats a prevalence
**stock** as an annual **flow**: every prevalent treated patient is counted as
newly presenting each year. `assert_incident_not_prevalent()` refuses it at
ratio exactly 1.00 against a production cohort of 315,544, and 31 tests are
held red pending resolution.

| dataset | can it measure incident entry? | why |
|---|---|---|
| **CHIA inpatient extract** (held) | **No** | inpatient-only; new consultations are outpatient, so the numerator is the wrong care setting and the denominator is unobservable |
| MEPS | No | panel too short and sample too small for a POP-specific annual transition |
| Medicare FFS | Partial at best | age-restricted; POP care-seeking spans pre-Medicare ages |
| **APCD / all-payer longitudinal claims** | **Yes** | patient-year histories with continuous-enrolment observability across payers and care settings |

## 2. Minimum viable specification

| requirement | value | why it is required, not preferred |
|---|---|---|
| **care setting** | outpatient **and** inpatient | the event of interest is an outpatient consultation; inpatient alone is what makes the held CHIA data unusable here |
| **longitudinal** | patient-level linkage across years | incidence is undefined without prior history |
| **continuous enrolment** | monthly eligibility/enrolment spans | distinguishes "no event" from "not observable" — without it the denominator is guesswork |
| **minimum span** | **≥ 4 consecutive years** | 24-month washout **plus** ≥ 2 observation years |
| **payer coverage** | all-payer | commercial, Medicaid, Medicare Advantage, Medicare FFS; single-payer extracts bias the age distribution |
| **clinical detail** | ICD-10 diagnosis, CPT/HCPCS procedure, service date, place of service | needed to define a qualifying POP consultation/treatment event |
| **demographics** | age (or birth year), sex | age-band stratification |
| **geography** | state; county or ZIP if permitted | supports the geographic access layer |

## 3. Analytic sample definition (fixed in advance)

1. Female, age ≥ 18.
2. Continuously enrolled ≥ 24 months with **no** qualifying POP event —
   the washout.
3. Prevalent POP and eligible to enter the modelled pathway.
4. **Not** already in the pathway at the start of the observation year.

An index event is a qualifying POP consultation or treatment following the
washout. The annual transition probability is estimated on the denominator at
(4), with a Wilson interval, stratified by age band where cell counts permit.
Index-month right-censoring is handled explicitly — the same error corrected in
the MEPS Panel 27 re-estimation.

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
| **MA CHIA APCD** (outpatient release) | we already hold a CHIA DUA; an outpatient/all-payer amendment is the shortest path |
| CO APCD | mature, well-documented research release |
| Other state APCDs | viable; generalisability to national estimates needs a transport argument |
| HCCI / MarketScan | commercially licensed, longitudinal, but commercial-only — payer-mix bias |

**Recommended first action: amend the existing CHIA DUA to add the outpatient
all-payer file.** It reuses governance already in place, which is where most of
the latency usually sits.

## 6. What happens when the data arrive

The estimator in `INCIDENT_ENTRY_ESTIMAND.md` §4 runs as written. If the
denominator turns out not to be observable, or the 24-month washout cannot be
satisfied, the parameter **stays unresolved and the pipeline stays refusing** —
a number that cannot be defined is not preferable to no number.

Only after `per_entering` is sourced does it become meaningful to ask whether
the 8.51× POP discrepancy collapses. That question is not answerable now, and
answering it by tuning this parameter is what the guard exists to prevent.

# Incident entry into the care pathway — estimand and pre-registered estimator

`condition_service_pathway()` `per_entering` for the `new_consultation` service.

**Status: UNRESOLVED. The production pipeline refuses.** This document exists to
fix the estimand and the estimator *before* any data arrive, so that the
definition cannot later be adjusted until the answer looks right.

Prepared 2026-08-16 on `feat/chia-inpatient-surgical-layer`.

---

## 1. The defect

`per_entering` for `new_consultation` shipped as **1.00** for both `pop` and
`ui`. That converts a **stock into an annual flow**: every prevalent treated
patient is counted as newly presenting, every year.

`assert_incident_not_prevalent()` now refuses it at the point volumes are
computed:

```
new_consultation volume is 315,544 against a treated cohort of 315,544
(ratio 1.00). Every prevalent treated patient is being counted as a NEW
patient annually.
```

Ratio exactly 1.00 at production scale. This was silently permitted until the
guard was wired, and 30 tests now refuse. **Those refusals are correct and are
to remain.**

## 2. The estimand

> **Among women with prevalent POP who are eligible to enter the modelled care
> pathway and are not already in it, what proportion NEWLY enters the
> conservative / new-consultation state during this model year?**

Formally, an annual transition probability:

```
per_entering(new_consultation)
  = P(qualifying new consultation in year t
      | prevalent condition, eligible, NOT already in the pathway at t-1)
```

| | |
|---|---|
| **unit** | woman-year |
| **numerator** | women with a qualifying POP consultation/treatment event in year *t* |
| **denominator** | eligible prevalent women **not already in the pathway** at the start of *t* |
| **type** | flow (annual transition probability), **not** a prevalence share |
| **range** | strictly `(0, 1)`; `1.0` is the defect, not an upper bound in use |

### What it is NOT

- **Not** the fraction of prevalent POP patients who exist this year. That is a
  stock, and treating it as a flow is precisely the shipped error.
- **Not** the care-seeking or treatment-seeking prevalence.
- **Not** anything derived from the utilization anchor — see §5.

## 3. Required data source

**Longitudinal all-payer claims / APCD**, patient-year histories with
continuous-enrolment observability.

Incidence is defined by a **washout**: a patient becomes incident when a
qualifying POP consultation/treatment event follows a sufficient period of
prior *observable* enrolment containing no qualifying event.

| parameter | value | rationale |
|---|---|---|
| washout | **≥ 24 months** continuous enrolment, no qualifying event | POP is chronic and recurrent; 12 months misclassifies returning patients as incident |
| denominator | prevalent + eligible + not in pathway at start of year | matches the estimand exactly |
| stratification | age band, if cell counts support it | entry rates are strongly age-dependent |
| censoring | index-month right-censoring handled explicitly | the same error corrected in MEPS Panel 27 |

### CHIA inpatient data must NOT be used for this parameter

The CHIA extract on this branch is **inpatient-only**. New consultations are an
**outpatient** event, so inpatient records cannot distinguish prevalent from
incident care-seeking — the denominator is unobservable and the numerator is
the wrong care setting. Using it would produce a number with no relationship to
the estimand. It remains valid for the inpatient surgical-volume work it was
acquired for.

## 4. Pre-registered estimator

Fixed now, before data access, so the definition cannot be tuned to the answer.

1. Build patient-year panels with continuous-enrolment flags.
2. Apply the **24-month** washout to classify each patient-year as
   *already-in-pathway*, *eligible-not-entered*, or *incident-entry*.
3. Restrict the denominator to *eligible-not-entered* at the start of the year.
4. Estimate the annual transition probability, with a Wilson interval; stratify
   by age band where cells permit.
5. Report the estimate **with its denominator**, so the flow/stock distinction
   is auditable in the output rather than implied.

**Stopping rule.** The estimate is adopted only if the denominator is
observable and the washout is satisfiable. If either fails, the parameter stays
unresolved and the pipeline stays refusing. A number that cannot be defined is
not preferable to no number.

## 5. The 0.297 ratio is a magnitude check, NOT an estimator

| | |
|---|---|
| predicted `urps_office_visits` | 16,226,458 |
| target | 4,814,760 |
| target / predicted | **0.297** |

> Correcting prevalent-as-incident classification is expected to reduce
> utilization substantially; the observed utilization ratio is 0.297, which
> provides an independent magnitude check but **is not used to estimate the
> incident-entry parameter.**

The ratio is consistent with the direction and rough scale of the defect, and
that is all it establishes. It does **not** show the true incident share is
29.7%: other pathway limbs contribute to the same discrepancy, and adopting it
would be back-solving a parameter from the anchor it is supposed to be
validated against — the exact failure mode `config/calibration_targets.yml`
forbids.

Any exploratory sensitivity analysis using a provisional value must be labelled
a scenario and must not be written into the pathway table as calibrated.

## 6. UI carries the same defect

`ui/conservative/new_consultation` also ships `per_entering = 1.00` and has the
same stock-as-flow error. It is not currently the binding constraint on the
reported discrepancy, but the same estimand and estimator apply and it should
be resolved from the same data extract.

## 7. Blocking status

- The guard stays wired and stays refusing.
- `per_entering` stays unresolved. It is **not** set to a placeholder.
- **`R CMD check` is green and the scientific-readiness gate is red**, and that
  separation is deliberate. The package behaves correctly — the guards refuse
  an invalid configuration and the tests assert that refusal — while the
  canonical parameterization is not scientifically runnable. Collapsing the two
  would cost both signals: a permanently red check stops meaning "the code is
  broken", and a green one that tolerated failing tests would be a lie.
- The canonical run is exercised by
  `.github/scripts/assert-canonical-science.R` (workflow
  `scientific-readiness`), which uses the REAL pathway — no fixture — and
  **is expected to fail**. It must not be muted or made non-blocking.
- Two fixtures work around the blocker for tests of machinery, each labelled
  as a fixture and not a candidate value: `valid_pathway()` in
  `tests/testthat/helper-setup.R` and `.github/scripts/_pathway_fixture.R`.
  Both should be **deleted** once the parameter is sourced.
- Unblocked by: APCD / longitudinal all-payer outpatient claims access.

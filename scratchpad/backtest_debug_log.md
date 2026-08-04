# Back-test debugging log

Working file for the 2020→2023 back-test investigation. Diagnostics live in
`scratchpad/backtest_diagnostics.R`; the shipped scoring is
`scripts/run_backtest_2020_to_2023.R`.

---

## Iteration 1 — 2026-08-04 ~05:30 MDT

**State check.** No back-test process running (`ps aux | grep -c "[r]un_backtest_2020"`
→ 0). The forced full run completed: `artifacts/.backtest_raw.rds` 05:38,
`artifacts/backtest_2020_to_2023_summary.csv` 05:40, exit 0. All 8 arms × 1000
iterations present.

**Hypothesis.** The 0/8 coverage was an artifact of intervals that contained no
parameter uncertainty, because `run_backtest()` never passed the `param_spec`
that `run_backtest_arm()` accepts.

**Diagnostic.** `grep -n "param_spec" R/35-backtest_run.R` — argument declared
and documented at line 31-33, passed by nothing.

**Finding.** Confirmed. PI95 widths in the pre-fix artifact: 35, 0, 34, 2, 40, 0,
40, 3 providers on a count near 1,300. Two arms had **literally zero width** —
no attrition and an integral entrant rate leaves nothing to vary.

**Files changed.** `R/35-backtest_run.R` (per-arm `arm_spec()`),
`R/36-parameter_uncertainty.R` (draw centred on `entrant_mean`).

**Result.** Widths 129–148. Coverage 0/8 → 2/8.

**Next.** Determine whether the remaining 6 failures are bias or variance.

---

## Iteration 2 — same session

**Hypothesis.** A single shared `param_spec` would overwrite `entrants_per_year`
on every iteration, collapsing the assumed-entrant arms (1, 3) into the
estimated-entrant arms (2, 4) and destroying the prespecified contrast.

**Diagnostic.** Read `run_backtest_arm()`: `it_entrants <- d$entrants`
unconditionally overrides the argument.

**Finding.** Confirmed — this is why the spec is built **per arm**, centred on
that arm's own entrant value, taking only the *spread* from the observed series.
Regression test added: `test-backtest.R` "run_backtest gives every arm
uncertainty but keeps its own centre".

---

## Iteration 3 — uncertainty decomposition

**Command.** `Rscript scratchpad/backtest_diagnostics.R`
(derived cohort, definition-matched/no-attrition, n = 1000, seed 20260802)

| family | sd | width 95 | median |
|---|---|---|---|
| none (individual stochasticity only) | 0.81 | 3.0 | 1197 |
| entrants only | 34.66 | 138.0 | 1198 |
| attrition only **[INVENTED cv = 0.20]** | 13.67 | 54.0 | 1112 |
| entrants + attrition **[INVENTED cv]** | 34.82 | 137.1 | 1113 |

**Finding.** Entrant-rate uncertainty is ≈97% of predictive variance. The
`cv = 0.20` row is a *sizing experiment only* — the published hazards carry no
standard errors, so that spread is not shipped and must not be.

**Structural checks — all clean, no arithmetic defect:**

- `n0` = 1,099 = observed 2020 exactly. Starting stock is correct.
- Transitions advanced = **3.018** (expect 3). The 0.018 is the fractional-entrant
  Bernoulli. **No off-by-one** between `cutoff_year` and `target_year`.
- Entrant rate that reproduces 2023 exactly: **69.0/yr**.
- Pre-cutoff window 2018–2020: **40, 48, 10** → mean 32.67, sd 20.03.
- Realized 2021–2023: **81, 54, 72** → mean 69.0.

**Conclusion — the residual failure is BIAS, not variance.** The pre-cutoff
entrant mean (32.67) is 2.1× below the realized rate (69.0). No defensible
widening closes that: the entrants-only interval already spans ±69 and the
centre would have to move ~108.

**Crash found and fixed.** `supply_parameter_spec(hazard_cv = 0.2)` with no
`entrant_mean` made `run_backtest_arm()` assign `NULL` to `it_entrants`,
producing `Error in rep(NA_real_, capacity - n0) : invalid 'times' argument`
from deep inside the engine. `run_supply_microsimulation()` already guarded
this; `run_backtest_arm()` did not. Guard made consistent in
`R/35-backtest_run.R`.

---

## Iteration 4 — why the pre-cutoff entrant rate cannot reach 69

**Hypothesis.** The 2018–2020 window is unrepresentative for reasons knowable at
the cutoff, so the miss is a data-availability limit rather than a model defect.

**Finding.**

- 2020 = 10 certifications is an **examination-scheduling artifact** (COVID); the
  2021 = 81 spike is its catch-up. Mean of the pair is 45.5.
- Even excluding 2020 entirely, 2018–19 averages 44 → projects 1,231 vs 1,306.
- Reaching 69/yr requires knowing that **URPS fellowship output expanded
  structurally**. That is knowable pre-2020 *only* from an NRMP appointment-year
  series.

**Blocker (data, not code).** `data-raw/calibration/nrmp_fellowship_entrants.csv`
carries a **single row, appointment year 2025**. There is no pre-2020 NRMP
series in the repository. Using the 2025 value in a 2020-cutoff arm would be
temporal leakage and is refused.

**Decision.** Do not invent a pre-cutoff entrant series. Document the missing
input. The back-test verdict stays FAILED.

---

## Iteration 5 — acceptance criteria review (queue item 8)

**Question.** Is "≥80% of 8 arms cover" a statistically defensible criterion?

**Finding — two independent problems, both knowable a priori:**

1. **There is exactly ONE validation observation** (2023 = 1,306). The 8 arms are
   8 configurations scored against the same number, sharing `n0`, the same
   entrant series and the same target. They are not 8 independent Bernoulli
   trials, so "coverage = k/8" does not estimate interval coverage at all. A
   single target year cannot support a coverage claim in either direction.
2. **4 of the 8 arms are definition-mismatched by construction.** They apply
   attrition to project an active-workforce stock, then score it against a
   cumulative certification series that removes nobody
   (`observed_series_applies_attrition = FALSE`). Those arms are *expected* to
   under-predict. This was knowable before the result — the code already labels
   the others "definition-matched".

**Action taken.** The pass/fail bar was **not** loosened. Changing it would not
help: among the 4 definition-matched arms coverage is 2/4, which still fails 80%.
Instead the status now reports the definition-matched subset separately and
records that a single target year cannot support a coverage claim, so the
diagnostic is honest in both directions.

---

## Iteration 6 — demand calibration, and a sex-coding defect found on the way

**Hypothesis.** NAMCS 2019 is present locally, so an independent national anchor
is derivable and `assert_demand_calibrated()` need not warn forever.

**Diagnostic.** Built the anchor; the female-adult URPS cell came back with
**14 unweighted records** — implausibly thin — and the file's overall sex split
was 4,609 / 3,641 in favour of SEX = 1, backwards for an ambulatory survey.

**FINDING — NAMCS codes SEX 1 = FEMALE, 2 = MALE.** The reverse of Census, ACS,
BRFSS and MEPS, all of which this package also reads. `namcs_urps_stratum_visits()`
had `if_else(SEX == 2L, "Female", "Male")`. Every NAMCS-derived "female"
quantity was built from **male** visits — for a female-predominant subspecialty
that silently substitutes the complement of the estimand.

Verified against sex-specific diagnoses in the file itself:

| code | meaning | SEX=1 | SEX=2 |
|---|---|---|---|
| N40 | benign prostatic hyperplasia (male only) | 0 | 136 |
| C61 | prostate cancer (male only) | 0 | 68 |
| Z34 | supervision of pregnancy (female only) | 108 | 0 |
| N81 | female genital prolapse | 18 | 0 |
| C50 | breast cancer | 43 | 0 |

Corrected cell: **55 records, 4,814,760 weighted visits** — above the NCHS
30-record reliability floor.

**MEPS checked the same way and is CORRECT** (`SEX = 2` is female there:
N40 279/0, C61 147/0), so `R/48` needed no change. Verified, not assumed.

**Files changed.** `R/45-namcs_urps_visit_equations.R`,
`data-raw/namcs/01-namcs_acquire.R`, new `R/52-namcs_demand_calibration.R`,
new `tests/testthat/test-namcs-demand-calibration.R`.

**Result.** Scalar = 4,814,760 / 10,316,893 = **0.467**, unflagged and inside
HDMM's published 0.243–1.665 range (nearest: Family Medicine 0.492).

**Leakage.** None possible: the back-test is supply-only. `run_backtest()` and
`run_backtest_arm()` contain no reference to demand, service volumes or
calibration. The NAMCS 2019 public release (2021) therefore cannot reach a
2020-cutoff arm.

---

## Iteration 7 — calibration was checked but never applied

**Finding.** `assert_demand_calibrated()` gated on `calibration`, and
`apply_calibration_scalars()` was called by **nothing** in a workforce run. A
caller who supplied scalars got uncalibrated output that reported itself as
calibrated. Now applied — and only to `new_consultation` + `return_visit`; the
nine procedure rows are untouched, because a visit-count anchor is not evidence
about sling volume.

---

## Iteration 8 — the entrant-policy scenarios were inert

**Diagnostic.** The calibrated run emitted, once per scenario:
`entrants_per_year = 77 was passed but param_spec carries entrant_mean = 50.83,
which takes precedence`.

**FINDING.** One `param_spec` was shared across all nine supply scenarios, and
`entrant_mean` beats `entrants_per_year` inside the engine. Every scenario ran
at the same entrant rate. In the pre-fix run, **"Fellowship output +10%" and
"-10%" returned 2560 / 2111 / 2969 — identical to Baseline in all three
columns.** The most policy-relevant lever in the model did nothing.

**Fix.** `recentre_entrant_spec()`: the scenario sets the LEVEL, the observed
series sets the SPREAD. Same principle already applied to the back-test arms.

**Result.** Fellowship +10% → 2205, Baseline → 2116, −10% → 1911. Lever live.

**Second-order finding.** Because the old spec carried the double-counted
`mean(series) + departures = 86.9`, the shipped model was projecting **86.9
entrants/yr, not the documented 55** — `baseline_entrants` never reached the
engine at all. The 2050 surplus falls from +975 FTE (61.5%) to +534 FTE (33.8%)
once the level is correct and demand is calibrated.

---

## Iteration 9 — delegation matrix (queue item 7)

**Question.** Measured, literature, borrowed, judgement, or unprovenanced?

**Finding.** Borrowed and explicitly declared: Forte et al. physiatry shares,
subspecialist level rescaled by 0.434, `URPS_DELEGATION_STATUS =
"derived_by_analogy"`.

**Can it be measured?** *Partly, and not the part that matters.*

- `data-raw/cms_psps/` holds only `DOWNLOAD.md` — no PSPS extract.
- The Medicare realized-care artifacts DO carry `provider_type`. But Medicare
  has **no urogynaecology provider type**: URPS subspecialists bill as Urology
  (82.3% of basket volume) or OB/GYN (9.1%), indistinguishable from generalists.
  **`urps_share` — the column the FTE calculation turns on — is not identified.**
- The APP share *is* measurable, and corroborates the borrowed matrix in shape
  but not level: **Spearman rho 0.72** across services, with the matrix 2–4×
  higher in level. Care-management services rank most-delegated in both.
- Medicare's URPS basket contains no E/M codes, so `new_consultation`,
  `return_visit` and `postoperative_care` — where delegation is highest — are
  unobserved entirely.
- **Incident-to billing** makes every claims-measured APP share a LOWER bound.

**Decision.** Matrix unchanged and still declared an assumption. Added
`medicare_delegation_corroboration()` (carrying its own caveats) and
`delegation_capacity_sensitivity()`. The sweep shows the 0.434 constant is a
first-order lever: 0.30 → 0.69×, 0.60 → 1.38× of default URPS work RVUs, i.e.
**a factor of two across a plausible range**, flowing straight into required FTE.

**Data required to actually measure it:** NPPES taxonomy 207VX0201X joined to
the Medicare Provider & Service PUF by NPI (obtainable, highest value); a
fielded URPS practice survey (only way to capture incident-to work); all-payer
claims (Medicare FFS is ~65+).

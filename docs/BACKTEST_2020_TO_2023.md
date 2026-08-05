# Historical back-test: fit through 2020, project 2021–2023

**Result: the back-test failed in all eight arms.** Every arm under-predicts the
2023 count, and the observed value falls outside the 95% prediction interval
everywhere. No parameter was re-tuned after the error was examined.

Run: 1,000 Monte Carlo iterations per arm, seed 20260802, using the same engine
as the main projections.

---

## 1. Target reconciliation

The project contains several 2023 counts. They are not interchangeable, and the
back-test would be meaningless scored against the wrong one.

| Value | Geography | Pathway | Measure | Contract | Status | Certification-year basis |
|---:|---|---|---|---|---|---|
| **1,306** | national | ABOG_PLUS_ABU | board_certified_active | **3.0.0** | **current** | **URPS subspecialty** |
| 1,303 | CONUS | ABOG_PLUS_ABU | board_certified_active | 3.0.0 | current | URPS subspecialty |
| 1,332 | national | ABOG_PLUS_ABU | board_certified_active | 2.1.0 | **retired** | primary board |
| 1,329 | CONUS | ABOG_PLUS_ABU | board_certified_active | 2.1.0 | **retired** | primary board |
| 1,027 | national | ABOG only | board_certified_active | 3.0.0 | current | URPS subspecialty |
| 1,339 | national | ABOG_PLUS_ABU | roster_snapshot | 3.0.0 | current | 2025 headcount, not 2023 |

**1,306 is the target.** The simulated cohort is constructed from the same
contract on the same basis: national geography, ABOG_PLUS_ABU pathway,
`board_certified_active` measure, contract v3.0.0, keyed on the **URPS
subspecialty certification year**.

**1,332 / 1,329 are rejected.** They come from the retired v2.1.0 contract keyed
on the **primary board certification year** — a different certification-year
treatment. `mufflyaccess::urps_retired_values()` returns exactly `c(1332, 1329)`,
confirming their deprecated status. Scoring a subspecialty-cert-year cohort
against a primary-cert-year target would inflate apparent error by ~2% for no
modelling reason.

`validate_backtest_target()` checks geography, pathway, measure, contract
version, certification-year basis, and the retired-value list, and **stops** on
any mismatch. A wrong pathway or measure raises an error, never a warning.

### A definition mismatch that changes what the comparison means

`n_retired` is **0 in every row** of the contract series, and `n_active` equals
`n_ever_certified` in every row. The observed series therefore applies **no
attrition** — it is a cumulative certification series, not an active count net
of departures. The simulation *does* apply retirement hazards.

These are not the same quantity, and the model will structurally under-predict
against it. `validate_backtest_target()` fails closed on this; proceeding
requires `acknowledge_no_attrition = TRUE`. Both comparisons are reported below:

- **Primary** — the model as specified, applying attrition. This is the model's
  real prediction of the active workforce, but it is scored against a series
  that never retires anyone.
- **Definition-matched** — attrition disabled on the model side, so both sides
  are cumulative certifications. This isolates the **entrant model**, which is
  the part the observed series can genuinely test.

## 2. Leakage control

Every contract read routes through `.series_through()`, which filters to the
cutoff and records the maximum year touched. `assert_no_leakage(2020)` runs
*before* the observed series is read for scoring, and fails if any read reached
2021 or later. An unaudited run also fails — silence does not count as success.

The entrant estimate uses **2018–2020 only**. The main model's estimator uses
2018–2023, which would leak the entire validation window; a test asserts the two
give materially different answers.

The steady-state window still begins in 2018 because net growth averaged 86.5/yr
over 2014–2017 while the initial certification backlog cleared. That judgement
uses only pre-cutoff information.

**Pre-2021 entrant estimate:** net growth 32.7/yr + departures 28.1/yr =
**60.7 gross entrants/yr**, against the model's shipped assumption of 55.

## 3. Results

Observed: 2020 = 1,099 → 2023 = **1,306** (+69/yr).

### Primary — model applies attrition

| Arm | Median | 95% PI | Abs. error | % error | In 80% | In 95% |
|---|---:|---|---:|---:|:-:|:-:|
| 1. Derived, entrants = 55 | 1,178 | 1,159–1,194 | −128 | −9.8% | ✗ | ✗ |
| 2. Derived, entrants pre-2021 | **1,195** | 1,176–1,210 | **−111** | **−8.5%** | ✗ | ✗ |
| 3. Synthetic, entrants = 55 | 1,142 | 1,122–1,162 | −164 | −12.6% | ✗ | ✗ |
| 4. Synthetic, entrants pre-2021 | 1,159 | 1,138–1,178 | −147 | −11.3% | ✗ | ✗ |

### Definition-matched — no attrition on either side

| Arm | Median | 95% PI | Abs. error | % error | In 95% |
|---|---:|---|---:|---:|:-:|
| 1. Derived, entrants = 55 | 1,264 | 1,264–1,264 | −42 | −3.2% | ✗ |
| 2. Derived, entrants pre-2021 | **1,281** | 1,280–1,282 | **−25** | **−1.9%** | ✗ |
| 3. Synthetic, entrants = 55 | 1,264 | 1,264–1,264 | −42 | −3.2% | ✗ |
| 4. Synthetic, entrants pre-2021 | 1,281 | 1,279–1,282 | −25 | −1.9% | ✗ |

### Trajectory and calibration

| Arm | Observed Δ/yr | Predicted Δ/yr | Calibration slope | MC SE |
|---|---:|---:|---:|---:|
| 1. Derived, 55 | 69 | 26.3 | 2.47 | 0.29 |
| 2. Derived, pre-2021 | 69 | 32.0 | 2.00 | 0.28 |
| 3. Synthetic, 55 | 69 | 14.3 | 4.66 | 0.33 |
| 4. Synthetic, pre-2021 | 69 | 20.0 | 3.30 | 0.32 |
| 1. Derived, 55 [matched] | 69 | 55.0 | 1.15 | 0.002 |
| 2. Derived, pre-2021 [matched] | 69 | 60.7 | **1.04** | 0.02 |

A calibration slope of 1.0 means the trajectory shape is right. The primary arms
run 2.0–4.7: the model's growth is far too flat. The definition-matched arms sit
at 1.04–1.15, so the *entrant* trajectory is roughly correct once attrition is
removed from one side of the comparison.

## 4. What failed, and why

### The point estimate missed because 2021–2023 accelerated

| Window | Annual new certifications |
|---|---|
| Pre-cutoff (2018–2020), used for fitting | 40, 48, **10** → mean **32.7** |
| Validation (2021–2023), unseen | 81, 54, 72 → mean **69.0** |

Certification more than doubled in the validation window. The 2020 value of 10
is a COVID-year collapse that sits inside the fitting window and drags the
estimate down; 2021's 81 is partly that backlog clearing. **No model fitted only
on 2018–2020 could have predicted this**, and none of the eight arms does.

This is a real limitation of short-window extrapolation for a young subspecialty
whose certification pipeline is still not in steady state — not a bug.

### The intervals are the more serious failure

The observed value falls outside the **95%** interval in every arm. The
intervals are far too narrow:

| Arm | 95% PI half-width | Miss | Too narrow by |
|---|---:|---:|---:|
| 1. Derived, 55 | 17.5 | 128 | **7.3×** |
| 2. Derived, pre-2021 | 17.0 | 111 | **6.5×** |
| 3. Synthetic, 55 | 20.0 | 164 | **8.2×** |
| 4. Synthetic, pre-2021 | 20.0 | 147 | **7.3×** |

A −8.5% point error at a three-year horizon is arguably tolerable. A 95%
interval of ±17 providers that misses by 111 is not. The model is **overconfident
by roughly an order of magnitude**.

The cause is structural and was already documented as unfinished work: the
engine draws *individual* stochasticity (Bernoulli retirement, fractional
entrants) but holds every *coefficient* fixed. The intervals therefore describe
Monte Carlo sampling noise, not forecast uncertainty. Monte Carlo standard error
is 0.28–0.33 providers — the simulation has converged precisely on the wrong
answer.

The definition-matched arms make this vivid: with attrition off and integral
entrants, the model has **no stochasticity at all** (PI width 0–2 providers).

### Addendum: parameter uncertainty was added, and it is not sufficient

`R/36-parameter_uncertainty.R` now redraws the entrant rate each iteration from
the observed series' own sampling distribution (pre-cutoff only: 40/48/10, SE
11.6 on n=3), reusing PR #8's `.param_draw()` where a fitted model exists.

Re-scoring the best arm, with the point estimate held fixed:

| | Median | 95% PI | Width | In 95%? |
|---|---:|---|---:|:-:|
| Fixed parameters | 1,194 | 1,177–1,212 | 35 | ✗ |
| Drawn entrant rate | 1,194 | 1,127–1,258 | **131** | ✗ |

The interval widened **3.7×** and the median did not move — this adds
uncertainty, it does not recalibrate. Coverage improved from ~6.5× too narrow to
~1.7× too narrow.

**It still does not cover.** That is the honest and informative outcome:
sampling variation in a three-year mean of 32.7/yr cannot reach an outcome that
averaged 69.0/yr. The residual is **structural break, not sampling error** — a
COVID trough inside the fitting window followed by backlog clearance outside it.
Prediction intervals derived from within-window variation cannot represent a
regime change, and no correctly-derived parameter uncertainty would have.

Two parameters remain unquantified and are labelled as such rather than given an
invented spread: the retirement hazard schedule and the hours schedule are
published (HWSM Exhibits 14 and 17, FutureDocs Figure 10) as point estimates
with no sample sizes or standard errors. `supply_parameter_spec()` exposes a
`hazard_cv` that defaults to zero and reports the omission.

### Addendum 2: both failures are addressed by an entrant regime model

`R/49-entrant_regime.R` takes up the two failures above, and
**`docs/ENTRANT_REGIME_MODEL.md` is the write-up**. In brief:

- The point estimate missed because a **cancelled board examination** was
  averaged into the steady-state rate. `urps_entry_counts()` shows ABOG
  certifications falling 35 → 3 in 2020 while ABU held at 13 → 7 — an
  examination failure, not a pipeline contraction. Disrupted years are now
  screened out of the fit and their deficit is **deferred**, because candidates
  who miss a cancelled exam sit the next one.
- The interval missed because coefficients were fixed. The regime model draws
  trend coefficients, overdispersion, deferral timing, and a per-year regime
  break whose deficit carries forward.

**Arm 5 reached the same diagnosis first, and from a better direction.** The NRMP
fellowship-match arm names the identical defect — the certification flow is a
lagging measure corrupted in exactly the estimation window — and answers it with
a leading indicator published before the cutoff rather than a repaired lagging
one. The two are complements: arm 5 supplies the cleaner entrant level, the
regime model supplies the deferral mechanism and the regime-break uncertainty
arm 5's scalar draw has no way to express. `docs/ENTRANT_REGIME_MODEL.md` §5
scores the regime model **against arm 5**, not against the superseded shipped
assumption: −0.08% versus −4.36% definition-matched, and covering where arm 5's
34-wide interval misses.

**Nothing above is re-scored or re-tuned.** The five arms, the
frozen record in `BACKTEST_RECORD_2020_2023`, and `run_backtest()` are unchanged,
because the value of this document is that no parameter moved after the error was
seen. The regime estimator runs through a separate entry point,
`run_entrant_regime_backtest()`, whose every output row is stamped
`out_of_sample = FALSE` — it was built after this miss was examined, so the 2020
cutoff is a refit and is reported as one. Its out-of-sample evidence is
`entrant_regime_rolling_validation()`, and that evidence is thin: the series
admits two folds at a three-year horizon, and
`assert_interval_coverage_publishable()` still refuses it.

The conclusion in §6 therefore stands unchanged for this engine's published
intervals.

## 5. What the back-test does establish

**The derived cohort beats the synthetic one in both matched pairs** — −9.8% vs
−12.6% at 55 entrants, −8.5% vs −11.3% at 60.7. The certification-cohort
construction is worth its complexity.

**The pre-2021 entrant estimate beats the shipped assumption in both matched
pairs** — −8.5% vs −9.8% derived, −11.3% vs −12.6% synthetic. Deriving the rate
from the observed series is better than the hardcoded 55, even when the derived
value is itself too low.

**Best configuration: arm 2** (derived cohort + pre-2021 entrant estimate) at
−8.5% primary, −1.9% definition-matched, calibration slope 1.04.

## 6. Honest verdict

Against the question that matters — *can the model predict an unseen three-year
period without using information from that period?* — the answer is **no, not
with usable uncertainty**.

The point estimates are in a defensible range (−1.9% to −12.6% depending on arm
and definition) and the ordering of the arms is informative. But every arm misses
its own 95% interval, so the model currently cannot state how confident it is.
Until parameter uncertainty is propagated, projected intervals from this engine
should not be reported.

Two caveats bound the interpretation in opposite directions, and neither is
resolvable with the current contract:

1. The observed series applies **no attrition**, so the primary comparison is
   biased against the model by roughly the departure rate (~3%/yr).
2. The validation window contains a **structural break** — a COVID trough
   immediately followed by backlog clearance — that no short-window
   extrapolation could anticipate.

A cleaner test needs a contract artifact that tracks retirement (`n_retired` is
currently 0 throughout), and a longer pre-cutoff steady state than three years,
one of which is 2020.

---

## Reproducing

```bash
Rscript scripts/run_backtest_2020_to_2023.R          # reuses the cached run
BACKTEST_FORCE=1 Rscript scripts/run_backtest_2020_to_2023.R   # re-runs
```

| Artifact | Contents |
|---|---|
| `artifacts/backtest_2020_to_2023_summary.csv` | one row per arm, all metrics |
| `artifacts/backtest_2020_to_2023_iterations.parquet` | every replicate × year × arm |
| `artifacts/backtest_2020_to_2023_trajectory.csv` | per-year medians and intervals |
| `figures/backtest_2020_to_2023.png` | trajectories against observed |

Guards live in `tests/testthat/test-backtest.R`: no post-2020 record enters
fitting, the leakage assertion fires when the cutoff is exceeded, an unaudited
run cannot pass, the target passes the cohort contract, a mismatched target
errors rather than warns, fixed seeds reproduce identical summaries, intervals
match their empirical quantiles, and both sides of the comparison are headcount.

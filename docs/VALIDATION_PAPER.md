# Validation results — manuscript narrative

Single manuscript-ready source of truth for the forecast-validation section.
Every figure below was regenerated from current code; the three starred
diagnostics reproduce byte-identically on re-run. Provenance table at the end.

Underlying diagnostic documents remain as sources and are not superseded:
`BACKTEST_2020_TO_2023.md` (the ten-arm design), `RESULTS_INTERVAL_CALIBRATION.md`
(interval scoring), `VALIDATION_RESULTS.md` (rolling-origin and Monte Carlo
convergence), `BACKTEST_CALIBRATION_AUDIT.md` (the debugging log).

---

## Thesis

> Workforce forecasts can appear precise while being poorly calibrated when the
> simulated quantity is misaligned with the operational definition of the
> validation target. Conventional coverage metrics may fail to reveal this
> problem because excessively wide intervals can achieve nominal containment
> despite poor forecast usefulness.

Results §2, §3, §4 and §5 converge on this from four directions: definition
alignment dominates accuracy; containment ranks the least useful forecast best;
early rolling-origin windows "cover" only because their intervals are enormous;
and leakage manufactures apparent precision.

---

## Design: what was validated, against what

The target is the national count of board-certified URPS physicians in 2023:
**1,306** (ABOG_PLUS_ABU pathway, `board_certified_active` measure, contract
v3.0.0, keyed on subspecialty certification year). `validate_backtest_target()`
fails closed on geography, pathway, measure, contract version and
certification-year basis, so a wrong-target comparison errors rather than warns.

Two designs, and the distinction governs what may be claimed:

| design | targets | establishes |
|---|---|---|
| Ten-arm back-test, cutoff 2020 | **one** (2023) | point error, direction, interval score |
| Rolling origin, origins 2017–2020 | **four** | interval coverage |

**The ten arms are alternative specifications, not independent forecast
occasions.** Two of ten produced an interval containing the observed value: a
containment count, not a 20% coverage rate. Coverage is a repeated-sampling
property and there is one realised value in the sample. Restricting to the five
definition-matched arms and reporting 2/5 carries the identical defect — the
denominator is not the problem. `assert_no_coverage_rate_claim()` enforces this
on manuscript-facing text.

---

## 1. Point-prediction performance

Across the ten arms the median error is **−9.0%**, ranging from **−3.14%** to
**−17.61%**. Every arm under-predicts the 2023 target.

| | arm | % error | contained |
|---|---|---:|:--:|
| best | derived cohort, assumed entrants, no attrition | **−3.14%** | ✔ |
| | synthetic cohort, assumed entrants, no attrition | −3.18% | ✔ |
| worst | synthetic cohort, estimated entrants, attrition | **−17.61%** | ✘ |

The spread between best and worst is 14.5 percentage points, and §2 shows it is
not principally a matter of cohort construction or entrant assumption.

---

## 2. Target-definition alignment dominates accuracy

The shipped configuration under-predicts by **127 providers (−9.7%; 1,179 vs
1,306)**. That miss decomposes as:

| step | 2023 level | Δ | share | per year |
|---|---:|---:|---:|---:|
| shipped forecast (attrition ON, entrants 55/yr) | 1,179 | — | — | — |
| **+ fix definition error** (attrition OFF on a cumulative stock) | 1,265 | **+86** | **68%** | +28.7 |
| + close entrant-regime residual (realised entry > 55/yr) | 1,306 | +41 | 32% | +13.7 |

Sixty-eight percent of the error was applying career attrition to a **cumulative
certification count** — a stock from which providers never exit. The observed
series carries `n_retired = 0` in every row and `n_active` equal to
`n_ever_certified`; the simulation applied retirement hazards to it. The two
quantities are not operationally equivalent, and the model was structurally
guaranteed to under-predict. The remaining 32% is an input trajectory: realised
net entry ran +69/yr against every pre-cutoff assumption (32.7–55/yr).
**Behavioural parameters account for essentially none of the miss.**

Why the best arm wins is now visible. Every paired arm improves by roughly six
percentage points when the simulated quantity is matched to what the target
counts:

| arm | with attrition | definition-matched |
|---|---:|---:|
| 1 derived / assumed entrants | −9.72% | **−3.14%** ✔ |
| 2 derived / estimated entrants | −14.97% | −8.27% |
| 3 synthetic / assumed entrants | −12.63% | **−3.18%** ✔ |
| 4 synthetic / estimated entrants | −17.61% | −8.27% |

Both arms containing the observation are definition-matched. Alignment moves the
answer more than cohort construction or entrant assumption do.

---

## 3. Interval calibration: containment versus interval score

Scored over 2021–2023:

| forecast | targets | contained | mean width | interval score | MAPE |
|---|---:|---:|---:|---:|---:|
| rolling-origin (wide) | 4 | 4/4 | 1,465.6 | 1,465.6 | 7.39% |
| sharp, attrition ON (definition MISMATCH) | 3 | 0/3 | 92.0 | 1,732.0 | 6.90% |
| sharp, no-attrition (definition-MATCHED) | 3 | 2/3 | 97.0 | **137.0** | 2.46% |

The rolling-origin forecast attains perfect containment while being the least
informative: mean width 1,466 providers, with one lower bound at **−594.5**, an
impossible value for a cumulative count. The definition-matched forecast is ~15×
sharper and scores an order of magnitude better (137 vs 1,466) on *lower*
nominal containment.

**Containment ranks the three forecasts in exactly the reverse order of the
interval score.** Because the interval score charges excess width and shortfall
on a single scale, it cannot be improved by widening intervals to force
containment, nor by narrowing them without correcting the point forecast.

A further illustration of width standing in for precision: an ablation of the
entrant model produces a specification (`S0`, frozen process plus NRMP sampling)
with an interval **8 providers wide on a count of 1,273**, which does not
contain the observation. Adding mechanism — fellowship non-completion, the
appointment→certification lag, matched→certified conversion, stock completeness
— degrades point accuracy monotonically from −2.53% to −4.86% while widening the
interval to 91. Neither end of that range is calibrated.

---

## 4. Temporal validation: uncertainty shrinks as the record lengthens

Rolling origin, strictly out-of-time — a training window is admitted only when
its outcome was observable at the origin:

| origin | target | observed | predicted | abs % error | contained | width |
|---:|---:|---:|---:|---:|:--:|---:|
| 2017 | 2020 | 1,099 | 997.9 | 9.20% | ✔ | **3,184.8** |
| 2018 | 2021 | 1,180 | 1,094.4 | 7.25% | ✔ | 1,172.2 |
| 2019 | 2022 | 1,234 | 1,169.2 | 5.25% | ✔ | 818.1 |
| 2020 | 2023 | 1,306 | 1,203.6 | 7.84% | ✔ | 687.1 |

All four origins contain the observation, and the widths fall monotonically as
the usable training record lengthens. But the earliest origin contains it only
because its interval spans **−594.5 to +2,590** on `df = 1` — two prior errors,
`t(0.975) = 12.71`, both training windows inside the COVID backlog regime. The
lower bound is deliberately not truncated at zero, because truncating it would
conceal the mechanism.

This is the thesis in miniature: 4/4 containment, and the forecast that achieves
it is the least usable one in the study.

---

## 5. Leakage sensitivity: non-causal validation manufactures precision

The same model and data, validated two ways:

| design | eligible origins | contained | median abs error | mean width |
|---|---:|---:|---:|---:|
| leave-one-out (**leaky**) | 8 | 7/8 | **2.83%** | 411.7 |
| rolling origin (**honest**) | 4 | 4/4 | **7.55%** | 1,465.6 |

Leave-one-out admits training windows whose outcomes were not observable at the
origin — for the 2013 origin, all seven training points lie in its future. That
contamination makes the model look **2.7× more accurate with 3.6× tighter
intervals**. Neither figure is a property of the forecasting method; the
difference is a property of the validation design.

---

## 6. Window dependence: validation design matters nearly as much as model design

Direction of error is not a stable property of the model:

| cutoff → target | certification predictor | NRMP predictor |
|---|---:|---:|
| 2016 → 2019 | **+17.63%** | — |
| 2017 → 2020 | **+6.64%** | +7.19% |
| 2018 → 2021 | −2.54% | +3.22% |
| 2019 → 2022 | −1.94% | +2.51% |
| 2020 → 2023 | −8.35% | −2.53% |

The model moves from substantial **over**-prediction (+17.6%) to
**under**-prediction (−8.4%) purely as a function of the historical window
chosen. Any single-window validation would support a confident and opposite
conclusion about the direction of bias.

The two entrant definitions also diverge materially. The NRMP-based predictor is
more accurate throughout (|error| 2.5–7.2%) than the certification-based one
(1.9–17.6%), despite the latter drawing on the same series the target is taken
from — a second instance of definitional choice driving apparent performance.

Together these argue against declaring a workforce model "validated" from a
single cutoff–target pair.

---

## Discussion contribution

> A workforce model can be internally sophisticated and apparently well
> calibrated yet fail external validation because it predicts a quantity that is
> not operationally equivalent to the quantity used as truth.

The corollary from §6 is that **validation design matters nearly as much as
model design**: window choice reverses the sign of the measured bias, leakage
inflates apparent accuracy 2.7-fold, and containment alone rewards the least
informative forecast. None of these are properties of the workforce being
modelled.

## What the evidence does not support

- **Not an interval-coverage estimate for the ten-arm design.** One target.
- **Not a validated forecast.** No configuration is offered as calibrated;
  `interval_label()` refuses forecast-interval language while the back-test
  fails and reports Monte Carlo ranges instead.
- **Not the entrant-regime correction as a validated result.** It was identified
  from the errors it would be scored against — model selection on the test set —
  and is preregistered (frozen functional form, hashed) for prospective
  evaluation.
- **Not a claim that behavioural parameters are well specified.** They account
  for essentially none of *this* miss, which is not evidence they are right.

## Provenance

Regenerated on the current tree; starred artifacts were re-run and compared
byte-for-byte against their committed versions.

| claim | artifact |
|---|---|
| target 1,306; ten arms; per-arm error, containment, width | `artifacts/diagnostics/backtest_arm_table.csv` |
| 68% / 32% decomposition; +28.7 and +13.7 per year | `artifacts/diagnostics/entrant_regime_bias_decomposition.csv` * |
| interval score, width, MAPE by forecast | `artifacts/diagnostics/interval_honesty_scorecard.csv` * |
| leaky vs honest validation comparison | `artifacts/diagnostics/validation_comparison_summary.csv` * |
| rolling-origin per-origin widths and bounds | `artifacts/diagnostics/validation_rolling_origin.csv` |
| leave-one-out origins and future-training counts | `artifacts/diagnostics/validation_loo.csv` |
| multi-window direction table | `artifacts/diagnostics/backtest_multi_window.csv` |
| entrant-model ablation | `artifacts/diagnostics/arm5_ablation_table.csv` |
| figure | `artifacts/figures/interval_calibration.{png,pdf}` |
| containment/coverage rule | `R/validation-coverage_language.R` |

Interval score: Gneiting & Raftery (2007). Weighted interval score: Bracher et
al. (2021).

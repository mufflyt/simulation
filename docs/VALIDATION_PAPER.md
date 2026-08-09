# Historical Validation of National Urogynecology Workforce Projections: When the Validation Target Does Not Match the Workforce

**Target journal: _Obstetrics & Gynecology_. Second choice: _Urogynecology_.**

Manuscript-ready source of truth for the validation study. Every figure was
regenerated from current code; the three starred diagnostics reproduce
byte-identically on re-run. Provenance table at the end.

Underlying diagnostic documents remain as sources and are not superseded:
`BACKTEST_2020_TO_2023.md`, `RESULTS_INTERVAL_CALIBRATION.md`,
`VALIDATION_RESULTS.md`, `BACKTEST_CALIBRATION_AUDIT.md`.

---

## Precis

National urogynecology workforce projections were more sensitive to how the
workforce was counted than to assumptions about physician behavior, and
conventional validation could not detect the difference.

## Objective

To evaluate the agreement between historical projections of the clinically
active urogynecology and reconstructive pelvic surgery (URPS) workforce and
subsequently observed national certification counts, and to determine how
differences in the definitions of the projected and observed workforces affect
apparent forecast performance.

---

## Introduction

Whether the United States will have enough urogynecologists to meet the needs of
an aging female population is an important question for the subspecialty.
Workforce projections may influence fellowship training, geographic access to
pelvic-floor care, and planning for future clinical capacity. Yet these
projections are rarely tested against what subsequently occurs, and historical
validation is only meaningful when the quantity predicted by the model is
equivalent to the quantity used as the validation target.

The gap is not particular to urogynecology. A 2024 systematic review of 40
health workforce projection studies found that **8 (20%) compared their
predictions against historical data, and only 4 conducted external validation**,
identifying model validity and transparent reporting as the field's principal
weaknesses [1]. Where validation has been done, it has compared projected totals
against observed totals: the clearest example, a backtest of Dutch
general-practitioner projections for target years 1998–2011, reported mean
absolute percentage errors of **1.9% to 14.9%** and attributed the error to
"bias, not variance" [2].

What such comparisons do not ask is whether the quantity the model simulates is
the same quantity used to check it. A workforce model simulates a stock of
*practicing* physicians; it is typically validated against a registry (board certifications, licenses, national provider identifiers)
assembled for a different purpose. If the model removes physicians who retire and the registry
never does, the two series measure different things, and the resulting
discrepancy will look like a flawed retirement assumption to anyone examining only the residual.

We built a national microsimulation of the URPS workforce and asked a
deliberately simple question: does it reproduce the observed number of
board-certified urogynecologists when the forecast is made from historical data
alone? It does not, and the reasons matter more than the miss. The largest single
source of discrepancy was not how the model represented retirement, career
length, or fellowship entry, but a mismatch between what was simulated and what was counted,
and the validation summary that workforce studies conventionally report could
not have revealed it.

---

## Principal finding

> Projections of the urogynecology workforce were more sensitive to **how the
> workforce was counted** than to assumptions about how urogynecologists behave.
> A single definitional mismatch accounted for **68% of the discrepancy**, while
> the validation measure conventionally reported, containment of the observed
> value, ranked the least useful forecast best.

Four results support this: definition alignment dominates accuracy (§2);
containment inverts the interval-score ranking (§3); early rolling-origin
windows contain the observation only because their intervals are enormous (§4);
and validation designs that admit future information manufacture apparent
precision (§5). Window dependence (§6) shows the conclusions of a validation
exercise are themselves unstable across historical eras.

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
| Ten-arm back-test, cutoff 2020 | **one** (2023) | point difference, direction, interval score |
| Rolling origin, origins 2017–2020 | **four** | interval coverage |

**The ten arms are alternative specifications, not independent forecast
occasions.** Two of ten produced an interval containing the observed value: a
containment count, not a 20% coverage rate. Coverage is a repeated-sampling
property and there is one realised value in the sample. Restricting to the five
definition-matched arms and reporting 2/5 carries the identical defect: the
denominator is not the problem. `assert_no_coverage_rate_claim()` enforces this
on manuscript-facing text.

---

## 1. Agreement with the observed certification count

Across the ten arms the median difference from the observed certification count
is **−9.0%**, ranging from **−3.14%** to **−17.61%**. Every arm falls below the
2023 target.

| | arm | % difference | contained |
|---|---|---:|:--:|
| best | derived cohort, assumed entrants, no attrition | **−3.14%** | ✔ |
| | synthetic cohort, assumed entrants, no attrition | −3.18% | ✔ |
| worst | synthetic cohort, estimated entrants, attrition | **−17.61%** | ✘ |

The spread between best and worst is 14.5 percentage points, and §2 shows it is
not principally a matter of cohort construction or entrant assumption.

---

## 2. Target-definition alignment dominates the discrepancy

The shipped configuration under-predicts by **127 providers (−9.7%; 1,179 vs
1,306)**. That miss decomposes as:

| step | 2023 level | Δ | share | per year |
|---|---:|---:|---:|---:|
| shipped forecast (attrition ON, entrants 55/yr) | 1,179 | n/a | n/a | n/a |
| **+ align definitions** (attrition OFF on a cumulative stock) | 1,265 | **+86** | **68%** | +28.7 |
| + close entrant-regime residual (realised entry > 55/yr) | 1,306 | +41 | 32% | +13.7 |

Sixty-eight percent of the discrepancy came from applying career attrition to a **cumulative certification count**, a stock from which
providers never exit. The observed
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

"Definition-matched" here means the simulated quantity was made comparable to
what the target counts, by suspending attrition for the comparison only. It does
not mean the projection is better without retirement. Retirement is retained in
the production projection, which uses empirical URPS hazards, because practicing
capacity rather than cumulative certification is the quantity workforce planning
requires. These arms isolate how much of the discrepancy the definitional gap explains;
they are not a recommended configuration.

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
contain the observation. Adding mechanism (fellowship non-completion, the
appointment→certification lag, matched→certified conversion, and stock
completeness) degrades point accuracy monotonically from −2.53% to −4.86% while widening the
interval to 91. Neither end of that range is calibrated.

---

## 4. Temporal validation: uncertainty shrinks as the record lengthens

Rolling origin, strictly out-of-time: a training window is admitted only when
its outcome was observable at the origin:

| origin | target | observed | predicted | abs % difference | contained | width |
|---:|---:|---:|---:|---:|:--:|---:|
| 2017 | 2020 | 1,099 | 997.9 | 9.20% | ✔ | **3,184.8** |
| 2018 | 2021 | 1,180 | 1,094.4 | 7.25% | ✔ | 1,172.2 |
| 2019 | 2022 | 1,234 | 1,169.2 | 5.25% | ✔ | 818.1 |
| 2020 | 2023 | 1,306 | 1,203.6 | 7.84% | ✔ | 687.1 |

All four origins contain the observation, and the widths fall monotonically as
the usable training record lengthens. But the earliest origin contains it only
because its interval spans **−594.5 to +2,590** on `df = 1`: two prior errors,
`t(0.975) = 12.71`, both training windows inside the COVID backlog regime. The
lower bound is deliberately not truncated at zero, because truncating it would
conceal the mechanism.

This is the thesis in miniature: 4/4 containment, and the forecast that achieves
it is the least usable one in the study.

---

## 5. Leakage sensitivity: non-causal validation manufactures precision

The same model and data, validated two ways:

| design | eligible origins | contained | median abs difference | mean width |
|---|---:|---:|---:|---:|
| leave-one-out (**leaky**) | 8 | 7/8 | **2.83%** | 411.7 |
| rolling origin (**honest**) | 4 | 4/4 | **7.55%** | 1,465.6 |

Leave-one-out admits training windows whose outcomes were not observable at the
origin: for the 2013 origin, all seven training points lie in its future. That
contamination makes the model look **2.7× more accurate with 3.6× tighter
intervals**. Neither figure is a property of the forecasting method; the
difference is a property of the validation design.

---

## 6. Window dependence: validation design matters nearly as much as model design

The direction of the difference is not a stable property of the model:

| cutoff → target | certification predictor | NRMP predictor |
|---|---:|---:|
| 2016 → 2019 | **+17.63%** | n/a |
| 2017 → 2020 | **+6.64%** | +7.19% |
| 2018 → 2021 | −2.54% | +3.22% |
| 2019 → 2022 | −1.94% | +2.51% |
| 2020 → 2023 | −8.35% | −2.53% |

The model moves from substantial **over**-prediction (+17.6%) to
**under**-prediction (−8.4%) purely as a function of the historical window
chosen. Any single-window validation would support a confident and opposite
conclusion about the direction of bias.

The two entrant definitions also diverge materially. The NRMP-based predictor is
closer to the observed series throughout (|difference| 2.5–7.2%) than the
certification-based one (1.9–17.6%), despite the latter drawing on the same series the target is taken from, a
second instance of definitional choice driving apparent performance.

Together these argue against declaring a workforce model "validated" from a
single cutoff-target pair.

---

## Discussion

The principal finding was not that the workforce model should retain physicians
indefinitely to reproduce cumulative certification counts. The production model
appropriately removes physicians from the clinically active workforce using
empirical URPS retirement hazards because clinical capacity, rather than
accumulated credentials, is the quantity relevant to workforce planning.
Instead, most of the discrepancy between the projected active workforce and the
published 2023 certification count arose because the two quantities were not
operationally equivalent. Turning attrition off improved agreement with the
cumulative certification series while making the simulated workforce less
representative of clinical capacity. Thus, historical validation can make an
appropriate workforce model appear inaccurate when the validation target
measures a different construct. Future workforce projections should define the
clinical workforce estimand before model construction and, when possible, be
validated against data that measure active clinical practice rather than
cumulative certification.

**There is no gold-standard external validation of the active workforce in this
study, and none is claimed.** The only national series available for URPS is
cumulative certification, which counts credentials rather than practicing
physicians. Everything reported here is therefore agreement against an imperfect
benchmark. That does not make the exercise uninformative; it makes it an
demonstration of what happens when an imperfect benchmark is treated as truth.
A licensure or billing-activity series would measure clinical practice more
closely, and neither is currently linked to this model.

**Quantifying the discrepancy.** The projected active workforce differed from
the published 2023 certification count by 127 providers (−9.7%). Sixty-eight
percent of that difference is attributable to the definitional gap: the model
removes physicians as they retire, the certification series never does. A
further 32% reflects faster-than-assumed entry into the field (realized entry of
+69/yr against pre-cutoff assumptions of 32.7–55/yr). Assumptions about
physician behavior (retirement timing, career length, and cohort composition)
account for essentially none of the remainder.

**Conventional validation would have selected the worst forecast.** Containment
of the observed value, the summary most workforce studies report, ranked the
three candidate forecasts in exactly the reverse order of the interval score.
The forecast achieving 4 of 4 containment did so with a mean interval width of
1,466 providers and one lower bound of −594.5, an impossible value for a
cumulative count, while the definition-matched forecast was roughly 15 times
sharper and scored an order of magnitude better on *lower* containment. A wide
enough interval contains almost anything; reporting containment alone rewards
exactly that.

**Apparent agreement also depends on how validation is designed.** The identical
model and data appeared 2.7 times closer to the observed series with 3.6 times
tighter intervals under leave-one-out validation, which admits training windows
whose outcomes were not yet observable at the forecast origin. And the direction
of the difference reversed, from +17.6% above the observed count to −8.4% below
it, purely with the choice of historical window. Any single cutoff-target
comparison would have supported a confident conclusion about the direction of
bias, and the opposite conclusion was equally available from the same model.

### Implications for urogynecology workforce planning

Projections are already being used to argue about fellowship expansion and future
access to pelvic-floor care. Three practical consequences follow.

First, a workforce projection should state **what it is counting** (practicing
physicians, board-certified physicians, ever-certified physicians, or clinical
full-time equivalents) and demonstrate that the series used to validate it
counts the same thing. In our case the two differed by whether anyone is ever
removed, which is not a subtle distinction once stated, and was invisible until
validation forced it into the open.

Second, a projection validated against a single historical target should not be
described as validated. Ten model configurations scored against one observed
value tell you which configurations were consistent with that value; they cannot
establish how often the method's intervals contain the truth.

Third, narrow intervals are not evidence of accuracy. One specification in our
ablation produced an interval **8 providers wide on a count of 1,273** and did not
contain the observation. Precision and calibration are different properties, and
a workforce estimate quoted without an interval, or with an implausibly tight
one, should invite scepticism rather than confidence.

### Generalizability beyond urogynecology

The specific mismatch here is an artifact of how one board publishes counts. The
class of error is not. Workforce models simulate stocks (physicians active,
clinically engaged, or delivering care at some effort level) and are validated
against registries assembled for administrative purposes. Certification series
are typically cumulative and rarely decremented; license files reflect renewal
behavior rather than practice; national provider identifiers persist after
retirement. A model that simulates exit, scored against a series that never
removes anyone, is structurally guaranteed to under-predict.

Two features of the literature suggest this is unlikely to be unique to us.
Validation is uncommon:
8 of 40 studies compared predictions with historical data and 4 externally [1], so
an error class that surfaces only during validation
will be under-detected. And the validation that does occur compares totals: the
Dutch general-practitioner backtest attributed its error to "bias, not variance"
[2], which is the signature a definitional mismatch produces, without examining
whether the projected and observed quantities were equivalent.

We suggest workforce projection studies state the quantity they simulate
explicitly (population, activity threshold, and treatment of exit), and then
select a validation target that measures that same quantity. Where no such
target exists, as here, the mismatch should be reported and quantified rather
than absorbed into the residual. The direction matters: the remedy is to choose
or adjust the yardstick, not to redefine the projection to match whatever series
happens to be published. A model bent to reproduce a cumulative registry would
score better and forecast worse. The 2024 good-practice reporting guideline is
the natural place for such a requirement [1].

## What the evidence does not support

- **Not an interval-coverage estimate for the ten-arm design.** One target.
- **Not a validated forecast.** No configuration is offered as calibrated;
  `interval_label()` refuses forecast-interval language while the back-test
  fails and reports Monte Carlo ranges instead.
- **Not the entrant-regime correction as a validated result.** It was identified from the errors it would be scored against, which is model selection
on the test set. It is preregistered (frozen functional form, hashed) for prospective
  evaluation.
- **Not a claim that behavioural parameters are well specified.** They account
  for essentially none of *this* miss, which is not evidence they are right.

## Limitations

These are diagnostic findings, not precise estimates of long-run operating
characteristics, and the paper should not be read as claiming otherwise.

**The repeated-target evidence rests on four rolling origins.** Four forecast
occasions cannot estimate coverage with useful precision, and we do not present
the 4/4 containment result as evidence of calibration; §4 exists largely to
show why it is not. An interval spanning −594.5 to +2,590 contains almost
anything. Reporting a small number of successful containment events as strong
calibration evidence is the error this paper is about; we decline to commit it
with our own results.

**The single-target exercise cannot estimate coverage at all.** Ten arms scored
against one realised value are alternative specifications, not independent
forecast occasions. Two of ten contained the observation; that is a containment
count and not a 20% coverage rate, and restricting to the five
definition-matched arms does not repair it.

**The findings are qualitative, and deliberately so.** What we report are a
reversal in ranking, a sign flip, and a 2.7-fold gap. These survive small samples
better than point estimates would, but none should be quoted as a calibrated
magnitude.

**One specialty, one country, one certification series.** The specific mismatch
is an artifact of how one board publishes counts. We argue in the Discussion that
the class of error generalizes
(stocks simulated, registries validated against), but we have not demonstrated it in a second setting, and doing so is the obvious
next study.

**The entrant-regime correction is not a validated result.** It was identified
from the errors it would be scored against, which is model selection on the test
set. It is preregistered with a frozen functional form for prospective
evaluation and is reported here only as a decomposition term.

**No demand-side or adequacy claim is made.** The base-year adequacy calibration
is adopted by analogy from another specialty and is out of scope here; nothing in
this paper should be read as an estimate of workforce adequacy.

## References

1. Lee JT, Crettenden I, Tran M, et al. Methods for health workforce projection
   model: systematic review and recommended good practice reporting guideline.
   *Human Resources for Health*. 2024;22:25.
2. Van Greuningen M, Batenburg RS, Van der Velden LFJ. The accuracy of general
   practitioner workforce projections. *Human Resources for Health*.
   2013;11:31.
3. Gneiting T, Raftery AE. Strictly proper scoring rules, prediction, and
   estimation. *J Am Stat Assoc*. 2007;102(477):359–378.
4. Bracher J, Ray EL, Gneiting T, Reich NG. Evaluating epidemic forecasts in an
   interval format. *PLoS Comput Biol*. 2021;17(2):e1008618.

## Provenance

Regenerated on the current tree; starred artifacts were re-run and compared
byte-for-byte against their committed versions.

Columns reported here as "% difference" correspond to the `percent_error` and
`abs_pct_error` fields in the artifacts; the label differs because the
certification series is an imperfect benchmark rather than a criterion standard.

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

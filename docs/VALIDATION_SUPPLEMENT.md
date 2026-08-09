# Supplement: Historical Validation of National Urogynecology Workforce Projections

Supporting material for the manuscript in `VALIDATION_PAPER.md`. Every figure
here was regenerated from current code; the three starred artifacts in Appendix
S6 were re-run and compared byte-for-byte against their committed versions.

---

## S1. Validation designs and what each establishes

| Design | Targets | Establishes |
|---|---|---|
| Ten-arm back-test, cutoff 2020 | one (2023) | point difference, direction, interval score |
| Rolling origin, origins 2017 to 2020 | four | interval calibration |

The ten arms are alternative specifications, not independent forecast occasions.
Two of ten produced an interval containing the observed value: a containment
count, not a 20% coverage rate. Coverage is a repeated-sampling property and
there is one realized value in the sample. Restricting to the five
definition-matched arms and reporting 2 of 5 carries the identical defect,
because the denominator is not the problem.

This distinction is enforced in code. `backtest_status()` exposes
`coverage_is_estimable = FALSE` for the single-target design, and
`assert_no_coverage_rate_claim()` refuses coverage-rate phrasing in
manuscript-facing text while permitting negated statements such as "coverage is
not estimable from a single target". `interval_label()` likewise refuses
forecast-interval language while the back-test fails, reporting Monte Carlo
ranges instead.

## S2. All ten configurations

Primary specification: the permanent under-50 career-change process is omitted
because its only estimate postdates the 2020 origin (Appendix S9).

| Arm | Cohort | Entrants/yr | Attrition | % difference | Contained | Width |
|---|---|---:|:--:|---:|:--:|---:|
| 1 | derived | 55 | yes | −7.58 | no | 129.1 |
| 1 | derived | 55 | no | **−3.14** | **yes** | 145.0 |
| 2 | derived | 32.67 | yes | −12.63 | no | 143.0 |
| 2 | derived | 32.67 | no | −8.27 | no | 129.0 |
| 3 | synthetic | 55 | yes | −11.18 | no | 142.1 |
| 3 | synthetic | 55 | no | **−3.18** | **yes** | 138.0 |
| 4 | synthetic | 32.67 | yes | −16.23 | no | 139.0 |
| 4 | synthetic | 32.67 | no | −8.27 | no | 139.1 |
| 5 | derived | 49.73 | yes | −8.81 | no | 46.0 |
| 5 | derived | 49.73 | no | −4.36 | no | 34.0 |

Arms 1 to 4 are the prespecified specifications. Arm 5 was added after the
original four and is scored alongside them, not in place of them.

Both arms containing the observation are definition-matched. Every paired arm
improves by four to eight percentage points when the simulated quantity is made
comparable to what the target counts.

"Definition-matched" means attrition was suspended **for the comparison only**.
It does not mean the projection is better without retirement. Retirement is
retained in the production projection, which uses empirical URPS hazards,
because practicing capacity rather than cumulative certification is the quantity
workforce planning requires. These arms isolate how much of the discrepancy the
definitional gap explains; they are not a recommended configuration.

## S3. Rolling-origin detail

| Origin | Target | Observed | Predicted | Abs % difference | Contained | Width |
|---:|---:|---:|---:|---:|:--:|---:|
| 2017 | 2020 | 1,099 | 997.9 | 9.20 | yes | 3,184.8 |
| 2018 | 2021 | 1,180 | 1,094.4 | 7.25 | yes | 1,172.2 |
| 2019 | 2022 | 1,234 | 1,169.2 | 5.25 | yes | 818.1 |
| 2020 | 2023 | 1,306 | 1,203.6 | 7.84 | yes | 687.1 |

Widths fall monotonically as the usable training record lengthens. Accumulating
history is a plausible explanation; this is an observed pattern, not a
mathematical property.

The 2017 origin is limited twice over. Only two prior errors are available, so
`df = 1` and `t(0.975) = 12.71`, and both training windows fall in the
pandemic backlog regime. The resulting lower bound is −594.5, deliberately not
truncated at zero, because truncating it would conceal the mechanism.

## S4. Leakage sensitivity

| Design | Eligible origins | Contained | Median abs difference | Mean width |
|---|---:|---:|---:|---:|
| Leave-one-out (leaky) | 8 | 7/8 | 2.83% | 411.7 |
| Rolling origin (honest) | 4 | 4/4 | 7.55% | 1,465.6 |

Leave-one-out admits training windows whose outcomes were not observable at the
origin. For the 2013 origin, all seven training points lie in its future.
Neither figure is a property of the forecasting method; the difference is a
property of the validation design.

## S5. Window dependence and entrant definition

| Cutoff to target | Certification predictor | NRMP predictor |
|---|---:|---:|
| 2016 to 2019 | +17.63% | +1.29% |
| 2017 to 2020 | +6.64% | +3.84% |
| 2018 to 2021 | −2.54% | +0.45% |
| 2019 to 2022 | −1.94% | +0.19% |
| 2020 to 2023 | −8.35% | −4.43% |

The model moves from substantial over-prediction to under-prediction purely as a
function of the historical window chosen. The NRMP-based predictor is closer in
every window despite the certification-based one drawing on the same series the
target is taken from, a second instance of definitional choice driving apparent
performance.

PROVENANCE NOTE. The NRMP column differs from earlier drafts of this table. That
change has nothing to do with the career-change correction in Appendix S9. The
NRMP entrant series was extended backwards from 2017-2020 to 2010-2020, and this
table had not been regenerated since. The predictor is a mean over all reports
published by the cutoff, so eleven years spanning the establishment ramp (30
positions filled in 2010, against a plateau near 57 from 2015) give a lower rate
than four years of the plateau alone: 49.73/yr rather than 58.0/yr at the 2020
cutoff. The certification column is unchanged, and rolling-origin and
leave-one-out results are unaffected because those artifacts were already
regenerated against the extended series.

More pre-cutoff evidence made this predictor's 2020 window worse, from −2.53% to
−4.43%, and made the four earlier windows better. Restricting the series to the
2015 plateau would score better and would be a window chosen with the answer in
hand.

## S6. Entrant-model ablation

Each row adds one mechanism to the row above.

| Specification | % difference | Width |
|---|---:|---:|
| S0 frozen: process plus NRMP sampling | −2.53 | 8 |
| S1 plus extended NRMP series, 2015 to 2020 | −2.53 | 7 |
| S2 plus fellowship non-completion (ACGME) | −2.83 | 11 |
| S3 plus appointment-to-certification lag | −2.76 | 11 |
| S4 plus matched-to-certified conversion | −4.82 | 69 |
| S5 plus baseline-stock completeness | −4.86 | 91 |

No specification contains the observation. Point accuracy degrades monotonically
as realism is added, and S0's width of 8 providers on a count of 1,273 is
overconfidence rather than precision.

## S7. Provenance

Columns reported as "% difference" correspond to the `percent_error` and
`abs_pct_error` fields in the artifacts; the label differs because the
certification series is an imperfect benchmark rather than a criterion standard.
Starred artifacts were re-run and compared byte-for-byte against their committed
versions.

| Claim | Artifact |
|---|---|
| target 1,306; ten arms; per-arm difference, containment, width | `artifacts/backtest_2020_to_2023_summary.csv` |
| parameter availability audit | `R/validation-parameter_provenance.R` |
| career-change sensitivity | `artifacts/diagnostics/career_change_sensitivity.csv` |
| 68% / 32% decomposition; +28.7 and +13.7 per year | `artifacts/diagnostics/entrant_regime_bias_decomposition.csv` * |
| interval score, width, MAPE by forecast | `artifacts/diagnostics/interval_honesty_scorecard.csv` * |
| leaky versus honest validation comparison | `artifacts/diagnostics/validation_comparison_summary.csv` * |
| rolling-origin per-origin widths and bounds | `artifacts/diagnostics/validation_rolling_origin.csv` |
| leave-one-out origins and future-training counts | `artifacts/diagnostics/validation_loo.csv` |
| multi-window direction table | `artifacts/diagnostics/backtest_multi_window.csv` |
| entrant-model ablation | `artifacts/diagnostics/arm5_ablation_table.csv` |
| figure | `artifacts/figures/interval_calibration.{png,pdf}` |
| containment versus coverage rule | `R/validation-coverage_language.R` |

Run configuration: 1,000 Monte Carlo iterations per arm, cutoff 2020, target
2023, URPS contract v3.0.0, certifications through 2020 only, permanent under-50
career-change process omitted (Appendix S9).

## S8. Baseline cohort construction

The derived cohort reconstructs individual physicians from the observed count of
certifications in each certification year through the cutoff, so the resulting
age structure inherits the actual shape of the certification record. Age at
certification is drawn from one of two distributions:

| Cohort | Definition | Age at certification |
|---|---|---|
| Backlog | certification year <= 2013, the first URPS certification year | Normal(45, 8) |
| Fellowship | certification year > 2013 | Normal(34, 2.5) |

The split exists because the first certification year admitted established
practitioners through a practice pathway rather than fellowship graduates, so
treating them as 34-year-old entrants would understate the age of roughly the
first third of the workforce and delay their modeled retirements by a decade.
Current age is the drawn age at certification plus elapsed years, clamped to
[34, 89]. Sex is assigned with a 0.55 female share.

The synthetic cohort draws every physician's age from a single Normal(52, 9)
distribution and matches only the aggregate headcount. It carries no
certification-year structure, so it cannot represent the backlog at all.

NRMP entrant series are filtered by report publication date rather than
appointment year (`available_by = cutoff_year`), so no match report published
after the cutoff can enter the fit. NRMP counts fellows at appointment while the
certification series counts them several years later at certification; the two
are offset in time by construction.

## S9. Parameter provenance and the 2020 cutoff

A forecast origin binds parameters, not only data. Every parameter reaching the
back-test was audited against the 2020 cutoff.

| Parameter | Basis | Source | Available by | In primary analysis |
|---|---|---|---:|:--:|
| Retirement hazard by age | published | HWSM Exhibit 17 (Florida physician survey 2012-2013), doc v5.19.20, May 2020; FutureDocs 2017 | 2020 | yes |
| Retirement sex multiplier | published | HWSM Exhibit 17 | 2020 | yes |
| Terminal age (90) | published | HWSM: 90 for physicians and dentists | 2020 | yes |
| Career change under 50 (1.42%/yr) | published | Zarek et al, *Phys Ther* 2025;105:pzaf014 (CPS ASEC, Wolf and Lockard method) | 2025 | **no** |
| Age at entry (34) | assumption | cliff `WC_ENTRY_AGE` | n/a | yes |
| Backlog age at certification | assumption | practice-pathway cohort certified <= 2013 | n/a | yes |
| Female share (0.55) | assumption | cohort construction | n/a | yes |

The career-change hazard is the one parameter that fails the audit. It was in the
back-test until this audit, and the existing leakage guard did not catch it: that
guard audits contract series **reads**, and a hard-coded parameter never
performs one. `assert_backtest_parameters_precede_cutoff()` now runs alongside
`assert_no_leakage()` inside `run_backtest()` and fails closed on any published
parameter in the primary path whose source postdates the cutoff.

Omitting the parameter is not a claim that the hazard is zero. HWSM represents
under-50 exit as temporary labour-force participation with re-entry, a different
process this model does not implement, so no 2020-vintage value was available to
substitute for a permanent separation hazard. Re-estimating one from pre-2020 CPS
ASEC data would be a legitimate future refinement.

Two clarifications on scope. The validation outcome is headcount, computed
independently of the FTE columns, so the post-2020 FTE parameters in the same
source (Dall 2021, 37.2 patient-care hours/week; Zarek 2025, 40 hours/week) do
not enter this comparison. Separately, `backtest_entrant_estimate()` reports
"modelled departures" using the 1.42% constant; that figure is documented as
recorded for reporting only, does not enter the entrant estimate or the parameter
draw, and was verified inert (every no-attrition arm is bit-identical whether the
hazard is applied or not).

## S10. Sensitivity: applying the post-cutoff career-change hazard

The primary analysis omits the permanent under-50 career-change process. This
appendix applies the 2025 estimate of 1.42%/yr to show what it would have done.
It is reported as a sensitivity analysis and not as the primary result, because
leakage is a property of the primary analysis: showing the effect is modest does
not make a post-cutoff parameter admissible in a forecast claiming a 2020 origin.

| Arm | Attrition | % difference, primary | % difference, 1.42% applied |
|---|:--:|---:|---:|
| 1 derived, 55 | yes | −7.58 | −9.72 |
| 2 derived, 32.67 | yes | −12.63 | −14.97 |
| 3 synthetic, 55 | yes | −11.18 | −12.63 |
| 4 synthetic, 32.67 | yes | −16.23 | −17.61 |
| 5 derived, 49.73 | yes | −8.81 | −11.03 |
| all no-attrition arms | no | unchanged | unchanged |

Every no-attrition arm is identical under both specifications, because both exit
hazards are already zero there. That is what makes the contrast interpretable:
the difference is attributable to the career-change process alone.

| Quantity | Primary | 1.42% applied |
|---|---:|---:|
| Arm 1 projected 2023 | 1,207 | 1,179 |
| Total discrepancy | 99 | 127 |
| Definitional component | 58 (59%) | 86 (68%) |
| Entrant-regime component | 41 (41%) | 41 (32%) |
| Containment | 2/10 | 2/10 |
| Interval score, attrition applied | 1,086.4 | 1,732.0 |
| Interval score, definition-matched | 137.0 | 137.0 |

Direction, ranking, and every qualitative conclusion are unchanged. Two
quantities move: the definitional share of the discrepancy (59% versus 68%) and
the interval score of the mismatch forecast, which under the post-cutoff
specification is worse than the wide rolling-origin forecast rather than better.
Rolling-origin, leave-one-out, and window analyses are unaffected in both
specifications because they do not call the career microsimulation.

## S11. What the evidence does not support

- Not a validated forecast. No configuration is offered as calibrated.
- Not a claim that behavioral parameters are well specified. They account for
  essentially none of this discrepancy, which is not evidence they are right.
- Not the entrant-regime correction as a validated result. It was identified
  from the errors it would be scored against, which is model selection on the
  test set, and is preregistered with a frozen functional form for prospective
  rolling-origin evaluation.

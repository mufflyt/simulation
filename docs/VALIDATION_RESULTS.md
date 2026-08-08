# Validation results

Produced by `scripts/validation/01_temporal_validation.R` and
`scripts/validation/02_monte_carlo_convergence.R` against the production roster
(n = 1,339), contract v3.0.0, roster snapshot 2026-07-22.

These are **validation of the calculation, not validation of the calibration**.
Everything here tests whether the engine forecasts and converges properly. None
of it establishes that the base-year adequacy calibration is correct for URPS —
see the gate note at the end.

---

## 1. Primary: rolling-origin validation, prespecified contemporary origins

Origins 2017–2020, horizon 3 years, strictly out-of-time: a training window is
admitted only when its outcome was observable at the origin. These origins are
`backtest_multi_window()`'s own defaults, not a set chosen after seeing
performance.

| Origin | Target | Observed | Predicted | \|% error\| | Covered | Interval width |
|---:|---:|---:|---:|---:|:--:|---:|
| 2017 | 2020 | 1099 | 997.9 | 9.20% | yes | 3185 |
| 2018 | 2021 | 1180 | 1094.4 | 7.25% | yes | 1172 |
| 2019 | 2022 | 1234 | 1169.2 | 5.25% | yes | 818 |
| 2020 | 2023 | 1306 | 1203.6 | 7.84% | yes | 687 |
| **median** | | | | **7.5%** | **4/4** | **995** |

Coverage is reported **with** width. An interval can cover by being uselessly
wide, and the 2017 interval does exactly that.

In these results the widths fall monotonically as the usable training record
lengthens (3185 → 1172 → 818 → 687). Accumulating history is a plausible
explanation; this is an observed pattern, not a mathematical property.

## 2. The 2017 origin: two limitations compounding

Its interval is enormous and its lower bound is **negative**. Deliberately not
truncated at zero.

* Only two prior errors are available: `df = 1`, so `t(0.975) = 12.71`.
* Both training windows fall in the backlog regime: mean relative error **139%**.

`lower factor = 1 + μ − t·s = 1 + 1.390 − 12.71 × 0.448 = −3.304`

The interval is reporting that the empirical error model is **essentially
unidentified** at that origin. Truncating would conceal the most informative
thing it says. A log-scale construction, `exp(μ_log − t·s_log) = 0.216`, has
positive support and is the principled improvement — reported as a **secondary**
construction rather than swapped in after seeing the negative value.

## 3. Historical stress test: crossing a structural break

`classify_certification_regimes()` labels years from the certification series'
own structure, with no reference to forecast error:

| Years | Certifications | Regime |
|---|---|---|
| 2013–2015 | 655, 175, 102 | **backlog** |
| 2016–2019 | 36–48 | steady |
| 2020 | 10 | **disrupted** (cancelled examination) |
| 2021–2023 | 54–81 | steady |

Extending the origins back to 2013 forces the model across that break, and point
error degrades accordingly: +171%, +107%, +79% at the earliest cutoffs against
−8.3% to +6.6% for the contemporary four.

This is a finding about **temporal transportability**, not a defect. The model
performs well inside the contemporary data-generating regime and predictably
poorly when extrapolated across a documented discontinuity.

Note that 2020 is itself abnormal, so "2017–2020" should be described as the
*prespecified contemporary validation origins* — **not** as a steady regime.

## 4. Leakage experiment: matched origins

Leave-one-out is **not a competing validation method**. It is here to quantify
what temporal leakage buys. Both methods on the **same four origins**:

| | median \|% error\| | coverage | median width | median Winkler | future windows used |
|---|---:|---:|---:|---:|---:|
| Rolling-origin | **7.5%** | 4/4 | **995** | 995 | **0** |
| Leave-one-out | **2.8%** | 4/4 | **488** | 488 | **14** |

Excluding the unstable 2017 origin, the effect persists: 7.25% vs 2.94% error,
818 vs 499 width.

Every interval covers, so the Winkler score reduces to width — there is no
hidden miss penalty driving the comparison.

**Using future information would have made the model appear ~2.7× more accurate
and ~2× sharper on identical forecast origins.** LOO interval widths are also
roughly flat across origins (438–508) while rolling-origin widths contract,
because every LOO fit sees nearly the whole record and so never faces the
early-history handicap. Leakage does not merely improve estimates; it erases
uncertainty an investigator would genuinely have faced at the time.

## 5. Monte Carlo convergence

Criterion **declared before the run**: across independent seeds the 2050 median
must vary by < 0.5%, and the 2.5th percentile, 97.5th percentile and interval
width by ≤ 5%.

Three independent seeds, 2050 supply FTE, range as % of mean:

| n | median | median range | 2.5% range | 97.5% range | width mean | width range | Verdict |
|---:|---:|---:|---:|---:|---:|---:|:--|
| 250 | 2075 | 0.158% | 0.62% | 1.36% | 227.4 | 14.91% | FAIL |
| 500 | 2076 | 0.162% | 0.52% | 1.01% | 228.9 | 8.82% | FAIL |
| 1,000 | 2075 | 0.144% | 0.36% | 0.72% | 226.6 | 3.80% | **PASS** |
| 2,000 | 2072 | 0.119% | 0.14% | 0.30% | 227.8 | 2.54% | **PASS** |

**n = 1,000 is the smallest passing count.**

The median is stable everywhere (range ≤ 0.16%). Mean width shows **no
systematic dependence on n**; what improves is the *reproducibility* of the
endpoints (14.9% → 2.5%).

> A single-seed sweep of this same design produced widths of 249 → 242 → 232 →
> 229 and reads as convergence. It is not: across three seeds the mean width is
> flat, and that sequence was one seed sitting at the high end at every count.
> Monte Carlo error moves an estimated quantile in either direction. Do not
> report a single-seed width trend.

## 6. Parameter-uncertainty sensitivity: retirement hazard

The engine draws the entrant rate but holds the retirement hazard fixed, because
it is published without standard errors. Fixing it is not the same as knowing
it. At n = 2,000, single seed:

| Retirement treatment | 2050 median | 2.5% | 97.5% | Width | Width inflation | Median shift |
|---|---:|---:|---:|---:|---:|---:|
| Fixed | 2073.11 | 1964.10 | 2193.45 | 229.35 | — | — |
| Moderate (CV 0.15) | 2075.89 | 1931.54 | 2212.85 | 281.31 | **+22.7%** | +0.13% |
| High (CV 0.30) | 2083.96 | 1880.38 | 2252.52 | 372.14 | **+62.3%** | +0.52% |

Interval-width inflation is `100 × (W_uncertain / W_fixed − 1)`. This is **not**
a variance decomposition, and it should not be described as "uncertainty
previously hidden" — a related but different quantity,
`100 × (1 − W_fixed / W_uncertain)`, gives 18.5% and 38.4%. Do not interchange
them.

Propagating plausible retirement-hazard variation had little effect on the
median 2050 projection but substantially widened the conditional simulation
interval. That is *median insensitivity to the assumed variation*, not a
demonstration that fixing retirement is unbiased.

**CV 0.15 and 0.30 are declared sensitivity assumptions**, labelled moderate and
high. They are not estimated uncertainty distributions and are not confidence
bounds on retirement rates.

Seed-to-seed width noise at n = 2,000 is ±2.5%, an order of magnitude below both
inflation effects, so the comparison is not seed-driven.

---

## What none of this establishes

Every result above validates the **calculation**. None validates the
**calibration**.

`balance_reversal_threshold()` computes a tipping point of **1.294×** the
reference adequacy calibration, reproduced independently. `balance_reversal_sentence()`
nevertheless refuses to emit it, because the demand calibration behind it is
below the tier required for a manuscript-ready threshold. It is a
software-validation result: it verifies the machinery, not the workforce.

The base-year adequacy figure (`REFERENCE_ADEQUACY_CALIBRATION`, 0.948) is a
calibration choice adopted by analogy from a physical-therapy workforce model.
The three published donor anchors span 1.00–1.065× the reference, and the model
uses the **lowest** of them. That range is legitimate to report as externally
motivated sensitivity; it does not establish where URPS adequacy actually lies.

Conditional simulation intervals are **not** empirical prediction intervals: in
the frozen 2020→2023 back-test the observed value fell outside the 95% interval
in 8 of 10 arms. Report stochastic and forecast uncertainty separately.
